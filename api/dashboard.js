const { getDb } = require('../lib/db');
const { google } = require('googleapis');
const SPREADSHEET_ID = '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw';

function addDays(isoDate, delta) {
  const d = new Date(`${isoDate}T12:00:00.000Z`);
  d.setUTCDate(d.getUTCDate() + delta);
  return d.toISOString().slice(0, 10);
}

function dateRangeInclusive(start, end) {
  const out = [];
  let cur = start;
  while (cur <= end) { out.push(cur); cur = addDays(cur, 1); }
  return out;
}

/** 발주가능상태가 "발주가능"이고 품절여부(Y/YES)인 SKU만 인사이트 품절·카운트에 포함 */
function isOrderableStatus(status) {
  return String(status ?? '').trim() === '발주가능';
}
function isOosFlagYes(v) {
  const u = String(v ?? '').trim().toUpperCase();
  return u === 'Y' || u === 'YES';
}
function isInsightListedOos(row) {
  return isOrderableStatus(row.status) && isOosFlagYes(row.oos_flag);
}

/** 최근일부터 역으로 연속 출고 0 구간의 맨 앞 날짜(ISO) */
function findOosSalesStartIso(datesAsc, dateToSales) {
  const n = datesAsc.length;
  if (!n) return null;
  let j = n - 1;
  while (j >= 0) {
    const v = Number(dateToSales[datesAsc[j]]) || 0;
    if (v > 0) break;
    j--;
  }
  const zi = j + 1;
  if (zi >= n) return null;
  return datesAsc[zi];
}

function fmtOosStartLabel(iso) {
  if (!iso) return '—';
  const p = String(iso).split('-');
  if (p.length !== 3) return String(iso) + '~';
  return `${Number(p[1])}/${Number(p[2])}~`;
}

const PERIODS = [
  { key: '7', days: 7 },
  { key: '14', days: 14 },
  { key: '30', days: 30 },
];

// 메모리 캐시
let _cache = null;
let _cacheTime = 0;
const CACHE_TTL = 10 * 60 * 1000; // 10분

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  if (_cache && (Date.now() - _cacheTime) < CACHE_TTL) {
    return res.status(200).json(_cache);
  }

  try {
    const db = getDb();
    const maxRow = await db.execute('SELECT MAX(date) AS d FROM sales');
    const rawLatest = maxRow.rows[0]?.d ?? null;

    if (!rawLatest || String(rawLatest).trim() === '') {
      return res.status(200).json({
        brands: {}, insights: [], dailyTrend: {}, dates: [],
        latestDate: null, flags: {}, brandInsights: {},
      });
    }

    const latestDate = String(rawLatest).trim().replace(/\./g, '-').replace(/\//g, '-').slice(0, 10);
    const testDate = new Date(`${latestDate}T12:00:00.000Z`);
    if (isNaN(testDate.getTime())) {
      return res.status(500).json({ error: `Invalid date: "${rawLatest}"` });
    }

        // 당월 + 이전 2개월
    const startMonth = new Date(`${latestDate}T12:00:00.000Z`);
    startMonth.setUTCMonth(startMonth.getUTCMonth() - 2);
    startMonth.setUTCDate(1);
    const start365 = startMonth.toISOString().slice(0, 10);

    const dates = dateRangeInclusive(start365, latestDate);

    // ── 핵심 쿼리 (병렬) ──
    const [todayAgg, sum7Agg, skuLatest, dailyTrendRows] = await Promise.all([
      db.execute({ sql: 'SELECT brand, SUM(sales) AS s FROM sales WHERE date = ? GROUP BY brand', args: [latestDate] }),
      db.execute({ sql: 'SELECT brand, SUM(sales) AS s FROM sales WHERE date >= ? AND date <= ? GROUP BY brand', args: [addDays(latestDate, -6), latestDate] }),
      db.execute({ sql: 'SELECT brand, sku_id, sku_name, sales, stock, status, oos_flag FROM sales WHERE date = ?', args: [latestDate] }),
      db.execute({ sql: 'SELECT brand, date, SUM(sales) AS s FROM sales WHERE date >= ? AND date <= ? GROUP BY brand, date', args: [start365, latestDate] }),
    ]);

    // ── sku_manage ──
    let flagsRows = { rows: [] };
    let watchList = { rows: [] };
    let skuManageMap = {};
    try {
      const smRows = await db.execute('SELECT * FROM sku_manage WHERE active = 1');
      if (smRows && smRows.rows && Array.isArray(smRows.rows)) {
        flagsRows = { rows: smRows.rows };
        watchList = { rows: smRows.rows };
        for (const r of smRows.rows) {
          if (r && r.sku_id) skuManageMap[r.sku_id] = r;
        }
      }
    } catch (e) {
      console.log('sku_manage:', e.message);
    }

    // ── brands 집계 ──
    const todayMap = Object.fromEntries(todayAgg.rows.map(r => [r.brand, Number(r.s)||0]));
    const sum7Map = Object.fromEntries(sum7Agg.rows.map(r => [r.brand, Number(r.s)||0]));
    const skuCountByBrand = {}, stockSumByBrand = {}, oosByBrand = {};

    for (const row of skuLatest.rows) {
      const b = row.brand;
      if (!skuCountByBrand[b]) { skuCountByBrand[b] = new Set(); stockSumByBrand[b] = 0; oosByBrand[b] = 0; }
      skuCountByBrand[b].add(String(row.sku_id));
      if (row.stock != null && row.stock !== '') stockSumByBrand[b] += Number(row.stock)||0;
      if (isInsightListedOos(row)) oosByBrand[b] += 1;
    }

    const allBrands = new Set([...Object.keys(todayMap), ...Object.keys(sum7Map), ...Object.keys(skuCountByBrand)]);
    const brands = {};
    for (const b of allBrands) {
      if (!b) continue;
      const sum7 = sum7Map[b] ?? 0;
      brands[b] = {
        todaySales: todayMap[b] ?? 0, sum7, dailyAvg: sum7 / 7,
        skuCount: skuCountByBrand[b] ? skuCountByBrand[b].size : 0,
        stockSum: stockSumByBrand[b] ?? 0, outOfStockCount: oosByBrand[b] ?? 0,
      };
    }

    // ── dailyTrend ──
    const dailyTrend = {};
    for (const row of dailyTrendRows.rows) {
      if (!dailyTrend[row.brand]) dailyTrend[row.brand] = {};
      dailyTrend[row.brand][row.date] = Number(row.s)||0;
    }

    // ── flags ──
    const flags = {};
    for (const row of flagsRows.rows) {
      flags[String(row.sku_id)] = { sku_name: row.sku_name, brand: row.brand, flag: row.flag ?? '', memo: row.memo ?? '' };
    }

    // ── insights ──
    const watchIds = watchList.rows.map(r => String(r.sku_id));
    let watchDailyMap = {};
    if (watchIds.length) {
      const placeholders = watchIds.map(() => '?').join(',');
      const watchSalesRows = await db.execute({
        sql: `SELECT sku_id, date, SUM(sales) AS s FROM sales WHERE date >= ? AND date <= ? AND sku_id IN (${placeholders}) GROUP BY sku_id, date`,
        args: [start365, latestDate, ...watchIds],
      });
      for (const row of watchSalesRows.rows) {
        const id = String(row.sku_id);
        if (!watchDailyMap[id]) watchDailyMap[id] = {};
        watchDailyMap[id][row.date] = Number(row.s)||0;
      }
    }

    const insights = watchList.rows.map(r => {
      const id = String(r.sku_id);
      const byDate = watchDailyMap[id] || {};
      const dailySales = dates.map(dt => byDate[dt] ?? 0);
      return { sku_id: id, sku_name: r.sku_name ?? '', brand: r.brand ?? '', flag: r.flag ?? '', memo: r.memo ?? '', dailySales, sum90: dailySales.reduce((a,c)=>a+c,0) };
    });

    // ── brandInsights: 단일 쿼리로 처리 (12개 → 1개) ──
    const allSkuDaily = await db.execute({
      sql: 'SELECT brand, sku_id, MAX(sku_name) AS sku_name, date, SUM(sales) AS s FROM sales WHERE date >= ? AND date <= ? GROUP BY brand, sku_id, date',
      args: [start365, latestDate],
    });

    const skuDailyMap = {};
    const rows = (allSkuDaily && allSkuDaily.rows) ? allSkuDaily.rows : [];
    for (const row of rows) {
      const k = row.brand + '||' + row.sku_id;
      if (!skuDailyMap[k]) skuDailyMap[k] = { brand: row.brand, sku_id: String(row.sku_id), sku_name: row.sku_name ?? '', dates: {} };
      skuDailyMap[k].dates[row.date] = Number(row.s)||0;
    }

    const oosLatestByBrand = {};
    for (const row of skuLatest.rows) {
      if (!isInsightListedOos(row)) continue;
      const k = row.brand + '||' + row.sku_id;
      const dm = (skuDailyMap[k] && skuDailyMap[k].dates) || {};
      const oosStartIso = findOosSalesStartIso(dates, dm);
      if (!oosLatestByBrand[row.brand]) oosLatestByBrand[row.brand] = [];
      oosLatestByBrand[row.brand].push({
        sku_id: String(row.sku_id),
        sku_name: row.sku_name ?? '',
        stock: row.stock,
        status: row.status ?? '',
        oos_start_iso: oosStartIso,
        oos_start_label: fmtOosStartLabel(oosStartIso),
      });
    }

    const brandInsights = {};
    for (const b of allBrands) brandInsights[b] = {};

    for (const { key, days } of PERIODS) {
      const curStart = addDays(latestDate, -(days - 1));
      const prevEnd = addDays(latestDate, -days);
      const prevStart = addDays(latestDate, -(days * 2 - 1));
      const topByBrand = {}, surgeByBrand = {};

      for (const k in skuDailyMap) {
        const v = skuDailyMap[k];
        let cur = 0, prev = 0;
        for (const dt in v.dates) {
          if (dt >= curStart && dt <= latestDate) cur += v.dates[dt];
          if (dt >= prevStart && dt <= prevEnd) prev += v.dates[dt];
        }
        if (!topByBrand[v.brand]) topByBrand[v.brand] = [];
        topByBrand[v.brand].push({ sku_id: v.sku_id, sku_name: v.sku_name, sales: cur });
        if (!surgeByBrand[v.brand]) surgeByBrand[v.brand] = [];
        surgeByBrand[v.brand].push({ sku_id: v.sku_id, sku_name: v.sku_name, cur: cur, prev: prev, delta: cur - prev });
      }

      for (const b of allBrands) {
        const tops = (topByBrand[b]||[]).sort((a,c) => c.sales - a.sales).slice(0, 8);
        const surges = (surgeByBrand[b]||[]).filter(x => x.prev > 0 || x.cur > 0);
        brandInsights[b][key] = {
          topSales: tops,
          oos: oosLatestByBrand[b] ?? [],
          surgeUp: [...surges].sort((a,c) => c.delta - a.delta).slice(0, 8),
          surgeDown: [...surges].sort((a,c) => a.delta - c.delta).slice(0, 8),
        };
      }
    }

    for (const b of allBrands) {
      if (!brandInsights[b]) brandInsights[b] = {};
      for (const p of PERIODS) {
        if (!brandInsights[b][p.key]) brandInsights[b][p.key] = { topSales:[], oos:[], surgeUp:[], surgeDown:[] };
      }
    }

    // ── 시트 gid (타임아웃 방지: 5초 제한) ──
    let sheetGids = {};
    try {
      let gkey = process.env.GOOGLE_PRIVATE_KEY || '';
      if (!gkey.includes('\n') && gkey.includes('\\n')) gkey = gkey.replace(/\\n/g, '\n');
      const gauth = new google.auth.JWT({ email: process.env.GOOGLE_CLIENT_EMAIL, key: gkey, scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'] });
      const authPromise = gauth.authorize();
      const timeout = new Promise((_, reject) => setTimeout(() => reject(new Error('sheets timeout')), 5000));
      await Promise.race([authPromise, timeout]);
      const sheetsApi = google.sheets({ version: 'v4', auth: gauth });
      const metaPromise = sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
      const meta = await Promise.race([metaPromise, new Promise((_, reject) => setTimeout(() => reject(new Error('sheets timeout')), 5000))]);
      for (const s of meta.data.sheets) sheetGids[s.properties.title] = s.properties.sheetId;
    } catch (e) { console.log('sheetGids skipped:', e.message); }

    let skuUrls = {};
    try {
      const urlRows = await db.execute('SELECT sku_id, url FROM sku_url_map');
      for (const r of urlRows.rows) skuUrls[r.sku_id] = r.url;
    } catch (e) {}

    const result = { brands, insights, dailyTrend, dates, latestDate, flags, brandInsights, sheetGids, skuUrls, skuManageMap };
    _cache = result;
    _cacheTime = Date.now();
    return res.status(200).json(result);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'dashboard failed' });
  }
};
