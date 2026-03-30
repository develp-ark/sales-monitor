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
  while (cur <= end) {
    out.push(cur);
    cur = addDays(cur, 1);
  }
  return out;
}

function isOosRow(stock, status) {
  const st = status != null ? String(status) : '';
  if (st.includes('품절')) return true;
  if (stock == null) return false;
  return Number(stock) === 0;
}

const PERIODS = [
  { key: '7', days: 7 },
  { key: '14', days: 14 },
  { key: '30', days: 30 },
  { key: '90', days: 90 },
  { key: '180', days: 180 },
  { key: '365', days: 365 },
];

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }
  try {
    const db = getDb();
    const maxRow = await db.execute('SELECT MAX(date) AS d FROM sales');
    const rawLatest = maxRow.rows[0]?.d ?? null;

    // ── 데이터 없음 또는 유효하지 않은 날짜 처리 ──
    if (!rawLatest || String(rawLatest).trim() === '') {
      
      return res.status(200).json({
        brands: {},
        insights: [],
        dailyTrend: {},
        dates: [],
        latestDate: null,
        flags: {},
        brandInsights: {},
      });
    }

    // 날짜 형식 정규화
    const latestDate = String(rawLatest).trim().replace(/\./g, '-').replace(/\//g, '-').slice(0, 10);

    // 유효성 검증
    const testDate = new Date(`${latestDate}T12:00:00.000Z`);
    if (isNaN(testDate.getTime())) {
      return res.status(500).json({
        error: `Invalid date in DB: "${rawLatest}" → "${latestDate}"`,
      });
    }

    const start365 = addDays(latestDate, -364);
    const dates = dateRangeInclusive(start365, latestDate);

    const [todayAgg, sum7Agg, skuLatest, dailyTrendRows] = await Promise.all([
      db.execute({
        sql: 'SELECT brand, SUM(sales) AS s FROM sales WHERE date = ? GROUP BY brand',
        args: [latestDate],
      }),
      db.execute({
        sql: 'SELECT brand, SUM(sales) AS s FROM sales WHERE date >= ? AND date <= ? GROUP BY brand',
        args: [addDays(latestDate, -6), latestDate],
      }),
      db.execute({
        sql: 'SELECT brand, sku_id, sku_name, sales, stock, status FROM sales WHERE date = ?',
        args: [latestDate],
      }),
      db.execute({
        sql: 'SELECT brand, date, SUM(sales) AS s FROM sales WHERE date >= ? AND date <= ? GROUP BY brand, date',
        args: [start365, latestDate],
      }),
    ]);

    let flagsRows = { rows: [] };
    let watchList = { rows: [] };
    let skuManageMap = {};
    try {
      flagsRows = await db.execute('SELECT sku_id, sku_name, brand, flag, memo FROM sku_manage WHERE active = 1');
      watchList = await db.execute({ sql: 'SELECT sku_id, sku_name, brand, flag, memo FROM sku_manage WHERE active = 1' });
      const smRows = await db.execute('SELECT * FROM sku_manage WHERE active = 1');
      for (const r of smRows.rows) {
        skuManageMap[r.sku_id] = r;
      }
    } catch (e) {
      console.log('sku_manage query failed (table may not exist):', e.message);
    }

    const todayMap = Object.fromEntries(todayAgg.rows.map((r) => [r.brand, Number(r.s) || 0]));
    const sum7Map = Object.fromEntries(sum7Agg.rows.map((r) => [r.brand, Number(r.s) || 0]));

    const skuCountByBrand = {};
    const stockSumByBrand = {};
    const oosByBrand = {};

    for (const row of skuLatest.rows) {
      const b = row.brand;
      if (!skuCountByBrand[b]) {
        skuCountByBrand[b] = new Set();
        stockSumByBrand[b] = 0;
        oosByBrand[b] = 0;
      }
      skuCountByBrand[b].add(String(row.sku_id));
      if (row.stock != null && row.stock !== '') {
        stockSumByBrand[b] += Number(row.stock) || 0;
      }
      if (isOosRow(row.stock, row.status)) {
        oosByBrand[b] += 1;
      }
    }

    const allBrands = new Set([
      ...Object.keys(todayMap),
      ...Object.keys(sum7Map),
      ...Object.keys(skuCountByBrand),
    ]);

    const brands = {};
    for (const b of allBrands) {
      if (!b) continue;
      const sum7 = sum7Map[b] ?? 0;
      brands[b] = {
        todaySales: todayMap[b] ?? 0,
        sum7,
        dailyAvg: sum7 / 7,
        skuCount: skuCountByBrand[b] ? skuCountByBrand[b].size : 0,
        stockSum: stockSumByBrand[b] ?? 0,
        outOfStockCount: oosByBrand[b] ?? 0,
      };
    }

    const dailyTrend = {};
    for (const row of dailyTrendRows.rows) {
      const b = row.brand;
      if (!dailyTrend[b]) dailyTrend[b] = {};
      dailyTrend[b][row.date] = Number(row.s) || 0;
    }

    const flags = {};
    for (const row of flagsRows.rows) {
      flags[String(row.sku_id)] = {
        sku_name: row.sku_name,
        brand: row.brand,
        watch: Number(row.watch) || 0,
        flag: row.flag ?? '',
        memo: row.memo ?? '',
      };
    }

    const watchIds = watchList.rows.map((r) => String(r.sku_id));
    let watchSalesRows = { rows: [] };
    if (watchIds.length) {
      const placeholders = watchIds.map(() => '?').join(',');
      watchSalesRows = await db.execute({
        sql: `SELECT sku_id, date, SUM(sales) AS s FROM sales WHERE date >= ? AND date <= ? AND sku_id IN (${placeholders}) GROUP BY sku_id, date`,
        args: [start365, latestDate, ...watchIds],
      });
    }

    const watchDailyMap = {};
    for (const row of watchSalesRows.rows) {
      const id = String(row.sku_id);
      if (!watchDailyMap[id]) watchDailyMap[id] = {};
      watchDailyMap[id][row.date] = Number(row.s) || 0;
    }

    const insights = watchList.rows.map((r) => {
      const id = String(r.sku_id);
      const byDate = watchDailyMap[id] || {};
      const dailySales = dates.map((dt) => byDate[dt] ?? 0);
      return {
        sku_id: id,
        sku_name: r.sku_name ?? '',
        brand: r.brand ?? '',
        flag: r.flag ?? '',
        memo: r.memo ?? '',
        dailySales,
        sum90: dailySales.reduce((a, c) => a + c, 0),
      };
    });

    const oosLatestByBrand = {};
    for (const row of skuLatest.rows) {
      if (!isOosRow(row.stock, row.status)) continue;
      const br = row.brand;
      if (!oosLatestByBrand[br]) oosLatestByBrand[br] = [];
      oosLatestByBrand[br].push({
        sku_id: String(row.sku_id),
        sku_name: row.sku_name ?? '',
        stock: row.stock,
        status: row.status ?? '',
      });
    }

    const brandInsights = {};
    for (const b of allBrands) brandInsights[b] = {};

    const periodQueries = PERIODS.map(async ({ key, days }) => {
      const curStart = addDays(latestDate, -(days - 1));
      const prevEnd = addDays(latestDate, -days);
      const prevStart = addDays(latestDate, -(days * 2 - 1));

      const [curAgg, prevAgg] = await Promise.all([
        db.execute({
          sql: `SELECT brand, sku_id, MAX(sku_name) AS sku_name, SUM(sales) AS s
                FROM sales WHERE date >= ? AND date <= ? GROUP BY brand, sku_id`,
          args: [curStart, latestDate],
        }),
        db.execute({
          sql: `SELECT brand, sku_id, SUM(sales) AS s
                FROM sales WHERE date >= ? AND date <= ? GROUP BY brand, sku_id`,
          args: [prevStart, prevEnd],
        }),
      ]);

      const prevMap = new Map();
      for (const row of prevAgg.rows) {
        prevMap.set(`${row.brand}||${row.sku_id}`, Number(row.s) || 0);
      }

      const byBrandSku = new Map();
      for (const row of curAgg.rows) {
        const k = `${row.brand}||${row.sku_id}`;
        byBrandSku.set(k, {
          brand: row.brand,
          sku_id: String(row.sku_id),
          sku_name: row.sku_name ?? '',
          cur: Number(row.s) || 0,
          prev: prevMap.get(k) ?? 0,
        });
      }

      const topByBrand = {};
      const surgeByBrand = {};
      for (const [, v] of byBrandSku) {
        if (!topByBrand[v.brand]) topByBrand[v.brand] = [];
        topByBrand[v.brand].push({ sku_id: v.sku_id, sku_name: v.sku_name, sales: v.cur });
        const delta = v.cur - v.prev;
        if (!surgeByBrand[v.brand]) surgeByBrand[v.brand] = [];
        surgeByBrand[v.brand].push({
          sku_id: v.sku_id,
          sku_name: v.sku_name,
          cur: v.cur,
          prev: v.prev,
          delta,
        });
      }

      for (const b of allBrands) {
        const tops = (topByBrand[b] || []).sort((a, c) => c.sales - a.sales).slice(0, 8);
        const surges = (surgeByBrand[b] || []).filter((x) => x.prev > 0 || x.cur > 0);
        const surgeUp = [...surges].sort((a, c) => c.delta - a.delta).slice(0, 8);
        const surgeDown = [...surges].sort((a, c) => a.delta - c.delta).slice(0, 8);
        brandInsights[b][key] = {
          topSales: tops,
          oos: oosLatestByBrand[b] ?? [],
          surgeUp,
          surgeDown,
        };
      }
    });

    await Promise.all(periodQueries);

    // ── 방어: null 체크 ──
    for (const b of allBrands) {
      if (!brandInsights[b]) brandInsights[b] = {};
      for (const p of PERIODS) {
        if (!brandInsights[b][p.key]) {
          brandInsights[b][p.key] = { topSales: [], oos: [], surgeUp: [], surgeDown: [] };
        }
      }
    }

    // ── 시트 gid 수집 ──
    let sheetGids = {};
    try {
      let gkey = process.env.GOOGLE_PRIVATE_KEY || '';
      if (!gkey.includes('\n') && gkey.includes('\\n')) gkey = gkey.replace(/\\n/g, '\n');
      const gauth = new google.auth.JWT({
        email: process.env.GOOGLE_CLIENT_EMAIL,
        key: gkey,
        scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
      });
      await gauth.authorize();
      const sheetsApi = google.sheets({ version: 'v4', auth: gauth });
      const meta = await sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
      for (const s of meta.data.sheets) {
        sheetGids[s.properties.title] = s.properties.sheetId;
      }
    } catch (e) { console.error('sheetGids error:', e.message); }

    let skuUrls = {};
    try {
      const urlRows = await db.execute('SELECT sku_id, url FROM sku_url_map');
      for (const r of urlRows.rows) {
        skuUrls[r.sku_id] = r.url;
      }
    } catch (e) { /* table may not exist yet */ }

    return res.status(200).json({
      brands,
      insights,
      dailyTrend,
      dates,
      latestDate,
      flags,
      brandInsights,
      sheetGids,
      skuUrls,
      skuManageMap,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'dashboard failed' });
  }
};
