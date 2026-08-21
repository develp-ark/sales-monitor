const { getDb } = require('../db');
const { google } = require('googleapis');
const { loadBrandDaily, NOT_EXCLUDED } = require('../agg');

const SPREADSHEET_ID = '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw';

function addDays(base, n) {
  const d = new Date(`${base}T12:00:00.000Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

function dateRangeInclusive(start, end) {
  const out = [];
  let cur = new Date(`${start}T12:00:00.000Z`);
  const e = new Date(`${end}T12:00:00.000Z`);
  while (cur <= e) { out.push(cur.toISOString().slice(0, 10)); cur.setUTCDate(cur.getUTCDate() + 1); }
  return out;
}

function isOrderableStatus(s) { return !s || (!s.includes('판매중단') && !s.includes('발주불가')); }
function isOosFlagYes(row) { return String(row.oos_flag || '').toUpperCase() === 'Y'; }
function isInsightListedOos(row) {
  if (!isOrderableStatus(row.status)) return false;
  return isOosFlagYes(row) || String(row.status || '').includes('일시품절') || String(row.status || '').includes('품절');
}

let _cache = null;
let _cacheTime = 0;
const CACHE_TTL = 5 * 60 * 1000;

module.exports = async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.searchParams.has('purge')) {
    _cache = null;
    _cacheTime = 0;
  }

  if (_cache && (Date.now() - _cacheTime < CACHE_TTL)) {
    return res.status(200).json(_cache);
  }

  try {
    const t0 = Date.now();
    const db = getDb();
    console.log('[DASH] getDb:', Date.now() - t0, 'ms');

    const t1 = Date.now();
    const maxRow = await db.execute("SELECT MAX(date) AS d FROM sales");
    console.log('[DASH] maxRow:', Date.now() - t1, 'ms');
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

    const start90 = addDays(latestDate, -29);

    const dates = dateRangeInclusive(start90, latestDate);

    // 시트 메타 + SKU URL 맵 (병렬, 타임아웃)
    const sheetGidsPromise = (async () => {
      const out = {};
      try {
        let gkey = process.env.GOOGLE_PRIVATE_KEY || '';
        if (!gkey.includes('\n') && gkey.includes('\\n')) gkey = gkey.replace(/\\n/g, '\n');
        const gauth = new google.auth.JWT({
          email: process.env.GOOGLE_CLIENT_EMAIL, key: gkey,
          scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly']
        });
        await Promise.race([gauth.authorize(), new Promise((_, r) => setTimeout(() => r(new Error('timeout')), 5000))]);
        const sheetsApi = google.sheets({ version: 'v4', auth: gauth });
        const meta = await Promise.race([
          sheetsApi.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID }),
          new Promise((_, r) => setTimeout(() => r(new Error('timeout')), 5000))
        ]);
        for (const s of meta.data.sheets) out[s.properties.title] = s.properties.sheetId;
      } catch (e) { console.log('sheetGids skipped:', e.message); }
      return out;
    })();

    const skuUrlsPromise = (async () => {
      const skuUrls = {};
      try {
        const urlRows = await db.execute('SELECT sku_id, url FROM sku_url_map');
        for (const r of urlRows.rows) skuUrls[r.sku_id] = r.url;
      } catch (e) {}
      return skuUrls;
    })();

    // 브랜드 일별 합계는 사전 집계 테이블에서 한 번만 읽고 오늘/7일/트렌드를 모두 파생시킨다.
    // (기존에는 sales 를 30일치 + 7일치 + 당일치로 세 번 스캔했다)
    const t2 = Date.now();
    const [dailyBrandRows, skuLatest] = await Promise.all([
      loadBrandDaily(db, start90, latestDate),
      db.execute({
        sql: `SELECT brand, sku_id, sku_name, sales, stock, status, oos_flag FROM sales
              WHERE date = ? AND ${NOT_EXCLUDED}`,
        args: [latestDate],
      }),
    ]);
    console.log('[DASH] queries:', Date.now() - t2, 'ms');

    const sum7Start = addDays(latestDate, -6);
    const todayAgg = { rows: [] };
    const sum7Acc = new Map();
    const dailyTrendRows = { rows: dailyBrandRows };
    for (const row of dailyBrandRows) {
      const v = Number(row.sales) || 0;
      if (row.date === latestDate) todayAgg.rows.push({ brand: row.brand, s: v });
      if (row.date >= sum7Start && row.date <= latestDate) {
        sum7Acc.set(row.brand, (sum7Acc.get(row.brand) || 0) + v);
      }
    }
    const sum7Agg = { rows: [...sum7Acc].map(([brand, s]) => ({ brand, s })) };

    // flags, skuManageMap
    let flagsRows = { rows: [] };
    let skuManageMap = {};
    const t3 = Date.now();
    try {
      const smRows = await db.execute('SELECT * FROM sku_manage WHERE active = 1');
      if (smRows && smRows.rows && Array.isArray(smRows.rows)) {
        flagsRows = { rows: smRows.rows };
        for (const r of smRows.rows) {
          if (r && r.sku_id) skuManageMap[r.sku_id] = r;
        }
      }
    } catch (e) { console.log('sku_manage:', e.message); }
    console.log('[DASH] sku_manage:', Date.now() - t3, 'ms');

    // 브랜드 요약
    const todayMap = Object.fromEntries(todayAgg.rows.map(r => [r.brand, Number(r.s) || 0]));
    const sum7Map = Object.fromEntries(sum7Agg.rows.map(r => [r.brand, Number(r.s) || 0]));
    const skuCountByBrand = {}, stockSumByBrand = {}, oosByBrand = {};

    for (const row of skuLatest.rows) {
      const b = row.brand;
      if (!skuCountByBrand[b]) { skuCountByBrand[b] = new Set(); stockSumByBrand[b] = 0; oosByBrand[b] = 0; }
      skuCountByBrand[b].add(String(row.sku_id));
      if (row.stock != null && row.stock !== '') stockSumByBrand[b] += Number(row.stock) || 0;
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

    // 일별 트렌드
    const dailyTrend = {};
    for (const row of dailyTrendRows.rows) {
      if (!dailyTrend[row.brand]) dailyTrend[row.brand] = {};
      dailyTrend[row.brand][row.date] = Number(row.sales) || 0;
    }

    // flags
    const flags = {};
    for (const row of flagsRows.rows) {
      flags[String(row.sku_id)] = { sku_name: row.sku_name, brand: row.brand, flag: row.flag ?? '', memo: row.memo ?? '' };
    }

    const t4 = Date.now();
    const [sheetGids, skuUrls] = await Promise.all([sheetGidsPromise, skuUrlsPromise]);
    console.log('[DASH] sheetGids+urls:', Date.now() - t4, 'ms');

    // brandInsights는 빈 객체로 — /api/insight에서 별도 로드
    const brandInsights = {};
    for (const b of allBrands) {
      brandInsights[b] = { '7': { topSales: [], oos: [], surgeUp: [], surgeDown: [] } };
    }

    const result = { brands, insights: [], dailyTrend, dates, latestDate, flags, brandInsights, sheetGids, skuUrls, skuManageMap };
    _cache = result;
    _cacheTime = Date.now();
    return res.status(200).json(result);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'dashboard failed' });
  }
};
