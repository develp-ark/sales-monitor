const { getDb } = require('../db');
const { loadBrandDaily, loadSkuMonthBefore, loadBrands } = require('../agg');

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  try {
    const db = getDb();
    await db.execute(`CREATE TABLE IF NOT EXISTS sku_exclude (
      sku_id TEXT PRIMARY KEY, sku_name TEXT, brand TEXT,
      excluded_at TEXT DEFAULT (datetime('now'))
    )`);
    await db.execute(`CREATE TABLE IF NOT EXISTS sales_monthly (
      brand TEXT NOT NULL, sku_id TEXT NOT NULL, sku_name TEXT,
      month TEXT NOT NULL, sales INTEGER DEFAULT 0,
      UNIQUE(brand, sku_id, month)
    )`);

    const brand = req.query?.brand || null;
    const exFilter = 'sku_id NOT IN (SELECT sku_id FROM sku_exclude)';

    const now = new Date();
    const y = now.getFullYear();
    const m = now.getMonth();
    var cutDate = new Date(y, m - 2, 1);
    var cutISO = cutDate.getFullYear() + '-' +
      String(cutDate.getMonth() + 1).padStart(2, '0') + '-01';

    let salesSql = `SELECT date, brand, sku_id, sku_name, sales, stock, status, oos_flag, revenue
      FROM sales WHERE ${exFilter} AND date >= ?`;
    let salesArgs = [cutISO];

    let archiveSql = `SELECT brand, sku_id, sku_name, month, sales
      FROM sales_monthly WHERE ${exFilter}`;

    if (brand) {
      salesSql += ' AND brand = ?';
      salesArgs.push(brand);
    }
    salesSql += ' ORDER BY brand, sku_id, date';

    // 3개월 이전 월별 집계 · 전체 일별 트렌드 · 브랜드 목록은 모두 sales 풀스캔이었다.
    // 셋 다 제외 SKU 를 반영한 파생 집계 테이블에서 읽는다.
    const cutMonth = cutISO.slice(0, 7);
    const [salesR, monthlyAgg, trendAgg, archiveR, brandList] = await Promise.all([
      db.execute({ sql: salesSql, args: salesArgs }),
      loadSkuMonthBefore(db, cutMonth),
      loadBrandDaily(db, '0000-01-01', '9999-12-31'),
      db.execute({ sql: archiveSql, args: [] }),
      loadBrands(db)
    ]);

    var monthlyMap = {};
    archiveR.rows.forEach(function(r) {
      var key = r.brand + '|' + r.sku_id + '|' + r.month;
      monthlyMap[key] = r;
    });
    monthlyAgg.forEach(function(r) {
      if (brand && r.brand !== brand) return;
      var key = r.brand + '|' + r.sku_id + '|' + r.month;
      monthlyMap[key] = { brand: r.brand, sku_id: r.sku_id, sku_name: r.sku_name, month: r.month, sales: Number(r.sales) || 0 };
    });

    return res.status(200).json({
      ok: true,
      brands: brandList,
      count: salesR.rows.length,
      rows: salesR.rows,
      dailyTrend: trendAgg.map(function(r) { return { brand: r.brand, date: r.date, s: Number(r.sales) || 0 }; }),
      monthly: Object.values(monthlyMap)
    });
  } catch (e) {
    console.error('[EXPORT]', e);
    return res.status(500).json({ error: e.message || 'export failed' });
  }
};
