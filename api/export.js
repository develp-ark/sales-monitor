const { getDb } = require('../lib/db');

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

    let monthlySalesSql = `SELECT brand, sku_id, sku_name,
      substr(date,1,7) AS month, SUM(sales) AS sales
      FROM sales WHERE ${exFilter} AND date < ?
      GROUP BY brand, sku_id, substr(date,1,7)`;
    let monthlySalesArgs = [cutISO];

    let trendSql = `SELECT brand, date, SUM(sales) AS s
      FROM sales WHERE ${exFilter}
      GROUP BY brand, date ORDER BY date`;

    let archiveSql = `SELECT brand, sku_id, sku_name, month, sales
      FROM sales_monthly WHERE ${exFilter}`;

    let brandsSql = 'SELECT DISTINCT brand FROM sales ORDER BY brand';

    if (brand) {
      salesSql += ' AND brand = ?';
      salesArgs.push(brand);
      monthlySalesSql += ' AND brand = ?';
      monthlySalesArgs.push(brand);
    }
    salesSql += ' ORDER BY brand, sku_id, date';

    const [salesR, monthlySalesR, trendR, archiveR, brandsR] = await Promise.all([
      db.execute({ sql: salesSql, args: salesArgs }),
      db.execute({ sql: monthlySalesSql, args: monthlySalesArgs }),
      db.execute({ sql: trendSql, args: [] }),
      db.execute({ sql: archiveSql, args: [] }),
      db.execute({ sql: brandsSql, args: [] })
    ]);

    var monthlyMap = {};
    archiveR.rows.forEach(function(r) {
      var key = r.brand + '|' + r.sku_id + '|' + r.month;
      monthlyMap[key] = r;
    });
    monthlySalesR.rows.forEach(function(r) {
      var key = r.brand + '|' + r.sku_id + '|' + r.month;
      monthlyMap[key] = { brand: r.brand, sku_id: r.sku_id, sku_name: r.sku_name, month: r.month, sales: Number(r.sales) || 0 };
    });

    return res.status(200).json({
      ok: true,
      brands: brandsR.rows.map((r) => r.brand),
      count: salesR.rows.length,
      rows: salesR.rows,
      dailyTrend: trendR.rows,
      monthly: Object.values(monthlyMap)
    });
  } catch (e) {
    console.error('[EXPORT]', e);
    return res.status(500).json({ error: e.message || 'export failed' });
  }
};
