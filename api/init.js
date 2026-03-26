const { getDb } = require('../lib/db');

const STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS sales (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  date TEXT NOT NULL,
  brand TEXT NOT NULL,
  sku_id TEXT NOT NULL,
  sku_name TEXT NOT NULL,
  sales INTEGER DEFAULT 0,
  stock INTEGER,
  status TEXT,
  revenue INTEGER DEFAULT 0,
  UNIQUE(date, sku_id)
)`,
  `CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)`,
  `CREATE INDEX IF NOT EXISTS idx_sales_brand_date ON sales(brand, date)`,
  `CREATE INDEX IF NOT EXISTS idx_sales_sku ON sales(sku_id)`,
  `CREATE TABLE IF NOT EXISTS sku_manage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  brand TEXT,
  sku_id TEXT UNIQUE,
  sku_name TEXT,
  watch INTEGER DEFAULT 1,
  flag TEXT DEFAULT '',
  memo TEXT DEFAULT ''
)`,
];

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }
  try {
    const db = getDb();
    for (const sql of STATEMENTS) {
      await db.execute(sql);
    }
    return res.status(200).json({ ok: true, message: '테이블이 준비되었습니다.' });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'init failed' });
  }
};
