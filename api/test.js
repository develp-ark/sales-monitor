const { getDb } = require('../lib/db');

module.exports = async (req, res) => {
  try {
    const db = getDb();
    
    // 인덱스 생성 (이미 있으면 무시)
    await db.execute('CREATE INDEX IF NOT EXISTS idx_sales_date ON sales(date)');
    await db.execute('CREATE INDEX IF NOT EXISTS idx_sales_brand_date ON sales(brand, date)');
    await db.execute('CREATE INDEX IF NOT EXISTS idx_sales_brand_sku_date ON sales(brand, sku_id, date)');
    
    const count = await db.execute('SELECT COUNT(*) AS cnt FROM sales');
    const maxDate = await db.execute('SELECT MAX(date) AS d FROM sales');
    const minDate = await db.execute('SELECT MIN(date) AS d FROM sales');
    
    return res.status(200).json({
      ok: true,
      indexes: 'created',
      totalRows: count.rows[0]?.cnt,
      minDate: minDate.rows[0]?.d,
      maxDate: maxDate.rows[0]?.d,
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
};
