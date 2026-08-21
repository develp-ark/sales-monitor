const { getDb } = require('../db');
const { touchFirstSeen } = require('../agg');

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  try {
    const db = getDb();
    await db.execute(`CREATE TABLE IF NOT EXISTS sales_monthly (
      brand TEXT NOT NULL, sku_id TEXT NOT NULL, sku_name TEXT,
      month TEXT NOT NULL, sales INTEGER DEFAULT 0,
      UNIQUE(brand, sku_id, month)
    )`);

    let body = '';
    await new Promise((r) => { req.on('data', (c) => body += c); req.on('end', r); });
    const parsed = JSON.parse(body);

    const brand = parsed.brand;
    const items = parsed.items;

    if (!brand) return res.status(400).json({ error: 'brand required' });
    if (!items || !items.length) return res.status(400).json({ error: 'no data' });

    const BATCH_SIZE = 100;
    let totalRows = 0;
    let batch = [];
    const firstSeenRows = [];

    for (const item of items) {
      if (!item.sku_id) continue;
      for (const [month, sales] of Object.entries(item.months || {})) {
        batch.push({
          sql: 'INSERT OR REPLACE INTO sales_monthly (brand, sku_id, sku_name, month, sales) VALUES (?, ?, ?, ?, ?)',
          args: [brand, String(item.sku_id), item.sku_name || '', month, parseInt(sales, 10) || 0]
        });
        firstSeenRows.push({ brand, sku_id: String(item.sku_id), date: month + '-01' });
        totalRows++;
        if (batch.length >= BATCH_SIZE) {
          await db.batch(batch);
          batch = [];
        }
      }
    }

    if (batch.length) await db.batch(batch);

    try {
      await touchFirstSeen(db, firstSeenRows);
    } catch (e) {
      console.error('[MONTHLY] 집계 테이블 갱신 실패:', e.message);
    }

    console.log('[MONTHLY] ' + brand + ': ' + totalRows + ' rows saved');
    return res.status(200).json({
      ok: true,
      brand,
      rows: totalRows,
      message: brand + ' 월별 데이터 ' + totalRows + '건 저장 완료'
    });
  } catch (e) {
    console.error('[MONTHLY ERROR]', e);
    return res.status(500).json({ error: e.message || 'upload failed' });
  }
};
