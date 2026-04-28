const { getDb } = require('../db');

module.exports = async (req, res) => {
  const db = getDb();

  // 테이블 보장
  await db.execute(`CREATE TABLE IF NOT EXISTS sku_exclude (
    sku_id TEXT PRIMARY KEY,
    sku_name TEXT,
    brand TEXT,
    excluded_at TEXT DEFAULT (datetime('now'))
  )`);

  // GET: 제외 목록 조회
  if (req.method === 'GET') {
    const rows = await db.execute('SELECT sku_id, sku_name, brand, excluded_at FROM sku_exclude ORDER BY brand, sku_name');
    return res.status(200).json({ ok: true, count: rows.rows.length, data: rows.rows });
  }

  // POST: 제외 SKU 추가 (단건 또는 다건)
  if (req.method === 'POST') {
    let body = '';
    await new Promise((r) => { req.on('data', (c) => body += c); req.on('end', r); });
    const parsed = JSON.parse(body);
    const items = Array.isArray(parsed) ? parsed : [parsed];

    let added = 0;
    for (const item of items) {
      if (!item.sku_id) continue;
      const skuId = String(item.sku_id).trim();

      // 브랜드 자동 조회
      let brand = item.brand || '';
      let skuName = item.sku_name || '';
      if (!brand || !skuName) {
        try {
          const found = await db.execute({
            sql: 'SELECT brand, sku_name FROM sales WHERE sku_id = ? LIMIT 1',
            args: [skuId]
          });
          if (found.rows.length > 0) {
            if (!brand) brand = found.rows[0].brand || '';
            if (!skuName) skuName = found.rows[0].sku_name || '';
          }
        } catch (e) { /* ignore */ }
      }

      try {
        await db.execute({
          sql: 'INSERT OR REPLACE INTO sku_exclude (sku_id, sku_name, brand, excluded_at) VALUES (?, ?, ?, datetime(\'now\'))',
          args: [skuId, skuName, brand]
        });
        added++;
      } catch (e) { /* duplicate ignore */ }
    }
    return res.status(200).json({ ok: true, added });
  }

  // DELETE: 제외 해제
  if (req.method === 'DELETE') {
    let body = '';
    await new Promise((r) => { req.on('data', (c) => body += c); req.on('end', r); });
    const parsed = JSON.parse(body);
    const ids = Array.isArray(parsed.sku_ids) ? parsed.sku_ids : [parsed.sku_id];

    let removed = 0;
    for (const id of ids) {
      if (!id) continue;
      await db.execute({ sql: 'DELETE FROM sku_exclude WHERE sku_id = ?', args: [String(id)] });
      removed++;
    }
    return res.status(200).json({ ok: true, removed });
  }

  res.setHeader('Allow', 'GET, POST, DELETE');
  return res.status(405).json({ error: 'Method Not Allowed' });
};
