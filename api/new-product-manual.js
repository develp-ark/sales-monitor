const { getDb } = require('../lib/db');

module.exports = async (req, res) => {
  const db = getDb();

  await db.execute(`CREATE TABLE IF NOT EXISTS sku_new_manual (
    sku_id TEXT PRIMARY KEY,
    sku_name TEXT,
    brand TEXT,
    added_at TEXT DEFAULT (datetime('now'))
  )`);

  if (req.method === 'GET') {
    const rows = await db.execute('SELECT sku_id, sku_name, brand, added_at FROM sku_new_manual ORDER BY brand, sku_name');
    return res.status(200).json({ ok: true, count: rows.rows.length, data: rows.rows });
  }

  if (req.method === 'POST') {
    let body = '';
    await new Promise((r) => { req.on('data', (c) => { body += c; }); req.on('end', r); });
    const parsed = JSON.parse(body);
    const items = Array.isArray(parsed) ? parsed : [parsed];

    let added = 0;
    for (const item of items) {
      if (!item.sku_id) continue;
      const skuId = String(item.sku_id).trim();

      let brand = item.brand || '';
      let skuName = item.sku_name || '';
      if (!brand || !skuName) {
        try {
          const found = await db.execute({
            sql: 'SELECT brand, sku_name FROM sales WHERE sku_id = ? LIMIT 1',
            args: [skuId],
          });
          if (found.rows.length > 0) {
            if (!brand) brand = found.rows[0].brand || '';
            if (!skuName) skuName = found.rows[0].sku_name || '';
          }
        } catch (e) { /* ignore */ }
      }

      try {
        await db.execute({
          sql: 'INSERT OR REPLACE INTO sku_new_manual (sku_id, sku_name, brand, added_at) VALUES (?, ?, ?, datetime(\'now\'))',
          args: [skuId, skuName, brand],
        });
        added++;
      } catch (e) { /* ignore */ }
    }
    return res.status(200).json({ ok: true, added });
  }

  if (req.method === 'DELETE') {
    let body = '';
    await new Promise((r) => { req.on('data', (c) => { body += c; }); req.on('end', r); });
    const parsed = JSON.parse(body);
    const ids = Array.isArray(parsed.sku_ids) ? parsed.sku_ids : [parsed.sku_id];

    let removed = 0;
    for (const id of ids) {
      if (!id) continue;
      await db.execute({ sql: 'DELETE FROM sku_new_manual WHERE sku_id = ?', args: [String(id)] });
      removed++;
    }
    return res.status(200).json({ ok: true, removed });
  }

  res.setHeader('Allow', 'GET, POST, DELETE');
  return res.status(405).json({ error: 'Method Not Allowed' });
};
