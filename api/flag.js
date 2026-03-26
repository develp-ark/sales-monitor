const { getDb } = require('../lib/db');

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }
  try {
    let body = '';
    await new Promise((resolve, reject) => {
      req.on('data', (c) => (body += c));
      req.on('end', resolve);
      req.on('error', reject);
    });
    const json = JSON.parse(body || '{}');
    const { skuId, skuName, flag, memo } = json;
    if (!skuId) {
      return res.status(400).json({ error: 'skuId가 필요합니다.' });
    }
    const db = getDb();
    await db.execute({
      sql: `INSERT INTO sku_manage (brand, sku_id, sku_name, watch, flag, memo)
            VALUES (NULL, ?, ?, 1, ?, ?)
            ON CONFLICT(sku_id) DO UPDATE SET
              sku_name = COALESCE(excluded.sku_name, sku_name),
              flag = excluded.flag,
              memo = excluded.memo`,
      args: [String(skuId), skuName != null ? String(skuName) : '', flag != null ? String(flag) : '', memo != null ? String(memo) : ''],
    });
    return res.status(200).json({ ok: true });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'flag failed' });
  }
};
