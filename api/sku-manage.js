const { createClient } = require('@libsql/client');

function getDb() {
  return createClient({ url: process.env.TURSO_URL, authToken: process.env.TURSO_AUTH_TOKEN });
}

module.exports = async function handler(req, res) {
  try {
    const db = getDb();

    await db.execute(`CREATE TABLE IF NOT EXISTS sku_manage (
      sku_id TEXT PRIMARY KEY,
      brand TEXT,
      sku_name TEXT,
      pid TEXT,
      iid TEXT,
      vid TEXT,
      product_url TEXT,
      flag TEXT,
      memo TEXT,
      active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    )`);

    // GET
    if (req.method === 'GET') {
      const brand = (req.query && req.query.brand) || '';
      const showInactive = req.query && req.query.inactive === '1';
      let sql = 'SELECT * FROM sku_manage';
      let args = [];
      let conds = [];
      if (brand) { conds.push('brand = ?'); args.push(brand); }
      if (!showInactive) { conds.push('active = 1'); }
      if (conds.length) sql += ' WHERE ' + conds.join(' AND ');
      sql += ' ORDER BY brand, sku_name';
      const result = await db.execute({ sql, args });
      return res.status(200).json({ ok: true, data: result.rows });
    }

    // POST
    if (req.method === 'POST') {
      let b = req.body;
      if (typeof b === 'string') b = JSON.parse(b);
      if (!b || !b.sku_id) return res.status(400).json({ error: 'sku_id required' });
      await db.execute({
        sql: `INSERT INTO sku_manage (sku_id, brand, sku_name, pid, iid, vid, product_url, flag, memo, active, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
              ON CONFLICT(sku_id) DO UPDATE SET
                brand=excluded.brand, sku_name=excluded.sku_name,
                pid=excluded.pid, iid=excluded.iid, vid=excluded.vid,
                product_url=excluded.product_url, flag=excluded.flag,
                memo=excluded.memo, active=excluded.active,
                updated_at=datetime('now')`,
        args: [b.sku_id, b.brand||'', b.sku_name||'', b.pid||null, b.iid||null, b.vid||null,
               b.product_url||null, b.flag||null, b.memo||null, b.active!=null?b.active:1]
      });
      return res.status(200).json({ ok: true });
    }

    // DELETE
    if (req.method === 'DELETE') {
      let b = req.body;
      if (typeof b === 'string') b = JSON.parse(b);
      if (!b || !b.sku_id) return res.status(400).json({ error: 'sku_id required' });
      if (b.hard === true) {
        await db.execute({ sql: 'DELETE FROM sku_manage WHERE sku_id = ?', args: [b.sku_id] });
      } else {
        await db.execute({ sql: "UPDATE sku_manage SET active=0, updated_at=datetime('now') WHERE sku_id=?", args: [b.sku_id] });
      }
      return res.status(200).json({ ok: true });
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (e) {
    console.error('[sku-manage error]', e.message);
    return res.status(500).json({ ok: false, error: e.message });
  }
};
