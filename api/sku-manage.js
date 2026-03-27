const { getDb } = require('../lib/db');

const INIT_SQL = `CREATE TABLE IF NOT EXISTS sku_manage (
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
)`;

module.exports = async function handler(req, res) {
  const db = getDb();
  await db.execute(INIT_SQL);

  if (req.method === 'GET') {
    const brand = req.query.brand || '';
    const showInactive = req.query.inactive === '1';
    let sql = 'SELECT * FROM sku_manage';
    let args = [];
    let conditions = [];
    if (brand) { conditions.push('brand = ?'); args.push(brand); }
    if (!showInactive) { conditions.push('active = 1'); }
    if (conditions.length) sql += ' WHERE ' + conditions.join(' AND ');
    sql += ' ORDER BY brand, sku_name';
    const rows = await db.execute({ sql, args });
    return res.json({ ok: true, data: rows.rows });
  }

  if (req.method === 'POST') {
    const b = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    if (!b.sku_id) return res.status(400).json({ error: 'sku_id required' });
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
    return res.json({ ok: true });
  }

  if (req.method === 'DELETE') {
    const b = typeof req.body === 'string' ? JSON.parse(req.body) : req.body;
    if (!b.sku_id) return res.status(400).json({ error: 'sku_id required' });
    if (b.hard === true) {
      await db.execute({ sql: 'DELETE FROM sku_manage WHERE sku_id = ?', args: [b.sku_id] });
    } else {
      await db.execute({ sql: "UPDATE sku_manage SET active=0, updated_at=datetime('now') WHERE sku_id=?", args: [b.sku_id] });
    }
    return res.json({ ok: true });
  }

  res.status(405).json({ error: 'Method not allowed' });
};
