const { getDb } = require('../lib/db');

module.exports = async function handler(req, res) {
  try {
    const db = getDb();

    await db.execute(`CREATE TABLE IF NOT EXISTS sku_manage (
      sku_id TEXT PRIMARY KEY,
      brand TEXT,
      sku_name TEXT,
      base_price INTEGER,
      current_price INTEGER,
      price_checked_at TEXT,
      pid TEXT,
      iid TEXT,
      vid TEXT,
      product_url TEXT,
      flag TEXT,
      memo TEXT,
      collect_cycle INTEGER DEFAULT 7,
      last_collected TEXT,
      active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    )`);

    const alters = [
      'ALTER TABLE sku_manage ADD COLUMN pid TEXT',
      'ALTER TABLE sku_manage ADD COLUMN iid TEXT',
      'ALTER TABLE sku_manage ADD COLUMN vid TEXT',
      'ALTER TABLE sku_manage ADD COLUMN product_url TEXT',
      'ALTER TABLE sku_manage ADD COLUMN active INTEGER DEFAULT 1',
      'ALTER TABLE sku_manage ADD COLUMN created_at TEXT',
      'ALTER TABLE sku_manage ADD COLUMN updated_at TEXT',
      'ALTER TABLE sku_manage ADD COLUMN memo TEXT',
      'ALTER TABLE sku_manage ADD COLUMN base_price INTEGER',
      'ALTER TABLE sku_manage ADD COLUMN current_price INTEGER',
      'ALTER TABLE sku_manage ADD COLUMN price_checked_at TEXT',
      'ALTER TABLE sku_manage ADD COLUMN collect_cycle INTEGER DEFAULT 7',
      'ALTER TABLE sku_manage ADD COLUMN last_collected TEXT',
    ];
    for (const sql of alters) {
      try { await db.execute(sql); } catch(e) { /* already exists */ }
    }

    // GET
    if (req.method === 'GET') {
      const brand = (req.query && req.query.brand) || '';
      const flag = (req.query && req.query.flag) || '';
      const showInactive = req.query && req.query.inactive === '1';
      const dueOnly = req.query && req.query.due === '1';
      let sql = 'SELECT * FROM sku_manage';
      let args = [];
      let conds = [];
      if (brand) { conds.push('brand = ?'); args.push(brand); }
      if (flag) { conds.push('flag = ?'); args.push(flag); }
      if (!showInactive) { conds.push('active = 1'); }
      if (dueOnly) {
        conds.push("collect_cycle > 0");
        conds.push("(last_collected IS NULL OR CAST(julianday('now') - julianday(last_collected) AS INTEGER) >= collect_cycle)");
      }
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

      const existing = await db.execute({ sql: 'SELECT * FROM sku_manage WHERE sku_id = ?', args: [b.sku_id] });
      const old = existing.rows.length ? existing.rows[0] : {};

      await db.execute({
        sql: `INSERT INTO sku_manage (sku_id, brand, sku_name, base_price, current_price, price_checked_at, pid, iid, vid, product_url, flag, memo, collect_cycle, last_collected, active, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
              ON CONFLICT(sku_id) DO UPDATE SET
                brand=CASE WHEN excluded.brand!='' THEN excluded.brand ELSE sku_manage.brand END,
                sku_name=CASE WHEN excluded.sku_name!='' THEN excluded.sku_name ELSE sku_manage.sku_name END,
                base_price=CASE WHEN excluded.base_price IS NOT NULL THEN excluded.base_price ELSE sku_manage.base_price END,
                current_price=CASE WHEN excluded.current_price IS NOT NULL THEN excluded.current_price ELSE sku_manage.current_price END,
                price_checked_at=CASE WHEN excluded.price_checked_at IS NOT NULL THEN excluded.price_checked_at ELSE sku_manage.price_checked_at END,
                pid=CASE WHEN excluded.pid IS NOT NULL THEN excluded.pid ELSE sku_manage.pid END,
                iid=CASE WHEN excluded.iid IS NOT NULL THEN excluded.iid ELSE sku_manage.iid END,
                vid=CASE WHEN excluded.vid IS NOT NULL THEN excluded.vid ELSE sku_manage.vid END,
                product_url=CASE WHEN excluded.product_url IS NOT NULL THEN excluded.product_url ELSE sku_manage.product_url END,
                flag=CASE WHEN excluded.flag IS NOT NULL THEN excluded.flag ELSE sku_manage.flag END,
                memo=CASE WHEN excluded.memo IS NOT NULL THEN excluded.memo ELSE sku_manage.memo END,
                collect_cycle=CASE WHEN excluded.collect_cycle IS NOT NULL THEN excluded.collect_cycle ELSE sku_manage.collect_cycle END,
                last_collected=CASE WHEN excluded.last_collected IS NOT NULL THEN excluded.last_collected ELSE sku_manage.last_collected END,
                active=excluded.active,
                updated_at=datetime('now')`,
        args: [
          b.sku_id,
          b.brand||old.brand||'',
          b.sku_name||old.sku_name||'',
          b.base_price!=null?b.base_price:(old.base_price||null),
          b.current_price!=null?b.current_price:(old.current_price||null),
          b.price_checked_at||old.price_checked_at||null,
          b.pid||old.pid||null,
          b.iid||old.iid||null,
          b.vid||old.vid||null,
          b.product_url||old.product_url||null,
          b.flag!=null?b.flag:(old.flag||null),
          b.memo!=null?b.memo:(old.memo||null),
          b.collect_cycle!=null?b.collect_cycle:(old.collect_cycle!=null?old.collect_cycle:7),
          b.last_collected||old.last_collected||null,
          b.active!=null?b.active:1
        ]
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
