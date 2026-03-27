const busboy = require('busboy');
const { parse } = require('csv-parse/sync');
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
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });

  try {
    const chunks = [];
    await new Promise((resolve, reject) => {
      const bb = busboy({ headers: req.headers });
      bb.on('file', (name, file) => {
        file.on('data', d => chunks.push(d));
      });
      bb.on('finish', resolve);
      bb.on('error', reject);
      req.pipe(bb);
    });

    const csv = Buffer.concat(chunks).toString('utf-8');
    const records = parse(csv, { columns: true, skip_empty_lines: true, bom: true, relax_quotes: true, relax_column_count: true });

    const db = getDb();
    await db.execute(INIT_SQL);

    const alters = [
      'ALTER TABLE sku_manage ADD COLUMN pid TEXT',
      'ALTER TABLE sku_manage ADD COLUMN iid TEXT',
      'ALTER TABLE sku_manage ADD COLUMN vid TEXT',
      'ALTER TABLE sku_manage ADD COLUMN product_url TEXT',
      'ALTER TABLE sku_manage ADD COLUMN active INTEGER DEFAULT 1',
      'ALTER TABLE sku_manage ADD COLUMN created_at TEXT',
      'ALTER TABLE sku_manage ADD COLUMN updated_at TEXT',
      'ALTER TABLE sku_manage ADD COLUMN memo TEXT',
    ];
    for (const sql of alters) {
      try { await db.execute(sql); } catch(e) { /* already exists */ }
    }

    function col(row, keys) {
      for (var k of keys) {
        if (row[k] != null && String(row[k]).trim()) return String(row[k]).trim();
      }
      return '';
    }

    let count = 0;
    for (const row of records) {
      const skuId = col(row, ['sku_id', 'SKU ID', 'SKU_ID', 'skuId']);
      if (!skuId) continue;

      const brand = col(row, ['brand', '브랜드', 'Brand']);
      const skuName = col(row, ['sku_name', 'SKU 명', 'SKU명', '상품명', 'name']);
      const pid = col(row, ['pid', 'PID', '상품ID', 'productId']);
      const iid = col(row, ['iid', 'IID', 'itemId', '아이템ID']);
      const vid = col(row, ['vid', 'VID', 'vendorItemId', '벤더아이템ID']);
      const productUrl = col(row, ['product_url', 'url', 'URL', '상품URL', 'link']);
      const flag = col(row, ['flag', '플래그', 'Flag']);
      const memo = col(row, ['memo', '메모', 'Memo']);

      let finalPid = pid, finalIid = iid, finalVid = vid;
      if (productUrl && (!pid || !iid || !vid)) {
        const pidMatch = productUrl.match(/products\/(\d+)/);
        const iidMatch = productUrl.match(/itemId=(\d+)/);
        const vidMatch = productUrl.match(/vendorItemId=(\d+)/);
        if (pidMatch && !pid) finalPid = pidMatch[1];
        if (iidMatch && !iid) finalIid = iidMatch[1];
        if (vidMatch && !vid) finalVid = vidMatch[1];
      }

      await db.execute({
        sql: `INSERT INTO sku_manage (sku_id, brand, sku_name, pid, iid, vid, product_url, flag, memo, active, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'))
              ON CONFLICT(sku_id) DO UPDATE SET
                brand=CASE WHEN excluded.brand!='' THEN excluded.brand ELSE sku_manage.brand END,
                sku_name=CASE WHEN excluded.sku_name!='' THEN excluded.sku_name ELSE sku_manage.sku_name END,
                pid=CASE WHEN excluded.pid!='' THEN excluded.pid ELSE sku_manage.pid END,
                iid=CASE WHEN excluded.iid!='' THEN excluded.iid ELSE sku_manage.iid END,
                vid=CASE WHEN excluded.vid!='' THEN excluded.vid ELSE sku_manage.vid END,
                product_url=CASE WHEN excluded.product_url!='' THEN excluded.product_url ELSE sku_manage.product_url END,
                flag=CASE WHEN excluded.flag!='' THEN excluded.flag ELSE sku_manage.flag END,
                memo=CASE WHEN excluded.memo!='' THEN excluded.memo ELSE sku_manage.memo END,
                updated_at=datetime('now')`,
        args: [skuId, brand, skuName, finalPid||null, finalIid||null, finalVid||null, productUrl||null, flag||null, memo||null]
      });
      count++;
    }

    res.json({ ok: true, imported: count });
  } catch (e) {
    console.error('[upload-sku-manage error]', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
};
