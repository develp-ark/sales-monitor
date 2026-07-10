const busboy = require('busboy');
const { parse } = require('csv-parse/sync');
const { getDb } = require('../db');
const { ensureSkuManage } = require('../schema');

const UPSERT_SQL = `INSERT INTO sku_manage (sku_id, brand, sku_name, base_price, pid, iid, vid, product_url, flag, memo, active, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'))
      ON CONFLICT(sku_id) DO UPDATE SET
        brand=CASE WHEN excluded.brand!='' THEN excluded.brand ELSE sku_manage.brand END,
        sku_name=CASE WHEN excluded.sku_name!='' THEN excluded.sku_name ELSE sku_manage.sku_name END,
        base_price=CASE WHEN excluded.base_price IS NOT NULL THEN excluded.base_price ELSE sku_manage.base_price END,
        pid=CASE WHEN excluded.pid!='' THEN excluded.pid ELSE sku_manage.pid END,
        iid=CASE WHEN excluded.iid!='' THEN excluded.iid ELSE sku_manage.iid END,
        vid=CASE WHEN excluded.vid!='' THEN excluded.vid ELSE sku_manage.vid END,
        product_url=CASE WHEN excluded.product_url!='' THEN excluded.product_url ELSE sku_manage.product_url END,
        flag=CASE WHEN excluded.flag!='' THEN excluded.flag ELSE sku_manage.flag END,
        memo=CASE WHEN excluded.memo!='' THEN excluded.memo ELSE sku_manage.memo END,
        updated_at=datetime('now')`;

/**
 * CSV 헤더를 대소문자·앞뒤 공백·연속 공백·BOM·비단절 공백에 관계없이 매칭합니다.
 * detectCsvType()은 헤더를 소문자로 낮춰 비교하므로, 여기서만 엄격하면
 * 파일이 sku-manage로 잘 분류된 뒤 모든 행이 조용히 건너뛰어집니다.
 */
const BOM = String.fromCharCode(0xFEFF);
const NBSP = String.fromCharCode(0x00A0); // 엑셀에서 복사하면 일반 공백 대신 섞여 들어옵니다.

function normKey(k) {
  return String(k == null ? '' : k)
    .split(BOM).join('')
    .split(NBSP).join(' ')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, ' ');
}

function normalizeRow(row) {
  const out = {};
  for (const k of Object.keys(row)) {
    const nk = normKey(k);
    if (out[nk] == null || String(out[nk]).trim() === '') out[nk] = row[k];
  }
  return out;
}

function col(row, keys) {
  for (const k of keys) {
    const v = row[normKey(k)];
    if (v != null && String(v).trim()) return String(v).trim();
  }
  return '';
}

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
    const headers = records.length ? Object.keys(records[0]).map(normKey) : [];

    const db = getDb();
    await ensureSkuManage(db);

    const stmts = [];
    let skippedNoSkuId = 0;

    for (const raw of records) {
      const row = normalizeRow(raw);
      const skuId = col(row, ['sku_id', 'SKU ID', 'SKU_ID', 'skuId']);
      if (!skuId) { skippedNoSkuId++; continue; }

      const brand = col(row, ['brand', '브랜드']);
      const skuName = col(row, ['sku_name', 'SKU 명', 'SKU명', '상품명', 'name']);
      const pid = col(row, ['pid', '상품ID', 'productId']);
      const iid = col(row, ['iid', 'itemId', '아이템ID']);
      const vid = col(row, ['vid', 'vendorItemId', '벤더아이템ID']);
      const productUrl = col(row, ['product_url', 'url', '상품URL', 'link']);
      const flag = col(row, ['flag', '플래그']);
      const memo = col(row, ['memo', '메모', '비고']);
      const basePrice = col(row, ['base_price', '등록가', '가격', 'price']);

      let finalPid = pid, finalIid = iid, finalVid = vid;
      if (productUrl && (!pid || !iid || !vid)) {
        const pidMatch = productUrl.match(/products\/(\d+)/);
        const iidMatch = productUrl.match(/itemId=(\d+)/);
        const vidMatch = productUrl.match(/vendorItemId=(\d+)/);
        if (pidMatch && !pid) finalPid = pidMatch[1];
        if (iidMatch && !iid) finalIid = iidMatch[1];
        if (vidMatch && !vid) finalVid = vidMatch[1];
      }

      stmts.push({
        sql: UPSERT_SQL,
        args: [skuId, brand, skuName, basePrice ? parseInt(basePrice) : null, finalPid || null, finalIid || null, finalVid || null, productUrl || null, flag || null, memo || null],
      });
    }

    // 행마다 왕복하면 수백 건에서 Vercel 60초 제한에 걸립니다.
    for (let i = 0; i < stmts.length; i += 200) {
      await db.batch(stmts.slice(i, i + 200));
    }

    const body = { ok: true, imported: stmts.length, skipped: skippedNoSkuId };
    if (stmts.length === 0 && records.length > 0) {
      const hasSkuIdCol = ['sku_id', 'SKU ID', 'SKU_ID', 'skuId'].some(k => headers.includes(normKey(k)));
      body.warning = hasSkuIdCol
        ? 'SKU ID 값이 모두 비어 있어 한 건도 저장하지 않았습니다.'
        : 'SKU ID 컬럼을 찾지 못해 한 건도 저장하지 않았습니다.';
      body.headers = headers;
    }
    res.json(body);
  } catch (e) {
    console.error('[upload-sku-manage error]', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
};
