const Busboy = require('busboy');
const { parse } = require('csv-parse');
const { getDb } = require('../lib/db');

const BATCH_SIZE = 800;

function detectBrandFromFilename(name) {
  if (!name) return null;
  const lower = name.toLowerCase();
  if (name.includes('건우') || lower.includes('gunu')) return '건우코리아';
  if (name.includes('아리코') || lower.includes('arico')) return '아리코';
  if (name.includes('윰') || lower.includes('yum')) return '윰';
  return null;
}

function normalizeHeader(cell) {
  const s = String(cell ?? '')
    .trim()
    .replace(/^\ufeff/, '');
  const u = s.toLowerCase();

  if (s === '날짜') return 'date';
  if (s === '브랜드') return 'brandCsv';
  if (u === 'sku id' || u === 'skuid' || u === 'sku_id') return 'sku_id';
  // SKU 명 - 공백 유무 모두 처리
  if (/^sku\s*명$/i.test(s)) return 'sku_name';
  if (s === '판매량' || s === '출고수량') return 'sales';
  if (s === '재고' || s === '현재재고수량') return 'stock';
  if (s === '상태' || s === '발주가능상태') return 'status';
  if (s === '품절여부') return 'outOfStock';

  return s;
}

function pickColumns(header) {
  return header.map(normalizeHeader);
}

function toISODate(v) {
  if (v == null || v === '') return null;
  const s = String(v).trim().replace(/\./g, '-').replace(/\//g, '-');
  const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) {
    const y = m[1];
    const mo = m[2].padStart(2, '0');
    const d = m[3].padStart(2, '0');
    return `${y}-${mo}-${d}`;
  }
  const m2 = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})/);
  if (m2) {
    const mo = m2[1].padStart(2, '0');
    const d = m2[2].padStart(2, '0');
    const y = m2[3];
    return `${y}-${mo}-${d}`;
  }
  return s.length >= 10 ? s.slice(0, 10) : s;
}

function num(v, def = 0) {
  if (v == null || v === '') return def;
  const n = parseInt(String(v).replace(/,/g, '').trim(), 10);
  return Number.isFinite(n) ? n : def;
}

const UPSERT_SQL = `INSERT INTO sales (date, brand, sku_id, sku_name, sales, stock, status, revenue)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(date, sku_id) DO UPDATE SET
  brand = excluded.brand,
  sku_name = excluded.sku_name,
  sales = sales.sales + excluded.sales,
  stock = CASE WHEN excluded.stock IS NOT NULL THEN excluded.stock ELSE sales.stock END,
  status = excluded.status,
  revenue = excluded.revenue`;

async function flushBatch(db, batch) {
  if (!batch.length) return;
  const stmts = batch.map((row) => ({
    sql: UPSERT_SQL,
    args: [row.date, row.brand, row.sku_id, row.sku_name, row.sales, row.stock, row.status, row.revenue],
  }));
  await db.batch(stmts);
}

function rowFromRecord(rec, fileBrand) {
  const date = toISODate(rec.date);
  const sku_id = rec.sku_id != null ? String(rec.sku_id).trim() : '';
  if (!date || !sku_id) return null;

  const brand =
    fileBrand ||
    (rec.brandCsv != null && String(rec.brandCsv).trim()) ||
    '기타';
  const sku_name = rec.sku_name != null ? String(rec.sku_name).trim() : '';
  const sales = num(rec.sales, 0);
  const stockVal = rec.stock;
  const stock = stockVal === '' || stockVal == null ? null : num(stockVal, 0);

  let status = rec.status != null ? String(rec.status).trim() : '';
  if (rec.outOfStock && String(rec.outOfStock).trim().toUpperCase() === 'Y') {
    status = '품절';
  }

  return { date, brand, sku_id, sku_name, sales, stock, status, revenue: 0 };
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  let totalRows = 0;
  let fileBrandFromName = null;

  try {
    const db = getDb();
    const bb = Busboy({ headers: req.headers, defParamCharset: 'utf8' });

    const done = new Promise((resolve, reject) => {
      let chain = Promise.resolve();

      bb.on('file', (fieldname, fileStream, info) => {
        const filename = (info && info.filename) || '';
        fileBrandFromName = detectBrandFromFilename(filename);

        const parser = parse({
          columns: pickColumns,
          relax_column_count: true,
          skip_empty_lines: true,
          trim: true,
          bom: true,
        });

        chain = chain.then(async () => {
          const batch = [];
          fileStream.on('error', reject);
          fileStream.pipe(parser);

          for await (const rec of parser) {
            const row = rowFromRecord(rec, fileBrandFromName);
            if (!row) continue;
            batch.push(row);
            totalRows++;
            if (batch.length >= BATCH_SIZE) {
              await flushBatch(db, batch.splice(0, BATCH_SIZE));
            }
          }
          if (batch.length) {
            await flushBatch(db, batch);
          }
        });
      });

      bb.on('error', reject);
      bb.on('finish', () => chain.then(resolve).catch(reject));
    });

    req.pipe(bb);
    await done;

    return res.status(200).json({
      ok: true,
      rows: totalRows,
      fileBrand: fileBrandFromName,
      message: `${totalRows.toLocaleString()}행 반영 완료`,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'upload failed' });
  }
};
