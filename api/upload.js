const { google } = require('googleapis');

const SPREADSHEET_ID = '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw';

let _sheets = null;

async function getSheetsAsync() {
  if (_sheets) return _sheets;
  let key = process.env.GOOGLE_PRIVATE_KEY || '';
  if (!key.includes('\n') && key.includes('\\n')) {
    key = key.replace(/\\n/g, '\n');
  }
  const auth = new google.auth.JWT(
    process.env.GOOGLE_CLIENT_EMAIL, null, key,
    ['https://www.googleapis.com/auth/spreadsheets']
  );
  await auth.authorize();
  _sheets = google.sheets({ version: 'v4', auth });
  return _sheets;
}

async function syncBrandSheet(brandName, rows) {
  const sheets = await getSheetsAsync();
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = meta.data.sheets.some(s => s.properties.title === brandName);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: brandName } } }] }
      });
    }
  } catch (e) { console.error('Sheet check error:', e.message); return; }

  const header = ['date', 'SKU ID', 'SKU name', 'sales', 'stock', 'status'];
  const data = rows.map(r => [r.date, r.sku_id, r.sku_name, r.sales, r.stock != null ? r.stock : '', r.status]);
  try {
    let existing = [];
    try {
      const res = await sheets.spreadsheets.values.get({ spreadsheetId: SPREADSHEET_ID, range: brandName + '!A:F' });
      existing = res.data.values || [];
    } catch (e) {}
    const newDates = new Set(rows.map(r => r.date));
    const kept = existing.length > 1 ? existing.slice(1).filter(row => !newDates.has(row[0])) : [];
    const sorted = [header, ...[...kept, ...data].sort((a, b) => (b[0] > a[0] ? 1 : b[0] < a[0] ? -1 : 0))];
    await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: brandName + '!A:F' });
    await sheets.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: brandName + '!A1', valueInputOption: 'RAW', requestBody: { values: sorted } });
    console.log('[Sheets] ' + brandName + ': ' + data.length + ' rows synced');
  } catch (e) { console.error('[Sheets] sync error:', e.message); }
}

async function syncDailyTrend(brandRows) {
  const sheets = await getSheetsAsync()
  const title = 'daily_trend';
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = meta.data.sheets.some(s => s.properties.title === title);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: title } } }] }
      });
    }
  } catch (e) { console.error('Sheet check error:', e.message); return; }
  const allDates = new Set();
  const brands = Object.keys(brandRows);
  for (const b of brands) for (const r of brandRows[b]) allDates.add(r.date);
  const dates = [...allDates].sort().reverse();
  const maps = {};
  for (const b of brands) { maps[b] = {}; for (const r of brandRows[b]) maps[b][r.date] = (maps[b][r.date] || 0) + r.totalSales; }
  const header = ['date', ...brands, 'total'];
  const dataRows = dates.map(d => { const vals = brands.map(b => maps[b][d] || 0); return [d, ...vals, vals.reduce((a, c) => a + c, 0)]; });
  try {
    await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: title + '!A:Z' });
    await sheets.spreadsheets.values.update({ spreadsheetId: SPREADSHEET_ID, range: title + '!A1', valueInputOption: 'RAW', requestBody: { values: [header, ...dataRows] } });
    console.log('[Sheets] daily_trend: ' + dataRows.length + ' rows');
  } catch (e) { console.error('[Sheets] trend error:', e.message); }
}

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

  // 8자리 숫자: 20260325 → 2026-03-25
  const m8 = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (m8) return `${m8[1]}-${m8[2]}-${m8[3]}`;

  const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) return `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`;

  const m2 = s.match(/^(\d{1,2})-(\d{1,2})-(\d{4})/);
  if (m2) return `${m2[3]}-${m2[1].padStart(2, '0')}-${m2[2].padStart(2, '0')}`;

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
  sales = excluded.sales,
  stock = excluded.stock,
  status = excluded.status,
  revenue = excluded.revenue`;

function mergeRows(rows) {
  const map = new Map();
  for (const r of rows) {
    const key = `${r.date}|${r.sku_id}`;
    if (map.has(key)) {
      const existing = map.get(key);
      existing.sales += r.sales;
      if (r.stock != null && (existing.stock == null || r.stock > existing.stock)) {
        existing.stock = r.stock;
      }
      if (r.sku_name) existing.sku_name = r.sku_name;
      if (r.status) existing.status = r.status;
      if (r.brand) existing.brand = r.brand;
    } else {
      map.set(key, { ...r });
    }
  }
  return [...map.values()];
}

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
  const allParsedRows = [];

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
          const rawRows = [];
          fileStream.on('error', reject);
          fileStream.pipe(parser);

          for await (const rec of parser) {
            const row = rowFromRecord(rec, fileBrandFromName);
            if (!row) continue;
            rawRows.push(row);
          }

          const merged = mergeRows(rawRows);
          totalRows += merged.length;
          allParsedRows.push(...merged);

          for (let i = 0; i < merged.length; i += BATCH_SIZE) {
            await flushBatch(db, merged.slice(i, i + BATCH_SIZE));
          }
        });
      });

      bb.on('error', reject);
      bb.on('finish', () => chain.then(resolve).catch(reject));
    });

    req.pipe(bb);
    await done;

    // ── Google Sheets 동기화 (응답 전에 실행) ──
    let sheetsMsg = '';
    if (totalRows > 0 && allParsedRows.length > 0) {
      try {
        const byBrand = {};
        for (const r of allParsedRows) {
          if (!byBrand[r.brand]) byBrand[r.brand] = [];
          byBrand[r.brand].push(r);
        }
        for (const [brand, rows] of Object.entries(byBrand)) {
          await syncBrandSheet(brand, rows);
        }

        const trendByBrand = {};
        for (const r of allParsedRows) {
          if (!trendByBrand[r.brand]) trendByBrand[r.brand] = [];
          trendByBrand[r.brand].push({ date: r.date, totalSales: r.sales });
        }
        await syncDailyTrend(trendByBrand);
        sheetsMsg = ' + 시트 동기화 완료';
      } catch (e) {
        console.error('[Sheets sync error]', e.message);
        sheetsMsg = ' (시트 동기화 실패: ' + e.message + ')';
      }
    }

    return res.status(200).json({
      ok: true,
      rows: totalRows,
      fileBrand: fileBrandFromName,
      message: `${totalRows.toLocaleString()}행 반영 완료${sheetsMsg}`,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'upload failed' });
  }
};
