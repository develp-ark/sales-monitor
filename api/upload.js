const { google } = require('googleapis');

const SPREADSHEET_ID = '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw';

async function getSheetsAsync() {
  const raw = process.env.GOOGLE_PRIVATE_KEY || '';
  const email = process.env.GOOGLE_CLIENT_EMAIL || '';
  
  console.log('[SHEETS AUTH] email:', email.length, 'key:', raw.length, 'hasNewline:', raw.includes('\n'));
  
  // test-sheets.js와 동일한 방식
  let key = raw;
  if (raw.length > 0 && !raw.includes('\n') && raw.includes('\\n')) {
    key = raw.replace(/\\n/g, '\n');
  }
  
  console.log('[SHEETS AUTH] final key length:', key.length, 'lines:', key.split('\n').length);
  
  const auth = new google.auth.JWT({
    email: email,
    key: key,
    scopes: ['https://www.googleapis.com/auth/spreadsheets']
  });
  
  await auth.authorize();
  console.log('[SHEETS AUTH] authorize success');
  return google.sheets({ version: 'v4', auth });
}



async function syncBrandSheet(brandName, rows) {
  const sheets = await getSheetsAsync();
  
  // 시트 존재 확인/생성
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

  // 1) 기존 시트 데이터 읽기
  let existing = [];
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID, range: brandName + '!A:ZZ'
    });
    existing = res.data.values || [];
  } catch (e) {}

  // 2) 기존 데이터에서 SKU 맵, 날짜 목록 복원
  let oldHeader = [];
  let oldDates = [];
  const skuMap = new Map(); // sku_id -> { name, stock, status, salesByDate }

  if (existing.length >= 2) {
    oldHeader = existing[0];
    // 날짜는 E열(index 4)부터
    oldDates = oldHeader.slice(4);
    for (let i = 2; i < existing.length; i++) { // 0=헤더, 1=합계행, 2~=SKU행
      const row = existing[i];
      const skuId = String(row[0] || '').trim();
      if (!skuId) continue;
      const salesByDate = {};
      for (let j = 4; j < row.length; j++) {
        const d = oldDates[j - 4];
        if (d) salesByDate[d] = num(row[j], 0);
      }
      skuMap.set(skuId, {
        name: row[1] || '',
        stock: row[2] != null ? row[2] : '',
        status: row[3] || '',
        salesByDate
      });
    }
  }

  // 3) 새 데이터 반영
  const newDates = new Set();
  for (const r of rows) {
    newDates.add(r.date);
    const sid = String(r.sku_id).trim();
    if (!skuMap.has(sid)) {
      skuMap.set(sid, { name: '', stock: '', status: '', salesByDate: {} });
    }
    const entry = skuMap.get(sid);
    // SKU명, 재고, 상태는 최신 데이터로 덮어쓰기
    if (r.sku_name) entry.name = r.sku_name;
    if (r.stock != null) entry.stock = r.stock;
    if (r.status) entry.status = r.status;
    // 해당 날짜 판매량 덮어쓰기
    entry.salesByDate[r.date] = r.sales;
  }

  // 4) 전체 날짜 목록 (오래된 날짜 → 최신 날짜, 왼쪽→오른쪽)
  const allDatesSet = new Set([...oldDates, ...newDates]);
  const allDates = [...allDatesSet].sort();

  // 5) 날짜 표시: MM-DD 형식
  const dateLabelRow = allDates.map(d => {
    const parts = d.split('-');
    return parts.length === 3 ? parts[1] + '-' + parts[2] : d;
  });

  // 6) 헤더행
  const header = ['SKU ID', 'SKU 명', '재고', '상태', ...dateLabelRow];

  // 7) 일별 합계 계산
  const dailyTotals = allDates.map(d => {
    let total = 0;
    for (const [, entry] of skuMap) {
      total += (entry.salesByDate[d] || 0);
    }
    return total;
  });
  const sumRow = ['', '', '', '', ...dailyTotals];

  // 8) SKU 행 (SKU ID 오름차순)
  const skuRows = [];
  const sortedSkus = [...skuMap.entries()].sort((a, b) => a[0].localeCompare(b[0]));
  for (const [skuId, entry] of sortedSkus) {
    const salesCells = allDates.map(d => entry.salesByDate[d] || 0);
    skuRows.push([skuId, entry.name, entry.stock, entry.status, ...salesCells]);
  }

  // 9) 최종 데이터: 헤더 + 합계행 + SKU행들
  const allRows = [header, sumRow, ...skuRows];

  // 10) 시트에 쓰기 + 서식 적용
  try {
    // 시트 ID 가져오기
    const metaForId = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const sheetObj = metaForId.data.sheets.find(s => s.properties.title === brandName);
    const sheetId = sheetObj.properties.sheetId;

    // 값 클리어 후 쓰기
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID, range: brandName + '!A:ZZ'
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: brandName + '!A1',
      valueInputOption: 'RAW',
      requestBody: { values: allRows }
    });

    const totalCols = header.length;
    const totalRows = allRows.length;

    // 서식 요청 배열
    const requests = [];

    // --- 1행(헤더): 진한 파랑 배경 + 흰색 굵은 글자 + 고정 ---
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: totalCols },
        cell: {
          userEnteredFormat: {
            backgroundColor: { red: 0.1, green: 0.3, blue: 0.6 },
            textFormat: { bold: true, foregroundColor: { red: 1, green: 1, blue: 1 }, fontSize: 10 },
            horizontalAlignment: 'CENTER',
            verticalAlignment: 'MIDDLE'
          }
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment)'
      }
    });

    // --- 2행(합계): 연한 노랑 배경 + 굵은 글자 ---
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex: 1, endRowIndex: 2, startColumnIndex: 0, endColumnIndex: totalCols },
        cell: {
          userEnteredFormat: {
            backgroundColor: { red: 1, green: 0.95, blue: 0.8 },
            textFormat: { bold: true, fontSize: 10 },
            horizontalAlignment: 'CENTER'
          }
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
      }
    });

    // --- A~D열(SKU ID, SKU명, 재고, 상태) 헤더 색: 연한 녹색 ---
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 4 },
        cell: {
          userEnteredFormat: {
            backgroundColor: { red: 0.85, green: 0.93, blue: 0.83 },
            textFormat: { bold: true, foregroundColor: { red: 0, green: 0, blue: 0 }, fontSize: 10 },
            horizontalAlignment: 'CENTER'
          }
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
      }
    });

    // --- 날짜 열 헤더(E~): 연한 파랑 ---
    if (totalCols > 4) {
      requests.push({
        repeatCell: {
          range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 4, endColumnIndex: totalCols },
          cell: {
            userEnteredFormat: {
              backgroundColor: { red: 0.82, green: 0.88, blue: 0.97 },
              textFormat: { bold: true, foregroundColor: { red: 0.1, green: 0.1, blue: 0.5 }, fontSize: 10 },
              horizontalAlignment: 'CENTER'
            }
          },
          fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
        }
      });
    }

    // --- 3행~ 데이터 영역: 기본 서식 ---
    if (totalRows > 2) {
      requests.push({
        repeatCell: {
          range: { sheetId, startRowIndex: 2, endRowIndex: totalRows, startColumnIndex: 0, endColumnIndex: totalCols },
          cell: {
            userEnteredFormat: {
              backgroundColor: { red: 1, green: 1, blue: 1 },
              textFormat: { fontSize: 9 }
            }
          },
          fields: 'userEnteredFormat(backgroundColor,textFormat)'
        }
      });
    }

    // --- 품절 셀 빨간색 (D열 = index 3) ---
    for (let i = 2; i < totalRows; i++) {
      const statusVal = allRows[i] && allRows[i][3] ? String(allRows[i][3]) : '';
      if (statusVal === '품절') {
        requests.push({
          repeatCell: {
            range: { sheetId, startRowIndex: i, endRowIndex: i + 1, startColumnIndex: 3, endColumnIndex: 4 },
            cell: {
              userEnteredFormat: {
                backgroundColor: { red: 1, green: 0.8, blue: 0.8 },
                textFormat: { bold: true, foregroundColor: { red: 0.8, green: 0, blue: 0 }, fontSize: 9 }
              }
            },
            fields: 'userEnteredFormat(backgroundColor,textFormat)'
          }
        });
      }
    }

    // --- 1행 고정 (freeze) ---
    requests.push({
      updateSheetProperties: {
        properties: { sheetId, gridProperties: { frozenRowCount: 2, frozenColumnCount: 2 } },
        fields: 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'
      }
    });

    // --- 필터 설정 (1행 기준) ---
    // 기존 필터 제거
    requests.push({
      clearBasicFilter: { sheetId }
    });
    // 새 필터 적용
    requests.push({
      setBasicFilter: {
        filter: {
          range: { sheetId, startRowIndex: 1, endRowIndex: totalRows, startColumnIndex: 0, endColumnIndex: totalCols }
        }
      }
    });

    // --- 열 너비 조정 ---
    requests.push({
      updateDimensionProperties: {
        range: { sheetId, dimension: 'COLUMNS', startIndex: 0, endIndex: 1 },
        properties: { pixelSize: 100 }, fields: 'pixelSize'
      }
    });
    requests.push({
      updateDimensionProperties: {
        range: { sheetId, dimension: 'COLUMNS', startIndex: 1, endIndex: 2 },
        properties: { pixelSize: 280 }, fields: 'pixelSize'
      }
    });
    requests.push({
      updateDimensionProperties: {
        range: { sheetId, dimension: 'COLUMNS', startIndex: 2, endIndex: 4 },
        properties: { pixelSize: 60 }, fields: 'pixelSize'
      }
    });
    if (totalCols > 4) {
      requests.push({
        updateDimensionProperties: {
          range: { sheetId, dimension: 'COLUMNS', startIndex: 4, endIndex: totalCols },
          properties: { pixelSize: 50 }, fields: 'pixelSize'
        }
      });
    }

    // 서식 일괄 적용
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests }
    });

    console.log('[Sheets] ' + brandName + ': ' + skuRows.length + ' SKUs, ' + allDates.length + ' dates synced + formatted');
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

  const brand = fileBrandFromName || row.brand || row['브랜드'] || '';
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
    const skipSync = req.query && req.query.skipSync === '1';
    let sheetsMsg = '';
    if (!skipSync) {
      try {
        const allBrands = await db.execute(
          "SELECT DISTINCT brand FROM sales WHERE brand != ''"
        );
        for (const bRow of allBrands.rows) {
          const brandName = bRow.brand;
          const brandData = await db.execute({
            sql: 'SELECT date, sku_id, sku_name, sales, stock, status FROM sales WHERE brand = ? ORDER BY date DESC, sku_id',
            args: [brandName]
          });
          const rows = brandData.rows.map(r => ({
            date: r.date, sku_id: r.sku_id, sku_name: r.sku_name,
            sales: Number(r.sales)||0, stock: r.stock, status: r.status
          }));
          await syncBrandSheet(brandName, rows);
        }

        const trendRows = await db.execute(
          'SELECT brand, date, SUM(sales) AS s FROM sales GROUP BY brand, date ORDER BY date'
        );
        const trendByBrand = {};
        for (const r of trendRows.rows) {
          if (!trendByBrand[r.brand]) trendByBrand[r.brand] = [];
          trendByBrand[r.brand].push({ date: r.date, totalSales: Number(r.s) || 0 });
        }
        await syncDailyTrend(trendByBrand);

        console.log('[Sheets] full DB sync complete');
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
