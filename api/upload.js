const { google } = require('googleapis');

const SPREADSHEET_ID = '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw';

// ── 브랜드별 컬러 ──
const BRAND_COLORS = {
  '건우코리아': {
    header: { red: 0.94, green: 0.27, blue: 0.32 },
    headerText: { red: 1, green: 1, blue: 1 },
    light: { red: 1, green: 0.93, blue: 0.93 },
  },
  '아리코': {
    header: { red: 0.19, green: 0.51, blue: 0.96 },
    headerText: { red: 1, green: 1, blue: 1 },
    light: { red: 0.92, green: 0.95, blue: 1 },
  },
  '윰': {
    header: { red: 0, green: 0.71, blue: 0.58 },
    headerText: { red: 1, green: 1, blue: 1 },
    light: { red: 0.91, green: 0.98, blue: 0.96 },
  },
};
const DEFAULT_BRAND_COLOR = {
  header: { red: 0.24, green: 0.52, blue: 0.78 },
  headerText: { red: 1, green: 1, blue: 1 },
  light: { red: 0.9, green: 0.93, blue: 1 },
};

function getBrandColor(name) {
  return BRAND_COLORS[name] || DEFAULT_BRAND_COLOR;
}

/** 0-based column index → A1 letter (0→A, 25→Z, 26→AA) */
function colToA1(zeroBasedCol) {
  let n = zeroBasedCol + 1;
  let s = '';
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

function addDaysIso(isoDate, deltaDays) {
  const x = new Date(`${isoDate}T12:00:00.000Z`);
  x.setUTCDate(x.getUTCDate() + deltaDays);
  return x.toISOString().slice(0, 10);
}

async function getSheetsAsync() {
  const raw = process.env.GOOGLE_PRIVATE_KEY || '';
  const email = process.env.GOOGLE_CLIENT_EMAIL || '';
  console.log('[SHEETS AUTH] email:', email.length, 'key:', raw.length, 'hasNewline:', raw.includes('\n'));
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
  if (!sheets) return;

  const bc = getBrandColor(brandName);

  // 1) 시트 존재 확인 / 생성
  let sheetId;
  const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
  const existing = meta.data.sheets.find(s => s.properties.title === brandName);
  if (existing) {
    sheetId = existing.properties.sheetId;
  } else {
    const addRes = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{ addSheet: { properties: { title: brandName } } }]
      }
    });
    sheetId = addRes.data.replies[0].addSheet.properties.sheetId;
  }

  // 2) 기존 데이터 읽기
  let existingRows = [];
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${brandName}!A1:ZZ`
    });
    existingRows = res.data.values || [];
  } catch (e) {
    console.log('[SHEETS] No existing data, starting fresh');
    // 시트가 존재하는데 읽기 실패한 경우 → 아카이브 보호를 위해 중단
    if (existing) {
      console.error('[SHEETS] ⚠️ 기존 시트 읽기 실패 - 아카이브 보호를 위해 동기화 중단:', e.message);
      return;
    }
  }

  // 3) 기존 데이터를 SKU Map으로 변환
  const skuMap = {};
  let existingDates = [];
  const fixedCols = 4;

  if (existingRows.length >= 2) {
    const header = existingRows[0];
    existingDates = header.slice(fixedCols).map(h => String(h).trim());
    const h2 = String(header[2] || '').trim();
    const h3 = String(header[3] || '').trim();
    const stockFirst = h2.includes('재고') || h3.includes('상태');

    const row1First = String(existingRows[1]?.[0] ?? '').trim();
    const row1IsSum = row1First === '합계' || row1First.startsWith('=');
    const dataStartIndex = row1IsSum ? 2 : 1;

    for (let r = dataStartIndex; r < existingRows.length; r++) {
      const row = existingRows[r];
      const skuId = String(row[0] || '').trim();
      if (!skuId || skuId === '합계') continue;
      const skuName = String(row[1] || '').trim();
      const stockCell = stockFirst ? row[2] : row[3];
      const statusCell = stockFirst ? row[3] : row[2];

      if (!skuMap[skuId]) {
        skuMap[skuId] = {
          name: skuName,
          stock: stockCell !== undefined && stockCell !== null ? stockCell : '',
          status: statusCell !== undefined && statusCell !== null ? statusCell : '',
          latestDate: '',
          dates: {}
        };
      }
      for (let d = 0; d < existingDates.length; d++) {
        const dateKey = existingDates[d];
        const val = row[fixedCols + d];
        if (val !== undefined && val !== null && val !== '') {
          skuMap[skuId].dates[dateKey] = Number(val) || 0;
        }
      }
      const dk = Object.keys(skuMap[skuId].dates);
      if (dk.length) {
        skuMap[skuId].latestDate = dk.reduce((a, b) => (a > b ? a : b));
      }
    }
  }

  // 4) 새 데이터 병합
  for (const row of rows) {
    const skuId = String(row.sku_id || '').trim();
    if (!skuId) continue;
    const dateRaw = row.date;
    const dateKey = dateRaw ? dateRaw.slice(5) : null;
    if (!dateKey) continue;

    if (!skuMap[skuId]) {
      skuMap[skuId] = { name: row.sku_name || '', stock: '', status: '', latestDate: '', dates: {} };
    }
    skuMap[skuId].dates[dateKey] = Number(row.sales) || 0;

    if (!skuMap[skuId].latestDate || dateKey > skuMap[skuId].latestDate) {
      skuMap[skuId].latestDate = dateKey;
      if (row.stock !== undefined && row.stock !== null && row.stock !== '') {
        skuMap[skuId].stock = row.stock;
      }
      if (row.status !== undefined && row.status !== null && row.status !== '') {
        skuMap[skuId].status = row.status;
      }
    }
  }

  // 5) 전체 날짜 목록 생성 및 정렬
  const allDatesSet = new Set();
  for (const sku of Object.values(skuMap)) {
    for (const d of Object.keys(sku.dates)) {
      allDatesSet.add(d);
    }
  }
  const allDates = [...allDatesSet].sort((a, b) => {
    const [am, ad] = a.split('-').map(Number);
    const [bm, bd] = b.split('-').map(Number);
    if (am !== bm) return am - bm;
    return ad - bd;
  });

  // 6) 헤더 행
  const headerRow = ['SKU ID', 'SKU명', '재고', '상태', ...allDates];

  // 7) 합계 행 — 3행부터 데이터 기준 =SUM(col3:col) (시트에서 항상 정확히 합산)
  const sumRow = ['합계', '', '', ''];
  for (let ci = 0; ci < allDates.length; ci++) {
    const col = colToA1(fixedCols + ci);
    sumRow.push(`=SUM(${col}3:${col})`);
  }

  // 8) SKU 행 (한국어순 정렬)
  const skuIds = Object.keys(skuMap).sort((a, b) => {
    const na = skuMap[a].name || '';
    const nb = skuMap[b].name || '';
    return na.localeCompare(nb, 'ko');
  });

  const dataRows = skuIds.map(skuId => {
    const sku = skuMap[skuId];
    const row = [skuId, sku.name, sku.stock || '', sku.status || ''];
    for (const d of allDates) {
      row.push(sku.dates[d] !== undefined ? sku.dates[d] : 0);
    }
    return row;
  });

  // 9) 전체 데이터 쓰기
  const allRows = [headerRow, sumRow, ...dataRows];

  await sheets.spreadsheets.values.clear({
    spreadsheetId: SPREADSHEET_ID,
    range: `${brandName}!A1:ZZ`
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${brandName}!A1`,
    valueInputOption: 'USER_ENTERED',
    requestBody: { values: allRows }
  });

  // 10) 서식 적용
  const totalCols = fixedCols + allDates.length;
  const totalRows = allRows.length;
  const requests = [];

  // ① 기존 필터 제거 (있으면)
  if (existing) {
    requests.push({ clearBasicFilter: { sheetId } });
  }

  // ② 전체 흰색 초기화
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 0, endRowIndex: totalRows, startColumnIndex: 0, endColumnIndex: totalCols },
      cell: {
        userEnteredFormat: {
          backgroundColor: { red: 1, green: 1, blue: 1 },
          textFormat: { bold: false, foregroundColor: { red: 0, green: 0, blue: 0 } }
        }
      },
      fields: 'userEnteredFormat(backgroundColor,textFormat)'
    }
  });

  // ③ 고정 행/열
  requests.push({
    updateSheetProperties: {
      properties: { sheetId, gridProperties: { frozenRowCount: 2, frozenColumnCount: 4 } },
      fields: 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'
    }
  });

  // ④ 헤더 행 (브랜드 컬러)
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: totalCols },
      cell: {
        userEnteredFormat: {
          backgroundColor: bc.header,
          textFormat: { bold: true, foregroundColor: bc.headerText },
          horizontalAlignment: 'CENTER'
        }
      },
      fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
    }
  });

  // ⑤ 합계 행
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 1, endRowIndex: 2, startColumnIndex: 0, endColumnIndex: totalCols },
      cell: {
        userEnteredFormat: {
          backgroundColor: { red: 1, green: 0.95, blue: 0.8 },
          textFormat: { bold: true },
          horizontalAlignment: 'CENTER'
        }
      },
      fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
    }
  });

  // ⑥ 판매 데이터 영역 가운데 정렬
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 2, endRowIndex: totalRows, startColumnIndex: fixedCols, endColumnIndex: totalCols },
      cell: { userEnteredFormat: { horizontalAlignment: 'CENTER' } },
      fields: 'userEnteredFormat.horizontalAlignment'
    }
  });

  // ⑦ 판매량 > 0인 셀 → 연한 브랜드컬러 (조건부 서식, 요청 1개)
  requests.push({
    addConditionalFormatRule: {
      rule: {
        ranges: [{ sheetId, startRowIndex: 2, endRowIndex: totalRows, startColumnIndex: fixedCols, endColumnIndex: totalCols }],
        booleanRule: {
          condition: { type: 'NUMBER_GREATER', values: [{ userEnteredValue: '0' }] },
          format: { backgroundColor: bc.light }
        }
      },
      index: 0
    }
  });

  // ⑧ 필터 — 1행은 헤더 제외, 2행(합계)부터 필터 영역 (필터 UI는 2행)
  requests.push({
    setBasicFilter: {
      filter: {
        range: { sheetId, startRowIndex: 1, endRowIndex: totalRows, startColumnIndex: 0, endColumnIndex: totalCols }
      }
    }
  });

  // ⑨ 열 너비
  requests.push(
    { updateDimensionProperties: { range: { sheetId, dimension: 'COLUMNS', startIndex: 0, endIndex: 1 }, properties: { pixelSize: 120 }, fields: 'pixelSize' } },
    { updateDimensionProperties: { range: { sheetId, dimension: 'COLUMNS', startIndex: 1, endIndex: 2 }, properties: { pixelSize: 250 }, fields: 'pixelSize' } },
    { updateDimensionProperties: { range: { sheetId, dimension: 'COLUMNS', startIndex: 2, endIndex: 4 }, properties: { pixelSize: 80 }, fields: 'pixelSize' } },
    { updateDimensionProperties: { range: { sheetId, dimension: 'COLUMNS', startIndex: fixedCols, endIndex: totalCols }, properties: { pixelSize: 60 }, fields: 'pixelSize' } }
  );

  // 분할 실행 (Sheets API 제한 대응)
  const CHUNK = 500;
  for (let i = 0; i < requests.length; i += CHUNK) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests: requests.slice(i, i + CHUNK) }
    });
  }

  console.log(`[SHEETS] ${brandName} synced: ${skuIds.length} SKUs × ${allDates.length} dates, ${requests.length} fmt reqs`);
}


async function syncDailyTrend(brandRows) {
  const sheets = await getSheetsAsync();
  const title = 'daily_trend';
  let sheetId;

  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const found = meta.data.sheets.find(s => s.properties.title === title);
    if (found) {
      sheetId = found.properties.sheetId;
    } else {
      const addRes = await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title } } }] }
      });
      sheetId = addRes.data.replies[0].addSheet.properties.sheetId;
    }
  } catch (e) { console.error('Sheet check error:', e.message); return; }

  // 브랜드 순서 고정
  const BRAND_ORDER = ['아리코', '윰', '건우코리아'];
  const allBrands = Object.keys(brandRows);
  const allDates = new Set();
  for (const b of allBrands) for (const r of brandRows[b]) allDates.add(r.date);

  const brands = BRAND_ORDER.filter(b => allBrands.includes(b));
  allBrands.forEach(b => { if (!brands.includes(b)) brands.push(b); });

  const datesDesc = [...allDates].sort().reverse();
  const maps = {};
  for (const b of brands) {
    maps[b] = {};
    for (const r of (brandRows[b] || [])) maps[b][r.date] = (maps[b][r.date] || 0) + r.totalSales;
  }
  const header = ['date', ...brands, 'total'];

  function sumBrandInDateRange(map, start, end) {
    let s = 0;
    for (const d of Object.keys(map)) {
      if (d >= start && d <= end) s += Number(map[d]) || 0;
    }
    return s;
  }

  const latestDate = datesDesc[0] || '';
  const start7 = latestDate ? addDaysIso(latestDate, -6) : '';
  const brandSums7 = brands.map(b => (latestDate ? sumBrandInDateRange(maps[b], start7, latestDate) : 0));
  const total7 = brandSums7.reduce((a, c) => a + c, 0);
  const sum7Row = latestDate
    ? [`최근7일 (${start7}~${latestDate})`, ...brandSums7, total7]
    : ['최근7일 (데이터 없음)', ...brands.map(() => 0), 0];
  const avg7Row = ['일평균 (÷7)', ...brandSums7.map(s => Math.round(s / 7)), Math.round(total7 / 7)];
  const sepRow = new Array(header.length).fill('');
  sepRow[0] = '▼ 일별 상세';

  // 일별 데이터 + 주간 합계 행 (역순: 최신일 → 과거)
  const dataRows = [];
  let weekSums = brands.map(() => 0);
  let weekTotal = 0;
  let weekDayCount = 0;

  for (let di = 0; di < datesDesc.length; di++) {
    const d = datesDesc[di];
    const vals = brands.map(b => maps[b][d] || 0);
    const rowTotal = vals.reduce((a, c) => a + c, 0);
    dataRows.push([d, ...vals, rowTotal]);

    vals.forEach((v, i) => { weekSums[i] += v; });
    weekTotal += rowTotal;
    weekDayCount++;

    const dow = new Date(d + 'T12:00:00Z').getUTCDay();
    if (dow === 0 || di === datesDesc.length - 1) {
      const label = weekDayCount + '일 합계';
      const avgRow = brands.map((b, i) => Math.round(weekSums[i] / weekDayCount));
      const avgTotal = Math.round(weekTotal / weekDayCount);
      dataRows.push([label, ...weekSums, weekTotal]);
      dataRows.push(['일평균(÷' + weekDayCount + ')', ...avgRow, avgTotal]);
      weekSums = brands.map(() => 0);
      weekTotal = 0;
      weekDayCount = 0;
    }
  }

  const allTrendRows = [header, sum7Row, avg7Row, sepRow, ...dataRows];

  try {
    await sheets.spreadsheets.values.clear({ spreadsheetId: SPREADSHEET_ID, range: title + '!A:Z' });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID, range: title + '!A1',
      valueInputOption: 'RAW', requestBody: { values: allTrendRows }
    });
    console.log('[Sheets] daily_trend: ' + dataRows.length + ' rows (일별+주간) + 7일 요약');
  } catch (e) { console.error('[Sheets] trend error:', e.message); return; }

  // ── 데일리트렌드 서식 ──
  if (sheetId == null) return;

  const totalCols = header.length;
  const totalRows = allTrendRows.length;
  const dataStartRow = 4; // 헤더(0) + 최근7일합(1) + 일평균(2) + 구분(3) + 일별(4~)
  const brandColColors = brands.map(b => getBrandColor(b));
  const requests = [];

  // 고정 행/열 — 요약·구분선까지 고정
  requests.push({
    updateSheetProperties: {
      properties: { sheetId, gridProperties: { frozenRowCount: 4, frozenColumnCount: 1 } },
      fields: 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'
    }
  });

  // 헤더 — date 열
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: 0, endColumnIndex: 1 },
      cell: {
        userEnteredFormat: {
          backgroundColor: { red: 0.3, green: 0.3, blue: 0.3 },
          textFormat: { bold: true, foregroundColor: { red: 1, green: 1, blue: 1 } },
          horizontalAlignment: 'CENTER'
        }
      },
      fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
    }
  });

  // 헤더 — 각 브랜드 열
  for (let i = 0; i < brands.length; i++) {
    const color = brandColColors[i];
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: i + 1, endColumnIndex: i + 2 },
        cell: {
          userEnteredFormat: {
            backgroundColor: color.header,
            textFormat: { bold: true, foregroundColor: color.headerText },
            horizontalAlignment: 'CENTER'
          }
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
      }
    });
  }

  // 헤더 — total 열
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 0, endRowIndex: 1, startColumnIndex: totalCols - 1, endColumnIndex: totalCols },
      cell: {
        userEnteredFormat: {
          backgroundColor: { red: 0.2, green: 0.2, blue: 0.2 },
          textFormat: { bold: true, foregroundColor: { red: 1, green: 1, blue: 1 } },
          horizontalAlignment: 'CENTER'
        }
      },
      fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
    }
  });

  // 최근 7일 합계·일평균 행 (대시보드 7일 합계/÷7 과 동일 윈도우)
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 1, endRowIndex: 3, startColumnIndex: 0, endColumnIndex: totalCols },
      cell: {
        userEnteredFormat: {
          backgroundColor: { red: 1, green: 0.97, blue: 0.88 },
          textFormat: { bold: true },
          horizontalAlignment: 'CENTER'
        }
      },
      fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
    }
  });

  // 일별 구분 행
  requests.push({
    repeatCell: {
      range: { sheetId, startRowIndex: 3, endRowIndex: 4, startColumnIndex: 0, endColumnIndex: totalCols },
      cell: {
        userEnteredFormat: {
          backgroundColor: { red: 0.94, green: 0.94, blue: 0.94 },
          textFormat: { bold: true, foregroundColor: { red: 0.35, green: 0.35, blue: 0.35 } },
          horizontalAlignment: 'LEFT'
        }
      },
      fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
    }
  });

  if (totalRows > dataStartRow) {
    // 데이터 — 각 브랜드 열 연한 컬러 (일별만)
    for (let i = 0; i < brands.length; i++) {
      const color = brandColColors[i];
      requests.push({
        repeatCell: {
          range: { sheetId, startRowIndex: dataStartRow, endRowIndex: totalRows, startColumnIndex: i + 1, endColumnIndex: i + 2 },
          cell: {
            userEnteredFormat: {
              backgroundColor: color.light,
              horizontalAlignment: 'CENTER'
            }
          },
          fields: 'userEnteredFormat(backgroundColor,horizontalAlignment)'
        }
      });
    }

    // date 열 가운데 정렬 (일별)
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex: dataStartRow, endRowIndex: totalRows, startColumnIndex: 0, endColumnIndex: 1 },
        cell: { userEnteredFormat: { horizontalAlignment: 'CENTER' } },
        fields: 'userEnteredFormat.horizontalAlignment'
      }
    });

    // total 열 — 회색 + 볼드 (일별)
    requests.push({
      repeatCell: {
        range: { sheetId, startRowIndex: dataStartRow, endRowIndex: totalRows, startColumnIndex: totalCols - 1, endColumnIndex: totalCols },
        cell: {
          userEnteredFormat: {
            backgroundColor: { red: 0.92, green: 0.92, blue: 0.92 },
            textFormat: { bold: true },
            horizontalAlignment: 'CENTER'
          }
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
      }
    });

    // 주간 합계·일평균 행 — 노란 배경
    let rowIdx = dataStartRow;
    for (let di = 0; di < dataRows.length; di++) {
      const firstCell = String(dataRows[di][0]);
      if (firstCell.includes('합계') || firstCell.includes('일평균')) {
        requests.push({
          repeatCell: {
            range: { sheetId, startRowIndex: rowIdx, endRowIndex: rowIdx + 1, startColumnIndex: 0, endColumnIndex: totalCols },
            cell: {
              userEnteredFormat: {
                backgroundColor: { red: 1, green: 1, blue: 0.6 },
                textFormat: { bold: true },
                horizontalAlignment: 'CENTER'
              }
            },
            fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)'
          }
        });
      }
      rowIdx++;
    }
  }

  // 열 너비
  requests.push(
    { updateDimensionProperties: { range: { sheetId, dimension: 'COLUMNS', startIndex: 0, endIndex: 1 }, properties: { pixelSize: 110 }, fields: 'pixelSize' } },
    { updateDimensionProperties: { range: { sheetId, dimension: 'COLUMNS', startIndex: 1, endIndex: totalCols }, properties: { pixelSize: 90 }, fields: 'pixelSize' } }
  );

  try {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests }
    });
    console.log('[Sheets] daily_trend formatted');
  } catch (e) {
    console.error('[Sheets] trend format error:', e.message);
  }
}


const Busboy = require('busboy');
const { parse } = require('csv-parse');
const { getDb } = require('../lib/db');

const BATCH_SIZE = 800;

function detectBrandFromFilename(name) {
  if (!name) return null;
  const n = name.normalize('NFC');
  const lower = n.toLowerCase();
  if (n.includes('건우') || lower.includes('gunu')) return '건우코리아';
  if (n.includes('아리코') || lower.includes('arico')) return '아리코';
  if (n.includes('윰') || lower.includes('yum')) return '윰';
  return null;
}

function normalizeHeader(cell) {
  const s = String(cell ?? '').trim().replace(/^\ufeff/, '');
  const u = s.toLowerCase();
  if (s === '날짜') return 'date';
  if (s === '브랜드') return 'brandCsv';
  if (u === 'sku id' || u === 'skuid' || u === 'sku_id') return 'sku_id';
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

const UPSERT_SQL = `INSERT INTO sales (date, brand, sku_id, sku_name, sales, stock, status, oos_flag, revenue)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(date, sku_id) DO UPDATE SET
  brand = excluded.brand,
  sku_name = excluded.sku_name,
  sales = excluded.sales,
  stock = excluded.stock,
  status = excluded.status,
  oos_flag = excluded.oos_flag,
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
      if (r.oos_flag != null && String(r.oos_flag).trim() !== '') existing.oos_flag = r.oos_flag;
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
    args: [row.date, row.brand, row.sku_id, row.sku_name, row.sales, row.stock, row.status, row.oos_flag ?? '', row.revenue],
  }));
  await db.batch(stmts);
}

function rowFromRecord(rec, fileBrand) {
  const date = toISODate(rec.date);
  const sku_id = rec.sku_id != null ? String(rec.sku_id).trim() : '';
  if (!date || !sku_id) return null;

  const brand = fileBrand || rec.brand || rec['브랜드'] || '';
  const sku_name = rec.sku_name != null ? String(rec.sku_name).trim() : '';
  const sales = num(rec.sales, 0);
  const stockVal = rec.stock;
  const stock = stockVal === '' || stockVal == null ? null : num(stockVal, 0);

  const status = rec.status != null ? String(rec.status).trim() : '';
  const oosRaw = rec.outOfStock != null ? String(rec.outOfStock).trim().toUpperCase() : '';
  let oos_flag = '';
  if (oosRaw === 'Y' || oosRaw === 'YES') oos_flag = 'Y';
  else if (oosRaw === 'N' || oosRaw === 'NO') oos_flag = 'N';

  return { date, brand, sku_id, sku_name, sales, stock, status, oos_flag, revenue: 0 };
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
        console.log('[UPLOAD] raw filename:', filename);
        console.log('[UPLOAD] info object:', JSON.stringify(info));
        fileBrandFromName = detectBrandFromFilename(filename);
        console.log('[UPLOAD] detected brand:', fileBrandFromName);

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

    // Google Sheets 동기화
    if (fileBrandFromName) {
      try {
        const brandName = fileBrandFromName;
        const brandData = await db.execute({
          sql: 'SELECT date, sku_id, sku_name, sales, stock, status FROM sales WHERE brand = ? ORDER BY date DESC, sku_id',
          args: [brandName]
        });
        await syncBrandSheet(brandName, brandData.rows.map(r => ({
          date: r.date, sku_id: r.sku_id, sku_name: r.sku_name,
          sales: Number(r.sales) || 0, stock: r.stock, status: r.status
        })));

        const trendRows = await db.execute(
          'SELECT brand, date, SUM(sales) AS s FROM sales GROUP BY brand, date ORDER BY date'
        );
        const trendByBrand = {};
        for (const r of trendRows.rows) {
          if (!trendByBrand[r.brand]) trendByBrand[r.brand] = [];
          trendByBrand[r.brand].push({ date: r.date, totalSales: Number(r.s) || 0 });
        }
        await syncDailyTrend(trendByBrand);
        console.log('[Sheets] sync complete for', brandName);
      } catch (e) {
        console.error('[Sheets sync error]', e.message);
      }
    }

    return res.status(200).json({
      ok: true,
      rows: totalRows,
      fileBrand: fileBrandFromName,
      message: `${totalRows.toLocaleString()}행 반영 완료 (시트 동기화 완료)`,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'upload failed' });
  }
};