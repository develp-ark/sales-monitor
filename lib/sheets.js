const { google } = require('googleapis');

const SPREADSHEET_ID = '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw';

function getAuth() {
  const raw = process.env.GOOGLE_PRIVATE_KEY || '';
  // \\n (이중 이스케이프) 또는 \n (단일) 모두 실제 줄바꿈으로 변환
  const key = raw.replace(/\\\\n/g, '\n').replace(/\\n/g, '\n');
  const auth = new google.auth.JWT(
    process.env.GOOGLE_CLIENT_EMAIL,
    null,
    key,
    ['https://www.googleapis.com/auth/spreadsheets']
  );
  return auth;
}

function getSheets() {
  return google.sheets({ version: 'v4', auth: getAuth() });
}

async function syncBrandSheet(brandName, rows) {
  // rows: [{date, sku_id, sku_name, sales, stock, status}]
  const sheets = getSheets();
  const sheetTitle = brandName;

  // 시트 존재 확인, 없으면 생성
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = meta.data.sheets.some(s => s.properties.title === sheetTitle);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          requests: [{ addSheet: { properties: { title: sheetTitle } } }]
        }
      });
    }
  } catch (e) {
    console.error('Sheet check error:', e.message);
  }

  // 헤더 + 데이터
  const header = ['날짜', 'SKU ID', 'SKU명', '출고수량', '현재재고', '상태'];
  const data = rows.map(r => [
    r.date, r.sku_id, r.sku_name,
    r.sales, r.stock != null ? r.stock : '', r.status
  ]);

  // 기존 데이터에서 같은 날짜 제거 후 추가 (upsert)
  // 간단하게: 전체 덮어쓰기 방식
  const allRows = [header, ...data];

  try {
    // 기존 데이터 읽기
    let existing = [];
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: `${sheetTitle}!A:F`,
      });
      existing = res.data.values || [];
    } catch (e) { /* 빈 시트 */ }

    // 새 날짜 목록
    const newDates = new Set(rows.map(r => r.date));

    // 기존 데이터에서 새 날짜와 겹치는 행 제거 (헤더 제외)
    const kept = existing.length > 1
      ? existing.slice(1).filter(row => !newDates.has(row[0]))
      : [];

    // 합치기: 헤더 + 기존(중복제거) + 신규
    const merged = [header, ...kept, ...data];

    // 날짜 내림차순 정렬 (헤더 제외)
    merged.slice(1).sort((a, b) => {
      if (a[0] > b[0]) return -1;
      if (a[0] < b[0]) return 1;
      return 0;
    });
    const sorted = [header, ...merged.slice(1).sort((a, b) => b[0] < a[0] ? -1 : b[0] > a[0] ? 1 : 0)];

    // 시트 클리어 후 쓰기
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetTitle}!A:F`,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetTitle}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: sorted },
    });

    console.log(`[Sheets] ${sheetTitle}: ${data.length} rows synced`);
  } catch (e) {
    console.error(`[Sheets] ${sheetTitle} sync error:`, e.message);
  }
}

async function syncDailyTrend(brandRows) {
  // brandRows: {brandName: [{date, totalSales}]}
  const sheets = getSheets();
  const sheetTitle = '일별_판매추이';

  // 시트 존재 확인
  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = meta.data.sheets.some(s => s.properties.title === sheetTitle);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: {
          requests: [{ addSheet: { properties: { title: sheetTitle } } }]
        }
      });
    }
  } catch (e) {
    console.error('Sheet check error:', e.message);
  }

  // 모든 날짜 수집
  const allDates = new Set();
  const brands = Object.keys(brandRows);
  for (const b of brands) {
    for (const r of brandRows[b]) {
      allDates.add(r.date);
    }
  }
  const dates = [...allDates].sort().reverse();

  // 브랜드별 날짜→판매 맵
  const maps = {};
  for (const b of brands) {
    maps[b] = {};
    for (const r of brandRows[b]) {
      maps[b][r.date] = (maps[b][r.date] || 0) + r.totalSales;
    }
  }

  // 헤더
  const header = ['날짜', ...brands, '합계'];
  const rows = dates.map(d => {
    const vals = brands.map(b => maps[b][d] || 0);
    const total = vals.reduce((a, c) => a + c, 0);
    return [d, ...vals, total];
  });

  try {
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetTitle}!A:Z`,
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetTitle}!A1`,
      valueInputOption: 'RAW',
      requestBody: { values: [header, ...rows] },
    });

    console.log(`[Sheets] 일별_판매추이: ${rows.length} rows synced`);
  } catch (e) {
    console.error(`[Sheets] 일별_판매추이 sync error:`, e.message);
  }
}

module.exports = { syncBrandSheet, syncDailyTrend, SPREADSHEET_ID };
