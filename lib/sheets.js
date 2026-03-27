const { google } = require('googleapis');

const SPREADSHEET_ID = '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw';

function getAuth() {
  let key = process.env.GOOGLE_PRIVATE_KEY || '';
  if (!key.includes('\n') && key.includes('\\n')) {
    key = key.replace(/\\n/g, '\n');
  }
  return new google.auth.JWT(
    process.env.GOOGLE_CLIENT_EMAIL,
    null,
    key,
    ['https://www.googleapis.com/auth/spreadsheets']
  );
}

function getSheets() {
  return google.sheets({ version: 'v4', auth: getAuth() });
}

async function syncBrandSheet(brandName, rows) {
  const sheets = getSheets();
  const sheetTitle = brandName;

  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = meta.data.sheets.some(s => s.properties.title === sheetTitle);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: sheetTitle } } }] }
      });
    }
  } catch (e) {
    console.error('Sheet check error:', e.message);
    return;
  }

  const header = ['date', 'SKU ID', 'SKU name', 'sales', 'stock', 'status'];
  const data = rows.map(r => [
    r.date, r.sku_id, r.sku_name,
    r.sales, r.stock != null ? r.stock : '', r.status
  ]);

  try {
    let existing = [];
    try {
      const res = await sheets.spreadsheets.values.get({
        spreadsheetId: SPREADSHEET_ID,
        range: sheetTitle + '!A:F',
      });
      existing = res.data.values || [];
    } catch (e) { /* empty sheet */ }

    const newDates = new Set(rows.map(r => r.date));
    const kept = existing.length > 1
      ? existing.slice(1).filter(row => !newDates.has(row[0]))
      : [];

    const sorted = [header, ...[...kept, ...data].sort((a, b) => (b[0] > a[0] ? 1 : b[0] < a[0] ? -1 : 0))];

    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: sheetTitle + '!A:F',
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: sheetTitle + '!A1',
      valueInputOption: 'RAW',
      requestBody: { values: sorted },
    });
    console.log('[Sheets] ' + sheetTitle + ': ' + data.length + ' rows synced');
  } catch (e) {
    console.error('[Sheets] ' + sheetTitle + ' sync error:', e.message);
  }
}

async function syncDailyTrend(brandRows) {
  const sheets = getSheets();
  const sheetTitle = 'daily_trend';

  try {
    const meta = await sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID });
    const exists = meta.data.sheets.some(s => s.properties.title === sheetTitle);
    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: SPREADSHEET_ID,
        requestBody: { requests: [{ addSheet: { properties: { title: sheetTitle } } }] }
      });
    }
  } catch (e) {
    console.error('Sheet check error:', e.message);
    return;
  }

  const allDates = new Set();
  const brands = Object.keys(brandRows);
  for (const b of brands) {
    for (const r of brandRows[b]) allDates.add(r.date);
  }
  const dates = [...allDates].sort().reverse();

  const maps = {};
  for (const b of brands) {
    maps[b] = {};
    for (const r of brandRows[b]) {
      maps[b][r.date] = (maps[b][r.date] || 0) + r.totalSales;
    }
  }

  const header = ['date', ...brands, 'total'];
  const dataRows = dates.map(d => {
    const vals = brands.map(b => maps[b][d] || 0);
    return [d, ...vals, vals.reduce((a, c) => a + c, 0)];
  });

  try {
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: sheetTitle + '!A:Z',
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: sheetTitle + '!A1',
      valueInputOption: 'RAW',
      requestBody: { values: [header, ...dataRows] },
    });
    console.log('[Sheets] daily_trend: ' + dataRows.length + ' rows synced');
  } catch (e) {
    console.error('[Sheets] daily_trend sync error:', e.message);
  }
}

module.exports = { syncBrandSheet, syncDailyTrend, SPREADSHEET_ID };
