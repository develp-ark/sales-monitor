const { google } = require('googleapis');

module.exports = async function handler(req, res) {
  try {
    const raw = process.env.GOOGLE_PRIVATE_KEY || '';
    const key = raw.replace(/\\\\n/g, '\n').replace(/\\n/g, '\n');
    
    const auth = new google.auth.JWT(
      process.env.GOOGLE_CLIENT_EMAIL,
      null,
      key,
      ['https://www.googleapis.com/auth/spreadsheets']
    );

    // 1) 토큰 획득 테스트
    await auth.authorize();

    // 2) 시트 접근 테스트
    const sheets = google.sheets({ version: 'v4', auth });
    const meta = await sheets.spreadsheets.get({
      spreadsheetId: '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw'
    });

    const sheetNames = meta.data.sheets.map(s => s.properties.title);

    res.json({ ok: true, sheetNames });
  } catch (e) {
    res.json({ 
      ok: false, 
      error: e.message,
      code: e.code,
      stack: e.stack?.split('\n').slice(0, 3)
    });
  }
};
