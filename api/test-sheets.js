const { google } = require('googleapis');

module.exports = async function handler(req, res) {
  const raw = process.env.GOOGLE_PRIVATE_KEY || '';
  
  const debug = {
    rawLength: raw.length,
    first50: raw.substring(0, 50),
    last30: raw.substring(raw.length - 30),
    includesBegin: raw.includes('-----BEGIN'),
    includesEnd: raw.includes('-----END'),
    rawHasRealNewline: raw.includes('\n'),
    rawHasLiteralBackslashN: raw.includes('\\n'),
  };

  try {
    let key = raw;
    if (!raw.includes('\n') && raw.includes('\\n')) {
      key = raw.replace(/\\n/g, '\n');
    }

    debug.keyLength = key.length;
    debug.keyLineCount = key.split('\n').length;

    const auth = new google.auth.JWT(
      process.env.GOOGLE_CLIENT_EMAIL,
      null,
      key,
      ['https://www.googleapis.com/auth/spreadsheets']
    );

    await auth.authorize();

    const sheets = google.sheets({ version: 'v4', auth });
    const meta = await sheets.spreadsheets.get({
      spreadsheetId: '1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw'
    });
    const sheetNames = meta.data.sheets.map(s => s.properties.title);

    res.json({ ok: true, sheetNames, debug });
  } catch (e) {
    res.json({ ok: false, error: e.message, debug });
  }
};
