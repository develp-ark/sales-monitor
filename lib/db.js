const { createClient } = require('@libsql/client');

let _client;

function getDb() {
  if (_client) return _client;
  const url = process.env.TURSO_URL;
  const authToken = process.env.TURSO_TOKEN;
  if (!url || !authToken) {
    throw new Error('TURSO_URL and TURSO_TOKEN must be set');
  }
  _client = createClient({ url, authToken });
  return _client;
}

module.exports = { getDb };
