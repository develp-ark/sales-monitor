module.exports = async function handler(req, res) {
  const email = process.env.GOOGLE_CLIENT_EMAIL || '(not set)';
  const rawKey = process.env.GOOGLE_PRIVATE_KEY || '(not set)';

  const keyPreview = rawKey.substring(0, 30);
  const keyLength = rawKey.length;
  const hasBegin = rawKey.includes('-----BEGIN');
  const hasNewline = rawKey.includes('\n');
  const hasLiteralBackslashN = rawKey.includes('\\n');

  res.json({
    email,
    keyLength,
    keyPreview,
    hasBegin,
    hasNewline,
    hasLiteralBackslashN,
  });
};
