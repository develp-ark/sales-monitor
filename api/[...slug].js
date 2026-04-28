/**
 * 단일 서버리스 진입점 — Hobby 플랜(함수 12개 제한) 회피.
 * 기존 /api/<이름> 은 첫 경로 세그먼트로 디스패치합니다.
 */
const handlers = {
  dashboard: require('../lib/handlers/dashboard'),
  exclude: require('../lib/handlers/exclude'),
  export: require('../lib/handlers/export'),
  flag: require('../lib/handlers/flag'),
  init: require('../lib/handlers/init'),
  insight: require('../lib/handlers/insight'),
  'new-product-manual': require('../lib/handlers/new-product-manual'),
  'sku-manage': require('../lib/handlers/sku-manage'),
  'test-sheets': require('../lib/handlers/test-sheets'),
  test: require('../lib/handlers/test'),
  'update-price': require('../lib/handlers/update-price'),
  'upload-monthly': require('../lib/handlers/upload-monthly'),
  'upload-sku-manage': require('../lib/handlers/upload-sku-manage'),
  upload: require('../lib/handlers/upload'),
};

function firstSegment(req) {
  const q = req.query && req.query.slug;
  if (q != null && q !== '') {
    return Array.isArray(q) ? q[0] : q;
  }
  try {
    const raw = req.url || '';
    const pathOnly = raw.split('?')[0];
    const parts = pathOnly.replace(/^\/api\/?/, '').split('/').filter(Boolean);
    return parts[0] || '';
  } catch (e) {
    return '';
  }
}

module.exports = async (req, res) => {
  const key = firstSegment(req);
  const h = handlers[key];
  if (!h) {
    res.status(404).json({ error: 'Unknown route', route: key || '(empty)' });
    return;
  }
  return h(req, res);
};
