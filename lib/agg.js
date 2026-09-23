/**
 * 파생 집계 테이블 (agg_*).
 *
 * Turso 는 "읽은 행 수"로 과금/차단하므로, 매 요청마다 sales 전체를 GROUP BY 하던
 * 쿼리를 미리 계산된 작은 테이블로 대체한다. 언제든 rebuildAll() 로 다시 만들 수 있다.
 *
 *  - agg_brand_daily    : (brand, date)          -> 일별 브랜드 매출 합계
 *  - agg_sku_month      : (brand, sku_id, month) -> SKU 월별 매출 합계
 *  - agg_sku_first_seen : (brand, sku_id)        -> 최초 등장일
 *  - agg_archive_sku    : (brand, sku_id)        -> 월별 아카이브의 최초 월과 그때의 이름
 *  - agg_archive_month  : (brand, month)         -> 월별 아카이브에 존재하는 월
 *
 * 제외 SKU(sku_exclude) 는 agg_brand_daily / agg_sku_month 에서 빠진다. 화면과
 * 다운로드가 같은 기준을 쓰도록 하기 위한 것이며, 제외 목록이 바뀌면
 * onExcludeChanged() 로 두 테이블을 다시 만들어야 한다.
 * agg_sku_first_seen 은 단순 조회용 맵이라 제외를 반영하지 않는다.
 */

const NOT_EXCLUDED = 'sku_id NOT IN (SELECT sku_id FROM sku_exclude)';

const CREATE_SQL = [
  // 제외 목록은 집계 조건에 쓰이므로 없으면 만든다.
  `CREATE TABLE IF NOT EXISTS sku_exclude (
    sku_id TEXT PRIMARY KEY, sku_name TEXT, brand TEXT,
    excluded_at TEXT DEFAULT (datetime('now'))
  )`,
  `CREATE TABLE IF NOT EXISTS agg_brand_daily (
    brand TEXT NOT NULL,
    date TEXT NOT NULL,
    sales INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (brand, date)
  )`,
  `CREATE TABLE IF NOT EXISTS agg_sku_month (
    brand TEXT NOT NULL,
    sku_id TEXT NOT NULL,
    sku_name TEXT,
    month TEXT NOT NULL,
    sales INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (brand, sku_id, month)
  )`,
  `CREATE TABLE IF NOT EXISTS agg_sku_first_seen (
    brand TEXT NOT NULL,
    sku_id TEXT NOT NULL,
    first_date TEXT NOT NULL,
    PRIMARY KEY (brand, sku_id)
  )`,
  // 품절 시작일 역추적(sku_id 고정 + date 역방향)이 인덱스만 훑고 끝나도록.
  'CREATE INDEX IF NOT EXISTS idx_sales_sku_date ON sales(sku_id, date)',
  // 인사이트가 페이지 로드마다 sales_monthly 를 통째로 훑던 것을 월 범위 검색으로 바꾼다.
  'CREATE INDEX IF NOT EXISTS idx_sales_monthly_month ON sales_monthly(month)',
  // sales_monthly 는 4분의 3이 매출 0 이다. 지우지 않고도 매번 훑지 않도록 비영 행만 담는
  // 부분 커버링 인덱스를 둔다 (24.5만 -> 6만).
  'CREATE INDEX IF NOT EXISTS idx_sales_monthly_nz ON sales_monthly(month, brand, sku_id, sales) WHERE sales <> 0',
  // 0 행만 기여하던 정보(월 축 / SKU 이름)를 따로 보관해 0 행을 읽지 않아도 되게 한다.
  `CREATE TABLE IF NOT EXISTS agg_archive_sku (
    brand TEXT NOT NULL,
    sku_id TEXT NOT NULL,
    sku_name TEXT,
    first_month TEXT NOT NULL,
    PRIMARY KEY (brand, sku_id)
  )`,
  `CREATE TABLE IF NOT EXISTS agg_archive_month (
    brand TEXT NOT NULL,
    month TEXT NOT NULL,
    PRIMARY KEY (brand, month)
  )`,
  // 인사이트 응답 캐시. 서버리스라 인스턴스 메모리 캐시는 거의 안 맞는다.
  `CREATE TABLE IF NOT EXISTS agg_insight_cache (
    cache_key TEXT PRIMARY KEY,
    payload BLOB NOT NULL,
    created_at INTEGER NOT NULL
  )`,
];

/** 월별 아카이브 메타 갱신: 이름은 더 이른 월 쪽이 이긴다 (병합 결과의 첫 항목 규칙). */
const UPSERT_ARCHIVE_SKU = `INSERT INTO agg_archive_sku (brand, sku_id, sku_name, first_month)
VALUES (?, ?, ?, ?)
ON CONFLICT(brand, sku_id) DO UPDATE SET
  sku_name = CASE WHEN excluded.first_month <= agg_archive_sku.first_month
                  THEN excluded.sku_name ELSE agg_archive_sku.sku_name END,
  first_month = MIN(agg_archive_sku.first_month, excluded.first_month)`;

const UPSERT_FIRST_SEEN = `INSERT INTO agg_sku_first_seen (brand, sku_id, first_date)
VALUES (?, ?, ?)
ON CONFLICT(brand, sku_id) DO UPDATE SET
  first_date = MIN(agg_sku_first_seen.first_date, excluded.first_date)`;

let _ensured = false;
// 같은 인스턴스에서 여러 요청/쿼리가 동시에 리빌드를 시작하면 DELETE 와 INSERT 가 엇갈려
// PK 충돌이 난다. 진행 중인 리빌드가 있으면 그것을 함께 기다린다.
const _rebuilding = new Map();
function once(key, fn) {
  const running = _rebuilding.get(key);
  if (running) return running;
  const p = fn().finally(() => _rebuilding.delete(key));
  _rebuilding.set(key, p);
  return p;
}

async function ensureAggTables(db) {
  if (_ensured) return;
  for (const sql of CREATE_SQL) {
    try { await db.execute(sql); } catch (e) { console.log('[AGG] ensure 실패:', e.message); }
  }
  _ensured = true;
}

function monthBounds(month) {
  const start = month + '-01';
  const d = new Date(start + 'T12:00:00Z');
  const last = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0));
  return { start, end: last.toISOString().slice(0, 10) };
}

/* ── 전체 재계산 (최초 1회 / 제외 목록 변경 / 수동 리빌드) ─────────────── */

async function rebuildBrandDaily(db) {
  return once('brand_daily', async () => {
    await ensureAggTables(db);
    await db.execute('DELETE FROM agg_brand_daily');
    // 다른 인스턴스가 동시에 리빌드해도 충돌로 실패하지 않도록 upsert 로 넣는다.
    await db.execute(`INSERT INTO agg_brand_daily (brand, date, sales)
      SELECT brand, date, COALESCE(SUM(sales), 0) FROM sales
      WHERE date IS NOT NULL AND date <> '' AND ${NOT_EXCLUDED}
      GROUP BY brand, date
      ON CONFLICT(brand, date) DO UPDATE SET sales = excluded.sales`);
  });
}

async function rebuildSkuMonth(db) {
  return once('sku_month', async () => {
    await ensureAggTables(db);
    await db.execute('DELETE FROM agg_sku_month');
    await db.execute(`INSERT INTO agg_sku_month (brand, sku_id, sku_name, month, sales)
      SELECT brand, sku_id, MAX(sku_name), substr(date, 1, 7), COALESCE(SUM(sales), 0) FROM sales
      WHERE date IS NOT NULL AND date <> '' AND ${NOT_EXCLUDED}
      GROUP BY brand, sku_id, substr(date, 1, 7)
      ON CONFLICT(brand, sku_id, month) DO UPDATE SET
        sku_name = excluded.sku_name, sales = excluded.sales`);
  });
}

async function rebuildFirstSeen(db) {
  return once('first_seen', () => _rebuildFirstSeen(db));
}

async function _rebuildFirstSeen(db) {
  await ensureAggTables(db);
  // INSERT ... SELECT 에 upsert 를 붙일 때 SQLite 파서가 WHERE 절을 요구한다.
  await db.execute(`INSERT INTO agg_sku_first_seen (brand, sku_id, first_date)
    SELECT brand, sku_id, MIN(date) FROM sales
    WHERE date IS NOT NULL AND date <> ''
    GROUP BY brand, sku_id
    ON CONFLICT(brand, sku_id) DO UPDATE SET
      first_date = MIN(agg_sku_first_seen.first_date, excluded.first_date)`);
  try {
    await db.execute(`INSERT INTO agg_sku_first_seen (brand, sku_id, first_date)
      SELECT brand, sku_id, MIN(month) || '-01' FROM sales_monthly
      WHERE month IS NOT NULL AND month <> ''
      GROUP BY brand, sku_id
      ON CONFLICT(brand, sku_id) DO UPDATE SET
        first_date = MIN(agg_sku_first_seen.first_date, excluded.first_date)`);
  } catch (e) { /* sales_monthly 가 아직 없을 수 있다 */ }
}

/** sales_monthly 전체를 훑어 아카이브 메타를 다시 만든다. 드물게 일어나는 작업. */
async function rebuildArchiveMeta(db) {
  return once('archive_meta', async () => {
    await ensureAggTables(db);
    try {
      await db.execute('DELETE FROM agg_archive_sku');
      // UNIQUE(brand, sku_id, month) 이므로 MIN(month) 행이 유일하고, sku_name 은 그 행의 값이 된다.
      await db.execute(`INSERT INTO agg_archive_sku (brand, sku_id, sku_name, first_month)
        SELECT brand, sku_id, sku_name, MIN(month) FROM sales_monthly
        WHERE month IS NOT NULL AND month <> '' AND ${NOT_EXCLUDED}
        GROUP BY brand, sku_id
        ON CONFLICT(brand, sku_id) DO UPDATE SET
          sku_name = excluded.sku_name, first_month = excluded.first_month`);
      await db.execute('DELETE FROM agg_archive_month');
      await db.execute(`INSERT INTO agg_archive_month (brand, month)
        SELECT DISTINCT brand, month FROM sales_monthly
        WHERE month IS NOT NULL AND month <> '' AND ${NOT_EXCLUDED}
        ON CONFLICT(brand, month) DO NOTHING`);
    } catch (e) { console.log('[AGG] archive meta 실패:', e.message); }
  });
}

/** 월별 업로드 시 증분 갱신. 0 인 달도 반영해야 월 축과 이름이 보존된다. */
async function touchArchiveMeta(db, items) {
  if (!items || !items.length) return;
  await ensureAggTables(db);
  // 제외 SKU 는 메타에도 들어가면 안 된다 (다운로드에서 그 SKU 행이 되살아난다).
  const excluded = new Set(
    (await db.execute('SELECT sku_id FROM sku_exclude')).rows.map((r) => String(r.sku_id)));
  const skus = new Map(), months = new Map();
  for (const it of items) {
    if (!it.brand || !it.sku_id || !it.month) continue;
    if (excluded.has(String(it.sku_id))) continue;
    const k = it.brand + '||' + it.sku_id;
    const prev = skus.get(k);
    if (!prev || it.month < prev.month) skus.set(k, it);
    months.set(it.brand + '||' + it.month, it);
  }
  const stmts = [
    ...[...skus.values()].map((v) => ({ sql: UPSERT_ARCHIVE_SKU, args: [v.brand, String(v.sku_id), v.sku_name || '', v.month] })),
    ...[...months.values()].map((v) => ({ sql: 'INSERT OR IGNORE INTO agg_archive_month (brand, month) VALUES (?, ?)', args: [v.brand, v.month] })),
  ];
  for (let i = 0; i < stmts.length; i += 100) await db.batch(stmts.slice(i, i + 100));
}

/** [{brand, sku_id, sku_name, first_month}] */
async function loadArchiveSku(db) {
  await ensureAggTables(db);
  if (await isEmpty(db, 'agg_archive_sku')) await rebuildArchiveMeta(db);
  const r = await db.execute('SELECT brand, sku_id, sku_name, first_month FROM agg_archive_sku');
  return r.rows;
}

/** [{brand, month}] */
async function loadArchiveMonth(db) {
  await ensureAggTables(db);
  if (await isEmpty(db, 'agg_archive_month')) await rebuildArchiveMeta(db);
  const r = await db.execute('SELECT brand, month FROM agg_archive_month');
  return r.rows;
}

async function rebuildAll(db) {
  await rebuildFirstSeen(db);
  await rebuildBrandDaily(db);
  await rebuildSkuMonth(db);
  await rebuildArchiveMeta(db);
  await clearInsightCache(db);
}

/** 제외 목록이 바뀌면 제외 반영 집계를 통째로 다시 만든다. 드물게 일어나는 작업. */
async function onExcludeChanged(db) {
  await rebuildBrandDaily(db);
  await rebuildSkuMonth(db);
  await rebuildArchiveMeta(db);
  await clearInsightCache(db);
}

/* ── 업로드 시 증분 갱신 ────────────────────────────────────────────── */

/** 업로드된 행에서 (brand, sku_id) 최초일을 갱신한다. 읽기 0회 — 순수 쓰기. */
async function touchFirstSeen(db, rows) {
  if (!rows || !rows.length) return;
  await ensureAggTables(db);
  const earliest = new Map();
  for (const r of rows) {
    if (!r.brand || !r.sku_id || !r.date) continue;
    const k = r.brand + '||' + r.sku_id;
    const prev = earliest.get(k);
    if (!prev || r.date < prev.date) earliest.set(k, { brand: r.brand, sku_id: String(r.sku_id), date: r.date });
  }
  const stmts = [...earliest.values()].map((v) => ({ sql: UPSERT_FIRST_SEEN, args: [v.brand, v.sku_id, v.date] }));
  for (let i = 0; i < stmts.length; i += 100) await db.batch(stmts.slice(i, i + 100));
}

/** 바뀐 (brand, date) 조합만 다시 집계한다. 조합이 많으면 전체 재계산이 더 싸다. */
async function refreshBrandDaily(db, pairs) {
  if (!pairs || !pairs.length) return;
  await ensureAggTables(db);
  if (pairs.length > 200) { await rebuildBrandDaily(db); return; }
  const stmts = pairs.map(({ brand, date }) => ({
    sql: `INSERT INTO agg_brand_daily (brand, date, sales)
      SELECT brand, date, COALESCE(SUM(sales), 0) FROM sales
      WHERE brand = ? AND date = ? AND ${NOT_EXCLUDED}
      GROUP BY brand, date
      ON CONFLICT(brand, date) DO UPDATE SET sales = excluded.sales`,
    args: [brand, date],
  }));
  for (let i = 0; i < stmts.length; i += 50) await db.batch(stmts.slice(i, i + 50));
}

/** 바뀐 (brand, month) 조합만 다시 집계한다. */
async function refreshSkuMonth(db, pairs) {
  if (!pairs || !pairs.length) return;
  await ensureAggTables(db);
  if (pairs.length > 24) { await rebuildSkuMonth(db); return; }
  for (const { brand, month } of pairs) {
    const { start, end } = monthBounds(month);
    // 해당 월에서 사라진 SKU 가 남지 않도록 먼저 지우고 다시 넣는다.
    await db.execute({ sql: 'DELETE FROM agg_sku_month WHERE brand = ? AND month = ?', args: [brand, month] });
    await db.execute({
      sql: `INSERT INTO agg_sku_month (brand, sku_id, sku_name, month, sales)
        SELECT brand, sku_id, MAX(sku_name), ?, COALESCE(SUM(sales), 0) FROM sales
        WHERE brand = ? AND date >= ? AND date <= ? AND ${NOT_EXCLUDED}
        GROUP BY brand, sku_id`,
      args: [month, brand, start, end],
    });
  }
}

/** 업로드 행 목록에서 갱신이 필요한 조합을 뽑는다. */
function distinctBrandDates(rows) {
  const seen = new Map();
  for (const r of rows || []) {
    if (!r.brand || !r.date) continue;
    const k = r.brand + '||' + r.date;
    if (!seen.has(k)) seen.set(k, { brand: r.brand, date: r.date });
  }
  return [...seen.values()];
}

function distinctBrandMonths(rows) {
  const seen = new Map();
  for (const r of rows || []) {
    if (!r.brand || !r.date) continue;
    const month = String(r.date).slice(0, 7);
    const k = r.brand + '||' + month;
    if (!seen.has(k)) seen.set(k, { brand: r.brand, month });
  }
  return [...seen.values()];
}

/** 업로드 직후 집계를 맞추고, 낡은 인사이트 캐시를 버린다. */
async function refreshAfterUpload(db, rows) {
  await touchFirstSeen(db, rows);
  await refreshBrandDaily(db, distinctBrandDates(rows));
  await refreshSkuMonth(db, distinctBrandMonths(rows));
  await clearInsightCache(db);
}

/* ── 인사이트 응답 캐시 ─────────────────────────────────────────────── */

const INSIGHT_TTL_MS = 24 * 60 * 60 * 1000;
const INSIGHT_MAX_BYTES = 900 * 1024;

/**
 * 인사이트는 호출 1회에 약 11만 행을 훑는다. 같은 기간을 다시 볼 때 1행 읽기로 끝내려고
 * 응답을 gzip 해 저장한다. 데이터가 바뀌는 경로에서 전부 비우고, TTL 은 그 경로를
 * 놓쳤을 때를 대비한 안전장치다.
 */
async function loadInsightCache(db, key) {
  try {
    // 캐시 히트는 왕복 1회로 끝나야 한다. 테이블 보장(CREATE IF NOT EXISTS 여러 개)은
    // 저장 시점에만 하고, 여기서는 없으면 그냥 미스로 취급한다.
    const r = await db.execute({
      sql: 'SELECT payload, created_at FROM agg_insight_cache WHERE cache_key = ?',
      args: [key],
    });
    if (!r.rows.length) return null;
    const age = Date.now() - Number(r.rows[0].created_at);
    if (!(age >= 0 && age < INSIGHT_TTL_MS)) return null;
    const zlib = require('zlib');
    return JSON.parse(zlib.gunzipSync(Buffer.from(r.rows[0].payload)).toString('utf8'));
  } catch (e) {
    console.log('[AGG] 인사이트 캐시 읽기 건너뜀:', e.message);
    return null;
  }
}

async function saveInsightCache(db, key, value) {
  try {
    await ensureAggTables(db);
    const zlib = require('zlib');
    const buf = zlib.gzipSync(Buffer.from(JSON.stringify(value), 'utf8'));
    if (buf.length > INSIGHT_MAX_BYTES) return;
    await db.execute({
      sql: `INSERT INTO agg_insight_cache (cache_key, payload, created_at) VALUES (?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET payload = excluded.payload, created_at = excluded.created_at`,
      args: [key, buf, Date.now()],
    });
  } catch (e) {
    console.log('[AGG] 인사이트 캐시 저장 건너뜀:', e.message);
  }
}

async function clearInsightCache(db) {
  try {
    await ensureAggTables(db);
    await db.execute('DELETE FROM agg_insight_cache');
  } catch (e) {
    console.log('[AGG] 인사이트 캐시 비우기 실패:', e.message);
  }
}

/* ── 조회 (비어 있으면 최초 1회 자동 재계산) ────────────────────────── */

async function isEmpty(db, table) {
  const r = await db.execute(`SELECT 1 AS x FROM ${table} LIMIT 1`);
  return r.rows.length === 0;
}

/** { 'brand||sku_id': 'YYYY-MM-DD' } 맵 */
async function loadFirstSeen(db) {
  await ensureAggTables(db);
  if (await isEmpty(db, 'agg_sku_first_seen')) await rebuildFirstSeen(db);
  const r = await db.execute('SELECT brand, sku_id, first_date FROM agg_sku_first_seen');
  const map = {};
  for (const row of r.rows) {
    if (row.first_date) map[row.brand + '||' + row.sku_id] = row.first_date;
  }
  return map;
}

/** 기간 내 (brand, date, sales) 행 */
async function loadBrandDaily(db, start, end) {
  await ensureAggTables(db);
  if (await isEmpty(db, 'agg_brand_daily')) await rebuildBrandDaily(db);
  const r = await db.execute({
    sql: 'SELECT brand, date, sales FROM agg_brand_daily WHERE date >= ? AND date <= ? ORDER BY date',
    args: [start, end],
  });
  return r.rows;
}

/** month < beforeMonth 인 SKU 월별 집계 */
async function loadSkuMonthBefore(db, beforeMonth) {
  await ensureAggTables(db);
  if (await isEmpty(db, 'agg_sku_month')) await rebuildSkuMonth(db);
  const r = await db.execute({
    sql: 'SELECT brand, sku_id, sku_name, month, sales FROM agg_sku_month WHERE month < ?',
    args: [beforeMonth],
  });
  return r.rows;
}

/** 집계에 남아 있는 브랜드 목록 */
async function loadBrands(db) {
  await ensureAggTables(db);
  if (await isEmpty(db, 'agg_brand_daily')) await rebuildBrandDaily(db);
  const r = await db.execute('SELECT DISTINCT brand FROM agg_brand_daily ORDER BY brand');
  return r.rows.map((x) => x.brand);
}

module.exports = {
  NOT_EXCLUDED,
  ensureAggTables,
  rebuildAll,
  rebuildBrandDaily,
  rebuildSkuMonth,
  rebuildFirstSeen,
  rebuildArchiveMeta,
  touchArchiveMeta,
  loadArchiveSku,
  loadArchiveMonth,
  loadInsightCache,
  saveInsightCache,
  clearInsightCache,
  onExcludeChanged,
  touchFirstSeen,
  refreshBrandDaily,
  refreshSkuMonth,
  refreshAfterUpload,
  distinctBrandDates,
  distinctBrandMonths,
  loadFirstSeen,
  loadBrandDaily,
  loadSkuMonthBefore,
  loadBrands,
};
