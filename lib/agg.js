/**
 * 파생 집계 테이블 (agg_*).
 *
 * Turso 는 "읽은 행 수"로 과금/차단하므로, 매 요청마다 sales 전체를 GROUP BY 하던
 * 쿼리를 미리 계산된 작은 테이블로 대체한다. 언제든 rebuildAll() 로 다시 만들 수 있다.
 *
 *  - agg_brand_daily    : (brand, date)          -> 일별 브랜드 매출 합계
 *  - agg_sku_month      : (brand, sku_id, month) -> SKU 월별 매출 합계
 *  - agg_sku_first_seen : (brand, sku_id)        -> 최초 등장일
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
];

const UPSERT_FIRST_SEEN = `INSERT INTO agg_sku_first_seen (brand, sku_id, first_date)
VALUES (?, ?, ?)
ON CONFLICT(brand, sku_id) DO UPDATE SET
  first_date = MIN(agg_sku_first_seen.first_date, excluded.first_date)`;

let _ensured = false;

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
  await ensureAggTables(db);
  await db.execute('DELETE FROM agg_brand_daily');
  await db.execute(`INSERT INTO agg_brand_daily (brand, date, sales)
    SELECT brand, date, COALESCE(SUM(sales), 0) FROM sales
    WHERE date IS NOT NULL AND date <> '' AND ${NOT_EXCLUDED}
    GROUP BY brand, date`);
}

async function rebuildSkuMonth(db) {
  await ensureAggTables(db);
  await db.execute('DELETE FROM agg_sku_month');
  await db.execute(`INSERT INTO agg_sku_month (brand, sku_id, sku_name, month, sales)
    SELECT brand, sku_id, MAX(sku_name), substr(date, 1, 7), COALESCE(SUM(sales), 0) FROM sales
    WHERE date IS NOT NULL AND date <> '' AND ${NOT_EXCLUDED}
    GROUP BY brand, sku_id, substr(date, 1, 7)`);
}

async function rebuildFirstSeen(db) {
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

async function rebuildAll(db) {
  await rebuildFirstSeen(db);
  await rebuildBrandDaily(db);
  await rebuildSkuMonth(db);
}

/** 제외 목록이 바뀌면 제외 반영 집계를 통째로 다시 만든다. 드물게 일어나는 작업. */
async function onExcludeChanged(db) {
  await rebuildBrandDaily(db);
  await rebuildSkuMonth(db);
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

/** 업로드 직후 세 테이블을 한 번에 맞춘다. */
async function refreshAfterUpload(db, rows) {
  await touchFirstSeen(db, rows);
  await refreshBrandDaily(db, distinctBrandDates(rows));
  await refreshSkuMonth(db, distinctBrandMonths(rows));
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
