const { getDb } = require('../db');
const { loadBrandDaily, loadSkuMonthBefore, loadBrands } = require('../agg');

/**
 * xlsx 다운로드용 원본 데이터.
 *
 * Turso 는 "읽은 행 수"로 과금/차단하므로 훑는 행을 최소화해야 한다. 두 가지가 핵심이다.
 *
 * 1) ORDER BY / GROUP BY 를 SQL 에 붙이면 SQLite 가 정렬을 피하려고
 *    idx_sales_brand_sku_date 를 고르고, 그러면 date >= ? 가 범위 조건으로 쓰이지 않아
 *    테이블을 통째로 훑는다(75만 행). 정렬·집계는 JS 에서 하고 SQL 에는 범위 조건만 남겨
 *    idx_sales_date 범위 검색(20만 행)이 되게 한다.
 * 2) 같은 구간을 여러 번 조회하면 그 배수만큼 읽는다. sales 와 sales_monthly 를
 *    각각 한 번만 읽고 필요한 형태는 JS 에서 파생시킨다.
 *
 * 프런트가 쓰던 선택 규칙을 그대로 재현해야 결과물이 안 바뀐다.
 *  - sku_name : 창 안 "첫 행"   -> date 최소 행 (UNIQUE(date, sku_id) 이므로 유일)
 *  - 재고/상태: 창 안 "마지막 행" -> date 최대 행
 *  - 월별 이름: 병합 결과의 "첫 항목" -> month 최소 행 (UNIQUE(brand, sku_id, month))
 */

/** 구버전 클라이언트용 응답. 형식이 예전 그대로다. */
async function legacyExport(res, db, brand, exFilter, cutISO, cutMonth) {
  // ORDER BY 는 SQL 에서 빼고(전체 스캔 유발) JS 에서 정렬한다.
  let salesSql = `SELECT date, brand, sku_id, sku_name, sales, stock, status
    FROM sales WHERE ${exFilter} AND date >= ?`;
  const salesArgs = [cutISO];
  if (brand) { salesSql += ' AND brand = ?'; salesArgs.push(brand); }

  const [salesR, monthlyAgg, trendAgg, archiveR, brandList] = await Promise.all([
    db.execute({ sql: salesSql, args: salesArgs }),
    loadSkuMonthBefore(db, cutMonth),
    loadBrandDaily(db, '0000-01-01', '9999-12-31'),
    db.execute({ sql: `SELECT brand, sku_id, sku_name, month, sales FROM sales_monthly WHERE ${exFilter}`, args: [] }),
    loadBrands(db),
  ]);

  const rows = salesR.rows.slice().sort((a, b) =>
    (a.brand < b.brand ? -1 : a.brand > b.brand ? 1 : 0)
    || (a.sku_id < b.sku_id ? -1 : a.sku_id > b.sku_id ? 1 : 0)
    || (a.date < b.date ? -1 : a.date > b.date ? 1 : 0));

  const monthlyMap = {};
  archiveR.rows.forEach((r) => { monthlyMap[r.brand + '|' + r.sku_id + '|' + r.month] = r; });
  monthlyAgg.forEach((r) => {
    if (brand && r.brand !== brand) return;
    monthlyMap[r.brand + '|' + r.sku_id + '|' + r.month] = {
      brand: r.brand, sku_id: r.sku_id, sku_name: r.sku_name, month: r.month, sales: Number(r.sales) || 0,
    };
  });

  return res.status(200).json({
    ok: true,
    brands: brandList,
    count: rows.length,
    rows,
    dailyTrend: trendAgg.map((r) => ({ brand: r.brand, date: r.date, s: Number(r.sales) || 0 })),
    monthly: Object.values(monthlyMap),
  });
}

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return res.status(405).json({ error: 'Method Not Allowed' });
  }

  try {
    const db = getDb();
    await db.execute(`CREATE TABLE IF NOT EXISTS sku_exclude (
      sku_id TEXT PRIMARY KEY, sku_name TEXT, brand TEXT,
      excluded_at TEXT DEFAULT (datetime('now'))
    )`);
    await db.execute(`CREATE TABLE IF NOT EXISTS sales_monthly (
      brand TEXT NOT NULL, sku_id TEXT NOT NULL, sku_name TEXT,
      month TEXT NOT NULL, sales INTEGER DEFAULT 0,
      UNIQUE(brand, sku_id, month)
    )`);

    const brand = req.query?.brand || null;
    const exFilter = 'sku_id NOT IN (SELECT sku_id FROM sku_exclude)';

    const now = new Date();
    const y = now.getFullYear();
    const m = now.getMonth();
    var cutDate = new Date(y, m - 2, 1);
    var cutISO = cutDate.getFullYear() + '-' +
      String(cutDate.getMonth() + 1).padStart(2, '0') + '-01';
    const cutMonth = cutISO.slice(0, 7);

    // 배포 시점에 열려 있던 탭은 옛 JS 라 v2 형식을 해석하지 못한다.
    if (req.query?.v !== '2') {
      return legacyExport(res, db, brand, exFilter, cutISO, cutMonth);
    }

    let salesSql = `SELECT date, brand, sku_id, sku_name, sales, stock, status
      FROM sales WHERE ${exFilter} AND date >= ?`;
    const salesArgs = [cutISO];
    if (brand) { salesSql += ' AND brand = ?'; salesArgs.push(brand); }

    const [salesR, archiveR, monthlyAgg, trendAgg, brandList] = await Promise.all([
      db.execute({ sql: salesSql, args: salesArgs }),
      db.execute({ sql: `SELECT brand, sku_id, sku_name, month, sales FROM sales_monthly WHERE ${exFilter}`, args: [] }),
      loadSkuMonthBefore(db, cutMonth),
      loadBrandDaily(db, '0000-01-01', '9999-12-31'),
      loadBrands(db),
    ]);

    // ── 일별: 값이 있는 행만 남기고, 이름·재고·상태는 SKU 당 1건으로 뽑는다 ──
    const rows = [];
    const metaMap = new Map();
    for (const r of salesR.rows) {
      const k = r.brand + '|' + r.sku_id;
      const meta = metaMap.get(k);
      if (!meta) {
        metaMap.set(k, {
          brand: r.brand, sku_id: r.sku_id,
          _first: r.date, sku_name: r.sku_name,
          _last: r.date, stock: r.stock, status: r.status,
        });
      } else {
        if (r.date < meta._first) { meta._first = r.date; meta.sku_name = r.sku_name; }
        if (r.date > meta._last) { meta._last = r.date; meta.stock = r.stock; meta.status = r.status; }
      }
      if (Number(r.sales) !== 0) {
        rows.push({ brand: r.brand, sku_id: r.sku_id, date: r.date, sales: r.sales });
      }
    }
    const skuMeta = [...metaMap.values()].map((v) => ({
      brand: v.brand, sku_id: v.sku_id, sku_name: v.sku_name, stock: v.stock, status: v.status,
    }));

    // ── 월별: 아카이브 + agg 병합 (agg 가 같은 월을 덮어쓴다) ──
    const monthlyMap = new Map();
    const monthAxis = new Map();
    const nameMap = new Map();
    for (const r of archiveR.rows) {
      const key = r.brand + '|' + r.sku_id + '|' + r.month;
      if (Number(r.sales) !== 0) {
        monthlyMap.set(key, { brand: r.brand, sku_id: r.sku_id, month: r.month, sales: Number(r.sales) || 0 });
      }
      monthAxis.set(r.brand + '|' + r.month, { brand: r.brand, month: r.month });
      const nk = r.brand + '|' + r.sku_id;
      const cur = nameMap.get(nk);
      if (!cur || r.month < cur.month) nameMap.set(nk, { month: r.month, sku_name: r.sku_name });
    }
    for (const r of monthlyAgg) {
      const key = r.brand + '|' + r.sku_id + '|' + r.month;
      const v = Number(r.sales) || 0;
      if (v !== 0) monthlyMap.set(key, { brand: r.brand, sku_id: r.sku_id, month: r.month, sales: v });
      else monthlyMap.delete(key); // agg 의 0 은 아카이브 값을 0 으로 덮어쓴다
      monthAxis.set(r.brand + '|' + r.month, { brand: r.brand, month: r.month });
      const nk = r.brand + '|' + r.sku_id;
      const cur = nameMap.get(nk);
      if (!cur || r.month <= cur.month) nameMap.set(nk, { month: r.month, sku_name: r.sku_name });
    }

    const monthlyNames = [];
    for (const [k, v] of nameMap) {
      const p = k.split('|');
      monthlyNames.push({ brand: p[0], sku_id: p[1], sku_name: v.sku_name });
    }

    return res.status(200).json({
      ok: true,
      v: 2,
      brands: brandList,
      count: rows.length,
      rows,
      skuMeta,
      // 날짜 축은 agg_brand_daily 가 (brand, date) 단위라 그대로 쓸 수 있다.
      dates: trendAgg.filter((r) => r.date >= cutISO && (!brand || r.brand === brand))
        .map((r) => ({ brand: r.brand, date: r.date })),
      dailyTrend: trendAgg.map((r) => ({ brand: r.brand, date: r.date, s: Number(r.sales) || 0 })),
      monthly: [...monthlyMap.values()],
      monthlyNames,
      monthlyMonths: [...monthAxis.values()],
    });
  } catch (e) {
    console.error('[EXPORT]', e);
    return res.status(500).json({ error: e.message || 'export failed' });
  }
};
