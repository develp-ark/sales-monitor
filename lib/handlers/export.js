const { getDb } = require('../db');
const { loadBrandDaily, loadSkuMonthBefore, loadBrands } = require('../agg');

/**
 * xlsx 다운로드용 원본 데이터.
 *
 * Turso 프라이머리가 도쿄, Vercel 함수가 버지니아라 이 구간이 대륙 간 왕복이다.
 * 응답 시간은 거기서 실어오는 데이터 양에 거의 정비례하므로, 같은 결과물을 만들되
 * 중복을 걷어낸 형태로 내려준다.
 *
 *  - 매출 0 행은 빼고, 그 때문에 사라지는 축(날짜/월)과 SKU 메타는 따로 내려준다.
 *  - sku_name 은 날짜마다 반복되던 것을 SKU 당 1건으로 분리한다 (기존 전송량의 44%).
 *
 * 프런트가 쓰던 선택 규칙을 그대로 재현해야 결과물이 안 바뀐다.
 *  - sku_name : 창 안 "첫 행" -> UNIQUE(date, sku_id) 이므로 MIN(date) 행이 유일
 *  - 재고/상태: 창 안 "마지막 행" -> MAX(date) 행
 *  - 월별 이름: 병합 결과의 "첫 항목" -> UNIQUE(brand, sku_id, month) 이므로 MIN(month) 행
 */
/** 구버전 클라이언트용 응답. 느리지만 형식이 예전 그대로다. */
async function legacyExport(res, db, brand, exFilter, cutISO, cutMonth) {
  let salesSql = `SELECT date, brand, sku_id, sku_name, sales, stock, status
    FROM sales WHERE ${exFilter} AND date >= ?`;
  const salesArgs = [cutISO];
  if (brand) { salesSql += ' AND brand = ?'; salesArgs.push(brand); }
  salesSql += ' ORDER BY brand, sku_id, date';

  const [salesR, monthlyAgg, trendAgg, archiveR, brandList] = await Promise.all([
    db.execute({ sql: salesSql, args: salesArgs }),
    loadSkuMonthBefore(db, cutMonth),
    loadBrandDaily(db, '0000-01-01', '9999-12-31'),
    db.execute({ sql: `SELECT brand, sku_id, sku_name, month, sales FROM sales_monthly WHERE ${exFilter}`, args: [] }),
    loadBrands(db),
  ]);

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
    count: salesR.rows.length,
    rows: salesR.rows,
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
    // v=2 를 붙여 보내는 클라이언트에게만 새 형식을 준다.
    if (req.query?.v !== '2') {
      return legacyExport(res, db, brand, exFilter, cutISO, cutMonth);
    }

    const brandCond = brand ? ' AND brand = ?' : '';
    const dailyArgs = brand ? [cutISO, brand] : [cutISO];

    const [salesR, nameR, stockR, archiveR, archiveNameR, archiveMonthR, monthlyAgg, trendAgg, brandList] =
      await Promise.all([
        // 값이 있는 일별 행만. 이름·재고·상태는 아래에서 SKU 당 1건으로 받는다.
        db.execute({
          sql: `SELECT brand, sku_id, date, sales FROM sales
                WHERE ${exFilter} AND date >= ?${brandCond} AND sales <> 0
                ORDER BY brand, sku_id, date`,
          args: dailyArgs,
        }),
        db.execute({
          sql: `SELECT brand, sku_id, MIN(date) AS d, sku_name FROM sales
                WHERE ${exFilter} AND date >= ?${brandCond}
                GROUP BY brand, sku_id`,
          args: dailyArgs,
        }),
        db.execute({
          sql: `SELECT brand, sku_id, MAX(date) AS d, stock, status FROM sales
                WHERE ${exFilter} AND date >= ?${brandCond}
                GROUP BY brand, sku_id`,
          args: dailyArgs,
        }),
        db.execute({
          sql: `SELECT brand, sku_id, month, sales FROM sales_monthly
                WHERE ${exFilter} AND sales <> 0`,
          args: [],
        }),
        db.execute({
          sql: `SELECT brand, sku_id, MIN(month) AS m, sku_name FROM sales_monthly
                WHERE ${exFilter} GROUP BY brand, sku_id`,
          args: [],
        }),
        db.execute({
          sql: `SELECT DISTINCT brand, month FROM sales_monthly WHERE ${exFilter}`,
          args: [],
        }),
        loadSkuMonthBefore(db, cutMonth),
        loadBrandDaily(db, '0000-01-01', '9999-12-31'),
        loadBrands(db),
      ]);

    // ── 일별 SKU 메타 (이름 = MIN(date) 행, 재고/상태 = MAX(date) 행) ──
    const metaMap = new Map();
    for (const r of nameR.rows) {
      metaMap.set(r.brand + '|' + r.sku_id,
        { brand: r.brand, sku_id: r.sku_id, sku_name: r.sku_name, stock: null, status: null });
    }
    for (const r of stockR.rows) {
      const hit = metaMap.get(r.brand + '|' + r.sku_id);
      if (hit) { hit.stock = r.stock; hit.status = r.status; }
    }

    // ── 월별: 아카이브 + agg 병합 (agg 가 같은 월을 덮어쓴다) ──
    const monthlyMap = new Map();
    const monthAxis = new Map();
    for (const r of archiveR.rows) {
      monthlyMap.set(r.brand + '|' + r.sku_id + '|' + r.month,
        { brand: r.brand, sku_id: r.sku_id, month: r.month, sales: Number(r.sales) || 0 });
    }
    for (const r of archiveMonthR.rows) monthAxis.set(r.brand + '|' + r.month, { brand: r.brand, month: r.month });

    // 이름은 "병합 결과의 최소 월" 기준. agg 가 그 월을 덮어썼다면 agg 이름이 이긴다.
    const nameMap = new Map();
    for (const r of archiveNameR.rows) {
      nameMap.set(r.brand + '|' + r.sku_id, { month: r.m, sku_name: r.sku_name });
    }
    for (const r of monthlyAgg) {
      const key = r.brand + '|' + r.sku_id + '|' + r.month;
      const v = Number(r.sales) || 0;
      if (v !== 0) monthlyMap.set(key, { brand: r.brand, sku_id: r.sku_id, month: r.month, sales: v });
      else monthlyMap.delete(key); // agg 의 0 은 아카이브 값을 덮어써서 0 으로 만든다
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
      count: salesR.rows.length,
      rows: salesR.rows,
      skuMeta: [...metaMap.values()],
      // 날짜 축은 agg_brand_daily 가 (brand, date) 단위라 그대로 쓸 수 있다 — 스캔 1회 절약.
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
