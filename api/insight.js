const { getDb } = require('../lib/db');

function readQueryParam(req, key) {
  const q = req.query || {};
  if (q[key] !== undefined && q[key] !== null && String(q[key]).trim() !== '') {
    return String(q[key]).trim();
  }
  try {
    const u = new URL(req.url || '/', 'http://localhost');
    return (u.searchParams.get(key) || '').trim();
  } catch (e) {
    return '';
  }
}

function isIsoDate(s) {
  return typeof s === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

/** 연속 일자 (대시보드와 동일: 마지막 매출 발생 다음 날부터 품절 구간으로 간주) */
function dateRangeInclusive(start, end) {
  const out = [];
  let cur = start;
  while (cur <= end) {
    out.push(cur);
    const d = new Date(`${cur}T12:00:00Z`);
    d.setUTCDate(d.getUTCDate() + 1);
    cur = d.toISOString().slice(0, 10);
  }
  return out;
}

function findOosSalesStartIso(datesAsc, dateToSales) {
  const n = datesAsc.length;
  if (!n) return null;
  let j = n - 1;
  while (j >= 0) {
    const v = Number(dateToSales[datesAsc[j]]) || 0;
    if (v > 0) break;
    j--;
  }
  const zi = j + 1;
  if (zi >= n) return null;
  return datesAsc[zi];
}

module.exports = async (req, res) => {
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed' });

  try {
    const db = getDb();
    const curStart = readQueryParam(req, 'curStart');
    const curEnd = readQueryParam(req, 'curEnd');
    const prevStart = readQueryParam(req, 'prevStart');
    const prevEnd = readQueryParam(req, 'prevEnd');

    if (!curStart || !curEnd || !prevStart || !prevEnd) {
      return res.status(400).json({ error: 'curStart, curEnd, prevStart, prevEnd required' });
    }
    if (!isIsoDate(curStart) || !isIsoDate(curEnd) || !isIsoDate(prevStart) || !isIsoDate(prevEnd)) {
      return res.status(400).json({ error: 'dates must be YYYY-MM-DD' });
    }

    function addDaysStr(dateStr, days) {
      const d = new Date(dateStr + 'T12:00:00Z');
      d.setUTCDate(d.getUTCDate() + days);
      return d.toISOString().slice(0, 10);
    }
    function addMonthsStr(dateStr, months) {
      const d = new Date(dateStr + 'T12:00:00Z');
      const day = d.getUTCDate();
      d.setUTCDate(1);
      d.setUTCMonth(d.getUTCMonth() + months);
      const lastDay = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 0)).getUTCDate();
      d.setUTCDate(Math.min(day, lastDay));
      return d.toISOString().slice(0, 10);
    }
    function addYearsStr(dateStr, years) {
      const d = new Date(dateStr + 'T12:00:00Z');
      const month = d.getUTCMonth();
      const day = d.getUTCDate();
      d.setUTCDate(1);
      d.setUTCFullYear(d.getUTCFullYear() + years);
      const lastDay = new Date(Date.UTC(d.getUTCFullYear(), month + 1, 0)).getUTCDate();
      d.setUTCMonth(month);
      d.setUTCDate(Math.min(day, lastDay));
      return d.toISOString().slice(0, 10);
    }
    function daysBetween(s, e) {
      const t0 = new Date(s + 'T12:00:00Z').getTime();
      const t1 = new Date(e + 'T12:00:00Z').getTime();
      if (Number.isNaN(t0) || Number.isNaN(t1)) return 1;
      const d = Math.floor((t1 - t0) / 86400000) + 1;
      return d < 1 ? 1 : d;
    }

    const curDays = daysBetween(curStart, curEnd);
    const prevDays = daysBetween(prevStart, prevEnd);

    // 집계기간과 동일 길이/동일 축으로 비교
    const lastWeekStart = addDaysStr(curStart, -7);
    const lastWeekEnd = addDaysStr(curEnd, -7);
    const lastMonthStart = addMonthsStr(curStart, -1);
    const lastMonthEnd = addMonthsStr(curEnd, -1);
    const lastYearStart = addYearsStr(curStart, -1);
    const lastYearEnd = addYearsStr(curEnd, -1);

    const allDates = [curStart, curEnd, prevStart, prevEnd, lastYearStart, lastYearEnd, lastMonthStart, lastMonthEnd, lastWeekStart, lastWeekEnd];
    const minDate = allDates.sort()[0];
    const maxDate = allDates.sort()[allDates.length - 1];
    const minMonth = minDate.slice(0, 7);
    const maxMonth = maxDate.slice(0, 7);
    const lmDays = daysBetween(lastMonthStart, lastMonthEnd);
    const lwDays = daysBetween(lastWeekStart, lastWeekEnd);
    const lyDays = daysBetween(lastYearStart, lastYearEnd);

    const periods = [
      { key: 'cur', start: curStart, end: curEnd, field: 'cur' },
      { key: 'prev', start: prevStart, end: prevEnd, field: 'prev' },
      { key: 'lastWeek', start: lastWeekStart, end: lastWeekEnd, field: 'lastWeek' },
      { key: 'lastMonth', start: lastMonthStart, end: lastMonthEnd, field: 'lastMonth' },
      { key: 'lastYear', start: lastYearStart, end: lastYearEnd, field: 'lastYear' }
    ];

    function nextDateStr(dateStr) {
      return addDaysStr(dateStr, 1);
    }
    function monthBounds(monthStr) {
      const start = monthStr + '-01';
      const first = new Date(start + 'T12:00:00Z');
      const last = new Date(Date.UTC(first.getUTCFullYear(), first.getUTCMonth() + 1, 0));
      return { start, end: last.toISOString().slice(0, 10), days: last.getUTCDate() };
    }
    function overlapRange(s1, e1, s2, e2) {
      const s = s1 > s2 ? s1 : s2;
      const e = e1 < e2 ? e1 : e2;
      if (s > e) return null;
      return { start: s, end: e };
    }

    await db.execute(`CREATE TABLE IF NOT EXISTS sales_monthly (
      brand TEXT NOT NULL,
      sku_id TEXT NOT NULL,
      sku_name TEXT,
      month TEXT NOT NULL,
      sales INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (brand, sku_id, month)
    )`);

    const [result, monthlyResult, firstDailyResult, firstMonthlyResult] = await Promise.all([
      db.execute({
        sql: `SELECT brand, sku_id, MAX(sku_name) AS sku_name, date, SUM(sales) AS s
          FROM sales WHERE date >= ? AND date <= ?
          GROUP BY brand, sku_id, date`,
        args: [minDate, maxDate]
      }),
      db.execute({
        sql: `SELECT brand, sku_id, MAX(sku_name) AS sku_name, month, SUM(sales) AS s
          FROM sales_monthly
          WHERE month >= ? AND month <= ?
          GROUP BY brand, sku_id, month`,
        args: [minMonth, maxMonth]
      }),
      db.execute({
        sql: `SELECT brand, sku_id, MIN(date) AS first_date
          FROM sales
          GROUP BY brand, sku_id`,
        args: []
      }),
      db.execute({
        sql: `SELECT brand, sku_id, MIN(month) AS first_month
          FROM sales_monthly
          GROUP BY brand, sku_id`,
        args: []
      })
    ]);

    const firstSeenMap = {};
    for (const row of firstDailyResult.rows) {
      const k = row.brand + '||' + row.sku_id;
      if (row.first_date) firstSeenMap[k] = row.first_date;
    }
    for (const row of firstMonthlyResult.rows) {
      const k = row.brand + '||' + row.sku_id;
      const d = row.first_month ? (row.first_month + '-01') : '';
      if (!d) continue;
      if (!firstSeenMap[k] || d < firstSeenMap[k]) firstSeenMap[k] = d;
    }

    function buildInsightItem(v, k) {
      const curAvg = curDays > 0 ? v.cur / curDays : 0;
      const prevAvg = prevDays > 0 ? v.prev / prevDays : 0;
      const lwAvg = lwDays > 0 ? v.lastWeek / lwDays : 0;
      const lmAvg = lmDays > 0 ? v.lastMonth / lmDays : 0;
      const lyAvg = lyDays > 0 ? v.lastYear / lyDays : 0;
      return {
        sku_id: v.sku_id, sku_name: v.sku_name,
        sales: v.cur, curAvg: Math.round(curAvg * 10) / 10,
        prev: v.prev, prevAvg: Math.round(prevAvg * 10) / 10,
        delta: v.cur - v.prev,
        vsWeek: lwAvg > 0 ? Math.round((curAvg - lwAvg) / lwAvg * 100) : (curAvg > 0 ? 999 : 0),
        vsMonth: lmAvg > 0 ? Math.round((curAvg - lmAvg) / lmAvg * 100) : (curAvg > 0 ? 999 : 0),
        vsYear: lyAvg > 0 ? Math.round((curAvg - lyAvg) / lyAvg * 100) : (curAvg > 0 ? 999 : 0),
        lwAvg: Math.round(lwAvg * 10) / 10,
        lmAvg: Math.round(lmAvg * 10) / 10,
        lyAvg: Math.round(lyAvg * 10) / 10,
        firstSeen: firstSeenMap[k] || ''
      };
    }
    function buildManualStub(m, k) {
      const fs = firstSeenMap[k] || (m.added_at ? String(m.added_at).slice(0, 10) : '');
      return {
        sku_id: String(m.sku_id),
        sku_name: m.sku_name || '',
        sales: 0, curAvg: 0,
        prev: 0, prevAvg: 0,
        delta: 0,
        vsWeek: 0, vsMonth: 0, vsYear: 0,
        lwAvg: 0, lmAvg: 0, lyAvg: 0,
        firstSeen: fs
      };
    }

    const skuMap = {};
    for (const row of result.rows) {
      const k = row.brand + '||' + row.sku_id;
      if (!skuMap[k]) skuMap[k] = {
        brand: row.brand, sku_id: String(row.sku_id), sku_name: row.sku_name || '',
        cur: 0, prev: 0, lastWeek: 0, lastMonth: 0, lastYear: 0,
        daysSeen: { cur: new Set(), prev: new Set(), lastWeek: new Set(), lastMonth: new Set(), lastYear: new Set() }
      };
      const d = row.date;
      let s = Number(row.s);
      if (Number.isNaN(s)) s = 0;
      for (const p of periods) {
        if (d >= p.start && d <= p.end) {
          skuMap[k][p.field] += s;
          skuMap[k].daysSeen[p.key].add(d);
        }
      }
    }

    // 월별 아카이브를 "일별이 비는 구간"에만 보완치로 반영
    for (const row of monthlyResult.rows) {
      const k = row.brand + '||' + row.sku_id;
      if (!skuMap[k]) skuMap[k] = {
        brand: row.brand, sku_id: String(row.sku_id), sku_name: row.sku_name || '',
        cur: 0, prev: 0, lastWeek: 0, lastMonth: 0, lastYear: 0,
        daysSeen: { cur: new Set(), prev: new Set(), lastWeek: new Set(), lastMonth: new Set(), lastYear: new Set() }
      };
      if (!skuMap[k].sku_name && row.sku_name) skuMap[k].sku_name = row.sku_name;

      let monthSales = Number(row.s);
      if (Number.isNaN(monthSales)) monthSales = 0;
      const mb = monthBounds(row.month);
      if (mb.days <= 0 || monthSales === 0) continue;
      const dayAvg = monthSales / mb.days;

      for (const p of periods) {
        const ov = overlapRange(p.start, p.end, mb.start, mb.end);
        if (!ov) continue;
        let missingDays = 0;
        for (let d = ov.start; d <= ov.end; d = nextDateStr(d)) {
          if (!skuMap[k].daysSeen[p.key].has(d)) missingDays++;
        }
        if (missingDays > 0) {
          skuMap[k][p.field] += dayAvg * missingDays;
        }
      }
    }

    let oosResult;
    try {
      oosResult = await db.execute({
        sql: 'SELECT brand, sku_id, sku_name, sales, stock, status, oos_flag FROM sales WHERE date = ?',
        args: [curEnd]
      });
    } catch (e) { oosResult = { rows: [] }; }

    const oosSet = {};
    for (const row of oosResult.rows) {
      const isOos = (row.oos_flag === 'Y') ||
        String(row.status || '').includes('품절') ||
        String(row.status || '').includes('일시품절');
      if (isOos) oosSet[row.brand + '||' + row.sku_id] = { stock: row.stock, status: row.status || '' };
    }

    const brandResults = {};
    for (const k in skuMap) {
      const v = skuMap[k];
      delete v.daysSeen;
      if (!brandResults[v.brand]) brandResults[v.brand] = { topSales: [], surgeUp: [], surgeDown: [], oos: [], newItems: [] };

      const item = buildInsightItem(v, k);

      brandResults[v.brand].topSales.push(item);

      if (v.prev > 0 || v.cur > 0) {
        const cA = item.curAvg;
        const pA = item.prevAvg;
        const rate = pA > 0 ? ((cA - pA) / pA) * 100 : (cA > 0 ? 999 : 0);
        const surgeItem = { ...item, rate: Math.round(rate) };
        if (rate > 0) brandResults[v.brand].surgeUp.push(surgeItem);
        else if (rate < 0) brandResults[v.brand].surgeDown.push(surgeItem);
      }
    }

    let manualNewResult = { rows: [] };
    try {
      await db.execute(`CREATE TABLE IF NOT EXISTS sku_new_manual (
        sku_id TEXT PRIMARY KEY,
        sku_name TEXT,
        brand TEXT,
        added_at TEXT DEFAULT (datetime('now'))
      )`);
      manualNewResult = await db.execute('SELECT sku_id, sku_name, brand, added_at FROM sku_new_manual');
    } catch (e) { manualNewResult = { rows: [] }; }

    const brandLookup = {};
    const missingSkuIds = [...new Set((manualNewResult.rows || []).filter((m) => !m.brand).map((m) => String(m.sku_id)))];
    if (missingSkuIds.length) {
      const chunkSize = 80;
      for (let i = 0; i < missingSkuIds.length; i += chunkSize) {
        const chunk = missingSkuIds.slice(i, i + chunkSize);
        const ph = chunk.map(() => '?').join(',');
        try {
          const br = await db.execute({
            sql: `SELECT sku_id, MIN(brand) AS brand FROM sales WHERE sku_id IN (${ph}) GROUP BY sku_id`,
            args: chunk
          });
          for (const row of br.rows) {
            brandLookup[String(row.sku_id)] = row.brand;
          }
        } catch (e) { /* ignore */ }
      }
    }

    for (const m of manualNewResult.rows || []) {
      const skuId = String(m.sku_id);
      const b = (m.brand && String(m.brand).trim()) || brandLookup[skuId];
      if (!b) continue;
      if (!brandResults[b]) {
        brandResults[b] = { topSales: [], surgeUp: [], surgeDown: [], oos: [], newItems: [] };
      }
      const key = b + '||' + skuId;
      const exists = brandResults[b].newItems.some((x) => String(x.sku_id) === skuId);
      if (exists) continue;
      const v = skuMap[key];
      const item = v ? buildInsightItem(v, key) : buildManualStub(m, key);
      brandResults[b].newItems.push(item);
    }

    const skuDailyByKey = {};
    for (const row of result.rows) {
      const rk = row.brand + '||' + row.sku_id;
      if (!skuDailyByKey[rk]) skuDailyByKey[rk] = {};
      let sv = Number(row.s);
      if (Number.isNaN(sv)) sv = 0;
      skuDailyByKey[rk][row.date] = sv;
    }
    const chronologicalDates = dateRangeInclusive(minDate, maxDate);

    for (const b in brandResults) {
      brandResults[b].topSales.sort((a, c) => c.sales - a.sales);
      brandResults[b].topSales = brandResults[b].topSales.slice(0, 100);
      brandResults[b].surgeUp.sort((a, c) => c.rate - a.rate);
      brandResults[b].surgeUp = brandResults[b].surgeUp.slice(0, 100);
      brandResults[b].surgeDown.sort((a, c) => a.rate - c.rate);
      brandResults[b].surgeDown = brandResults[b].surgeDown.slice(0, 100);
      brandResults[b].newItems.sort((a, c) => (c.sales || 0) - (a.sales || 0));
      brandResults[b].newItems = brandResults[b].newItems.slice(0, 100);

      const top30ids = {};
      brandResults[b].topSales.slice(0, 30).forEach((x) => { top30ids[x.sku_id] = true; });
      for (const k in oosSet) {
        const parts = k.split('||');
        if (parts[0] !== b) continue;
        if (!top30ids[parts[1]]) continue;
        const skuData = skuMap[k];
        if (!skuData) continue;
        const dm = skuDailyByKey[k] || {};
        const rawStart = findOosSalesStartIso(chronologicalDates, dm);
        const oosStartDate = rawStart || curEnd;
        brandResults[b].oos.push({
          sku_id: parts[1], sku_name: skuData.sku_name,
          stock: oosSet[k].stock, status: oosSet[k].status,
          sales: skuData.cur,
          oosStartDate,
          oos_start_iso: oosStartDate
        });
      }
    }

    return res.status(200).json({
      ok: true, brandInsights: brandResults,
      curStart, curEnd, prevStart, prevEnd,
      lastWeek: lastWeekStart + '~' + lastWeekEnd,
      lastMonth: lastMonthStart + '~' + lastMonthEnd,
      lastYear: lastYearStart + '~' + lastYearEnd,
      curDays, prevDays
    });
  } catch (e) {
    console.error('[INSIGHT]', e);
    return res.status(500).json({ error: e.message || 'insight failed' });
  }
};
