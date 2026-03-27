const { getDb } = require("../lib/db");

module.exports = async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(200).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  try {
    let b = req.body;
    if (typeof b === "string") b = JSON.parse(b);

    const db = getDb();
    const now = new Date().toISOString();

    let row = null;
    if (b.vid) {
      const r = await db.execute({
        sql: "SELECT * FROM sku_manage WHERE vid = ?",
        args: [b.vid],
      });
      if (r.rows.length) row = r.rows[0];
    }
    if (!row && b.pid) {
      const r = await db.execute({
        sql: "SELECT * FROM sku_manage WHERE pid = ?",
        args: [b.pid],
      });
      if (r.rows.length) row = r.rows[0];
    }
    if (!row && b.sku_id) {
      const r = await db.execute({
        sql: "SELECT * FROM sku_manage WHERE sku_id = ?",
        args: [b.sku_id],
      });
      if (r.rows.length) row = r.rows[0];
    }

    if (!row && b.url) {
      const r = await db.execute({
        sql: "SELECT * FROM sku_manage WHERE product_url LIKE ?",
        args: ["%" + b.pid + "%"],
      });
      if (r.rows.length) row = r.rows[0];
    }

    if (!row) {
      return res.json({
        ok: false,
        error: "SKU not found in DB",
        pid: b.pid,
        vid: b.vid,
      });
    }

    await db.execute({
      sql: "UPDATE sku_manage SET current_price=?, price_checked_at=?, updated_at=datetime('now') WHERE sku_id=?",
      args: [b.current_price, now, row.sku_id],
    });

    let diff = null;
    if (row.base_price && b.current_price) {
      const d = b.current_price - row.base_price;
      const pct = Math.round((d / row.base_price) * 100);
      if (d < 0) diff = "▼" + Math.abs(pct) + "%";
      else if (d > 0) diff = "▲" + pct + "%";
      else diff = "동일";
    }

    res.json({
      ok: true,
      sku_id: row.sku_id,
      sku_name: row.sku_name,
      base_price: row.base_price,
      current_price: b.current_price,
      diff: diff,
    });
  } catch (e) {
    console.error("[update-price error]", e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
};
