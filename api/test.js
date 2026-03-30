const { getDb } = require('../lib/db');

module.exports = async (req, res) => {
  try {
    const db = getDb();
    const count = await db.execute('SELECT COUNT(*) AS cnt FROM sales');
    const maxDate = await db.execute('SELECT MAX(date) AS d FROM sales');
    const minDate = await db.execute('SELECT MIN(date) AS d FROM sales');
    return res.status(200).json({
      totalRows: count.rows[0]?.cnt,
      minDate: minDate.rows[0]?.d,
      maxDate: maxDate.rows[0]?.d,
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
};
