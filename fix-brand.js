const { getDb } = require('../lib/db');

module.exports = async function handler(req, res) {
  if (req.query.key !== 'fix2026') return res.status(403).json({ error: 'forbidden' });
  
  const db = getDb();
  
  // 아리코 계열 브랜드를 모두 '아리코'로 통합
  const wrongBrands = [
    '브랜드없음', '닥터미니', '도그레미', '블루레이크', '3456샵', 
    'N_A', '스몰펫', 'TailUp', '딜', '팀버라인', '라이프스푼', 
    '아르코', '리브모어', '제이밀', '라이프스푼'
  ];
  
  let total = 0;
  for (const wb of wrongBrands) {
    const r = await db.execute({
      sql: "UPDATE sales SET brand = '아리코' WHERE brand = ?",
      args: [wb]
    });
    total += r.rowsAffected || 0;
  }
  
  res.json({ ok: true, updated: total });
};
