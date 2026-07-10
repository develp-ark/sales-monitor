/**
 * DB 스키마 단일 정의.
 *
 * sku_manage는 과거에 init.js / upload-sku-manage.js / sku-manage.js 세 곳에서
 * 서로 다르게 CREATE 되어, 어느 핸들러가 먼저 호출되느냐에 따라 컬럼 구성이
 * 달라졌습니다. 아래 DDL은 현재 운영 DB의 실제 스키마와 동일합니다.
 */

const SKU_MANAGE_DDL = `CREATE TABLE IF NOT EXISTS sku_manage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  brand TEXT,
  sku_id TEXT UNIQUE,
  sku_name TEXT,
  watch INTEGER DEFAULT 1,
  flag TEXT DEFAULT '',
  memo TEXT DEFAULT '',
  pid TEXT,
  iid TEXT,
  vid TEXT,
  product_url TEXT,
  active INTEGER DEFAULT 1,
  created_at TEXT,
  updated_at TEXT,
  base_price INTEGER,
  current_price INTEGER,
  price_checked_at TEXT,
  collect_cycle INTEGER DEFAULT 7,
  last_collected TEXT
)`;

/** 운영 DB는 init.js의 구버전 7컬럼 테이블에서 ALTER로 증축되었으므로, 기존 배포본과의 호환을 위해 유지합니다. */
const SKU_MANAGE_ALTERS = [
  'ALTER TABLE sku_manage ADD COLUMN pid TEXT',
  'ALTER TABLE sku_manage ADD COLUMN iid TEXT',
  'ALTER TABLE sku_manage ADD COLUMN vid TEXT',
  'ALTER TABLE sku_manage ADD COLUMN product_url TEXT',
  'ALTER TABLE sku_manage ADD COLUMN active INTEGER DEFAULT 1',
  'ALTER TABLE sku_manage ADD COLUMN created_at TEXT',
  'ALTER TABLE sku_manage ADD COLUMN updated_at TEXT',
  'ALTER TABLE sku_manage ADD COLUMN memo TEXT',
  'ALTER TABLE sku_manage ADD COLUMN base_price INTEGER',
  'ALTER TABLE sku_manage ADD COLUMN current_price INTEGER',
  'ALTER TABLE sku_manage ADD COLUMN price_checked_at TEXT',
  'ALTER TABLE sku_manage ADD COLUMN collect_cycle INTEGER DEFAULT 7',
  'ALTER TABLE sku_manage ADD COLUMN last_collected TEXT',
];

async function ensureSkuManage(db) {
  await db.execute(SKU_MANAGE_DDL);
  for (const sql of SKU_MANAGE_ALTERS) {
    try { await db.execute(sql); } catch (e) { /* 이미 존재 */ }
  }
}

module.exports = { SKU_MANAGE_DDL, SKU_MANAGE_ALTERS, ensureSkuManage };
