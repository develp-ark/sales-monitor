# Sales Monitor — 프로젝트 문서

## 1. 프로젝트 개요

쿠팡 셀러를 위한 **판매 데이터 모니터링 대시보드**.
CSV 업로드 → DB 저장 → 대시보드 시각화 → Google Sheets 연동까지 하나의 웹앱에서 처리.

- **배포**: Vercel (Hobby 플랜, 함수 12개 제한, 60초 타임아웃)
- **URL**: https://sales-monitor-navy.vercel.app/
- **DB**: Turso (LibSQL, SQLite 호환) — 무료 플랜 (500M rows read 제한)
- **Google Sheet**: https://docs.google.com/spreadsheets/d/1XCIdrZuHfwoPEqF6u0bVPn4fX32YCOXCKmGUMFz4dSw/edit
- **저장소**: https://github.com/jlee7ww/sales-monitor

---

## 2. 디렉토리 구조

```
sales-monitor/
├── api/
│   └── [...slug].js          # 단일 서버리스 진입점 (catch-all 라우트)
├── lib/
│   ├── db.js                  # Turso DB 연결
│   ├── schema.js              # sku_manage DDL 단일 정의 + ensureSkuManage()
│   └── handlers/
│       ├── dashboard.js       # 대시보드 데이터 API (캐시 5분)
│       ├── insight.js         # 브랜드 인사이트 API (기간별 분석)
│       ├── upload.js          # 판매 CSV 업로드 + 시트 동기화
│       ├── upload-sku-manage.js  # SKU 관리 CSV 업로드
│       ├── upload-monthly.js  # 월별 데이터 업로드
│       ├── flag.js            # 플래그 저장/삭제 API
│       ├── exclude.js         # 제외 SKU 관리 API
│       ├── export.js          # Excel 내보내기 API
│       ├── init.js            # DB 초기화
│       ├── new-product-manual.js  # 수동 신규상품 관리
│       ├── sku-manage.js      # SKU 관리 조회 API
│       ├── test-sheets.js     # 시트 연동 테스트
│       ├── test.js            # 일반 테스트
│       └── update-price.js    # 가격 업데이트
├── public/
│   └── index.html             # 프론트엔드 SPA (전체 UI, 약 3,100줄)
├── package.json
└── vercel.json
```

---

## 3. 아키텍처

### 3.1 API 라우팅

Vercel Hobby 플랜은 서버리스 함수 12개 제한이 있어, **`api/[...slug].js`** 하나로 모든 요청을 받아 `lib/handlers/`의 핸들러로 디스패치합니다.

```
/api/dashboard    → lib/handlers/dashboard.js
/api/upload       → lib/handlers/upload.js
/api/insight      → lib/handlers/insight.js
/api/flag         → lib/handlers/flag.js
... (총 14개 핸들러)
```

### 3.2 데이터 흐름

```
[CSV 파일] → 프론트엔드 detectCsvType() → 적절한 API로 라우팅
  ├── sales CSV      → /api/upload         → sales 테이블
  ├── sku-manage CSV → /api/upload-sku-manage → sku_manage 테이블
  ├── monthly CSV    → /api/upload-monthly  → sales_monthly 테이블
  └── sku-remarks CSV → /api/exclude + /api/new-product-manual

[DB] → /api/dashboard (캐시 5분) → 프론트엔드 renderDash/renderTrend
[DB] → /api/insight (기간별)     → 프론트엔드 renderInsight
```

### 3.3 캐시 전략

- **dashboard.js**: 모듈 레벨 `_cache`, `_cacheTime` (TTL 5분)
- **purge**: `?purge=1` 쿼리 파라미터로 캐시 무효화
- 업로드 완료 후 프론트엔드에서 `loadAll(true)` → `/api/dashboard?purge=1` 호출

---

## 4. DB 스키마

`lib/schema.js`가 `sku_manage`의 **단일 정의**입니다. 다른 핸들러는 `ensureSkuManage(db)`를 호출할 뿐, 자체 `CREATE TABLE`을 갖지 않습니다.
나머지 테이블은 `lib/handlers/init.js`의 `STATEMENTS`에 정의돼 있습니다.

### 4.1 sales 테이블 (일별 판매 데이터)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | INTEGER PK | AUTOINCREMENT |
| date | TEXT | 날짜 (YYYY-MM-DD) |
| brand | TEXT | 브랜드명 (건우코리아, 아리코, 윰) |
| sku_id | TEXT | SKU ID |
| sku_name | TEXT | 상품명 |
| sales | INTEGER | 판매량 |
| stock | INTEGER | 재고 |
| status | TEXT | 판매상태 |
| revenue | INTEGER | 매출액 |
| oos_flag | TEXT | 품절 플래그 (Y/N) |

제약: `UNIQUE(date, sku_id)`. 인덱스: `date`, `(brand, date)`, `sku_id`.

### 4.2 sku_manage 테이블 (SKU 관리)

| 컬럼 | 타입 | 설명 |
|------|------|------|
| id | INTEGER PK | AUTOINCREMENT |
| brand | TEXT | 브랜드 |
| sku_id | TEXT UNIQUE | SKU ID — **PK가 아니라 UNIQUE**. `ON CONFLICT(sku_id)`가 이에 의존 |
| sku_name | TEXT | 상품명 |
| watch | INTEGER | 기본 1. `flag.js`의 INSERT가 사용 |
| flag | TEXT | 플래그 (자유 텍스트: 중단, 모니터링, TOP 등) |
| memo | TEXT | 메모 |
| pid | TEXT | 쿠팡 상품 ID |
| iid | TEXT | 아이템 ID |
| vid | TEXT | 벤더아이템 ID |
| product_url | TEXT | 쿠팡 상품 URL |
| active | INTEGER | 활성 여부 (1=활성). 기본 1, 삭제는 `active=0` 소프트 삭제 |
| created_at / updated_at | TEXT | 생성일 / 수정일 |
| base_price | INTEGER | 등록가 |
| current_price | INTEGER | 현재가 |
| price_checked_at | TEXT | 가격 확인 시각 |
| collect_cycle | INTEGER | 수집 주기 (일), 기본 7 |
| last_collected | TEXT | 최종 수집 시각 |

### 4.3 기타 테이블

- **sku_exclude**: 제외 SKU 목록 (sku_id PK, sku_name, brand, excluded_at)
- **sku_new_manual**: 수동 등록 신규상품 (sku_id PK, sku_name, brand, added_at)
- **sales_monthly**: 월별 판매 데이터 (brand, sku_id, sku_name, month, sales) — `UNIQUE(brand, sku_id, month)`

---

## 5. 프론트엔드 구조 (public/index.html)

### 5.1 탭 구조

| 탭 | 기능 |
|----|------|
| **대시보드** | 브랜드별 요약 카드 + 일별 판매 추세 테이블 |
| **브랜드 인사이트** | 브랜드별 상세 분석: TOP 판매, 플래그 상품 현황, 급상승/급하락, 품절, 신규상품 |
| **SKU 관리** | 플래그 관리, 신규상품 수동 등록, 제외 SKU 관리 |

`/api/dashboard`는 **최근 30일치만** 반환하므로, 추세 테이블은 그 30일을 주 단위 블록(`이번 주`, `1주 전`…)으로 나눠 전부 **일별로** 표시합니다.
`renderTrend()` 안의 "이전 주간 요약" 블록은 30일보다 오래된 날짜를 대상으로 하는데, API가 그런 날짜를 주지 않으므로 **현재 도달하지 않는 죽은 코드**입니다.

### 5.2 CSV 업로드 자동 분류 (`detectCsvType`)

프론트엔드에서 CSV 헤더를 분석하여 위에서부터 순서대로 판정:

| 순서 | 분류 | 조건 | 라우트 |
|---|------|------|--------|
| 1 | monthly | 헤더에 `YY.MM` 형식 컬럼 | /api/upload-monthly |
| 2 | **sku-manage** | 헤더에 `플래그` 또는 `flag` 포함 | /api/upload-sku-manage |
| 3 | sku-remarks | `브랜드` + `비고`(또는 메모) + `SKU ID` 세 헤더가 모두 존재 | /api/exclude + /api/new-product-manual |
| 4 | deprecated-new-csv | 헤더에 `신규` 포함 | **업로드하지 않고 건너뜀** (안내 메시지 표시) |
| 5 | exclude | 4컬럼 이하 + `sku_id`/`sku id`/`sku 명` | /api/exclude |
| 6 | sku-manage (폴백) | 헤더에 `pid`/`iid`/`vid`/`비고` 포함 | /api/upload-sku-manage |
| 7 | sales | 헤더에 `날짜` 또는 `date` 존재 | /api/upload |
| 8 | sku-manage (폴백) | 날짜는 없고 `SKU ID`만 있음 | /api/upload-sku-manage |
| 9 | sales | 기본값 | /api/upload |

**판매 CSV는 반드시 날짜 컬럼을 가집니다.** `upload.js`의 `rowFromRecord()`가 날짜 없는 행을 전부 버리므로, 날짜 없는 파일이 `sales`로 가면 0건 삽입 후 조용히 성공합니다. 7·8번 규칙이 이를 막습니다.

**주의**: 2번(`플래그`/`flag`) 체크는 3번(`sku-remarks`)보다 반드시 앞에 있어야 합니다. 그렇지 않으면 `비고`+`브랜드`+`SKU ID`가 모두 있는 SKU관리 CSV가 `sku-remarks`로 잘못 분류되어 데이터가 저장되지 않습니다.

### 5.3 주요 전역 변수

```javascript
var D = null;              // dashboard API 응답 전체 데이터
var CTRL = {               // 인사이트 컨트롤 설정
  threshold: 30,           // 급상승/급하락 기준 (%)
  minSales: 5,             // 최소 판매량 필터
  period: 7,               // 기본 집계 기간 (일)
  topCount: 10             // TOP N 표시 개수
};
var BRAND_ORDER = ['아리코','윰','건우코리아'];  // 브랜드 표시 순서
window._insightData = null;   // /api/insight 응답 캐시
window._excludeList = [];     // 제외 SKU 목록
window._newManualList = [];   // 수동 신규상품 목록
```

### 5.4 주요 함수

| 함수 | 역할 |
|------|------|
| `loadAll(purge)` | dashboard API 호출 → renderDash + renderTrend + renderInsight |
| `handleFiles(files)` | CSV 파일 분류 → 적절한 API로 업로드 → 시트 동기화 → loadAll |
| `renderDash()` | 브랜드 카드 렌더링 |
| `renderTrend()` | 일별 판매 추세 테이블 |
| `renderInsight()` | 브랜드 탭 + 컨트롤 패널 생성 → fetchInsight 호출 |
| `fetchInsight()` | /api/insight 호출 → recomputeCustom |
| `recomputeCustom(brand)` | 인사이트 데이터로 TOP/급상승/급하락/품절/플래그 박스 렌더링 |
| `buildFlagInsight(brand, allSkuSales, bi)` | 플래그 상품 현황 테이블 (집계/비교기간 대비) |
| `renderSkuManage()` | SKU 관리 탭 전체 렌더링 |
| `renderFlagManageTable()` | 플래그 관리 테이블 |
| `exportXlsx()` | ExcelJS로 브랜드별 시트 + Daily Trend 시트 생성 |

상품 URL은 `getSkuUrl(skuManageMap[sku_id])`로 `sku_manage.product_url`에서 얻습니다.

---

## 6. API 상세

### 6.1 GET /api/dashboard

**응답 구조:**
```json
{
  "brands": {
    "건우코리아": {
      "todaySales": 717, "sum7": 5134, "dailyAvg": 733.4,
      "skuCount": 291, "stockSum": 21669, "outOfStockCount": 27
    }
  },
  "dailyTrend": { "건우코리아": { "2026-05-17": 717 } },
  "dates": ["2026-04-18"],
  "latestDate": "2026-05-17",
  "flags": { "69118150": { "sku_name": "...", "brand": "건우코리아", "flag": "", "memo": "" } },
  "skuManageMap": { "69118150": { } },
  "sheetGids": { "건우코리아": 981721695 },
  "brandInsights": { "건우코리아": { "7": { "topSales": [], "oos": [], "surgeUp": [], "surgeDown": [] } } }
}
```

- `?purge=1`: 캐시 무효화
- `dates`는 `latestDate` 기준 **최근 30일** (`latestDate - 29` ~ `latestDate`)
- `skuManageMap`은 `sku_manage WHERE active=1` 전체 데이터
- `flags`는 `skuManageMap`에서 sku_id → {sku_name, brand, flag, memo} 추출
- `brandInsights`는 **빈 껍데기**입니다. `/api/insight` 응답이 도착하기 전 첫 렌더에서 프론트가 `undefined`를 참조하지 않도록 하는 플레이스홀더이며, 실제 값은 `/api/insight`가 채웁니다. 키가 `'7'`로 하드코딩돼 있어 `CTRL.period`가 7이 아니면 프론트는 리터럴 기본값으로 폴백합니다.

### 6.2 GET /api/insight

**파라미터**: `curStart`, `curEnd`, `prevStart`, `prevEnd`

**응답 구조:**
```json
{
  "ok": true,
  "brandInsights": {
    "건우코리아": {
      "topSales": [{ "sku_id": "...", "sales": 100, "prev": 80, "rate": 25, "curAvg": 14.3 }],
      "surgeUp": [],
      "surgeDown": [],
      "oos": [],
      "newItems": []
    }
  }
}
```

### 6.3 POST /api/upload

판매 CSV 업로드. `?syncOnly=1`로 시트 동기화만 실행 가능.

**syncOnly 요청 body:**
```json
{ "brand": "건우코리아", "type": "brand" }
{ "type": "trend" }
```

### 6.4 POST /api/upload-sku-manage

SKU 관리 CSV 업로드. CSV 헤더 매핑:

| CSV 헤더 | DB 컬럼 |
|----------|---------|
| 브랜드 / brand | brand |
| SKU ID / sku_id | sku_id |
| SKU 명 / sku_name / 상품명 | sku_name |
| 비고 / 메모 / memo | memo |
| 플래그 / flag / Flag | flag |
| pid / PID / 상품ID | pid |
| url / URL / 상품URL | product_url |

헤더는 `normKey()`로 정규화해 매칭합니다(대소문자, 앞뒤/연속 공백, BOM, 비단절 공백 무시). 컬럼명 자체가 다르면(`상품번호` 등) 저장하지 않고 응답에 `warning`과 인식된 `headers`를 실어 보냅니다.

UPSERT 방식(`ON CONFLICT(sku_id)`): 기존 SKU는 비어있지 않은 값만 업데이트. 200건씩 `db.batch()`로 나눠 실행합니다(행마다 왕복하면 수백 건에서 60초 제한에 걸림).
`product_url`이 있고 pid/iid/vid가 비어 있으면 URL에서 정규식으로 추출합니다.

### 6.5 POST /api/flag

**요청:**
```json
{ "skuId": "12345", "skuName": "상품명", "flag": "중단", "memo": "메모" }
```
flag를 빈 문자열로 보내면 플래그 제거.

### 6.6 기타 API

- `GET/POST/DELETE /api/exclude` — 제외 SKU CRUD
- `GET/POST/DELETE /api/new-product-manual` — 수동 신규상품 CRUD
- `GET /api/export` — Excel 내보내기용 전체 데이터
- `POST /api/upload-monthly` — 월별 판매 데이터 업로드
- `GET /api/test-sheets` — 시트 연동 점검 (`ok`, `sheetNames`, `debug` 반환)

---

## 7. Google Sheets 연동

- 시트 탭: 브랜드별 (건우코리아, 아리코, 윰), `daily_trend`, `로켓그로스`
- **`로켓그로스`는 사람이 직접 관리하는 탭으로, 코드가 읽거나 쓰지 않습니다.**
- 브랜드 탭: A열 "제외" 컬럼 (Y = 해당 SKU 합계에서 제외)
- `fixedCols = 5` (제외, SKU ID, 상품명, 재고, 상태)
- 동기화 방향: **DB → Sheet** (단방향)
- 동기화 타이밍: CSV 업로드 완료 후 프론트엔드에서 순차 호출

### 7.1 인증 주의

`googleapis` v171에서 `new google.auth.JWT(email, null, key, scopes)` **positional 형태는 인증이 붙지 않습니다** (`Method doesn't allow unregistered callers`). 반드시 객체 형태를 사용하세요.

```javascript
new google.auth.JWT({ email, key, scopes: [...] })
```

`GOOGLE_PRIVATE_KEY`가 `\n` 리터럴로 저장돼 있으면 실제 개행으로 변환해야 합니다.

---

## 8. 브랜드 정보

| 브랜드 | 테마 색상 | 비고 |
|--------|----------|------|
| 건우코리아 | #F04452 (레드) | GUNU 브랜드 |
| 아리코 | #3182F6 (블루) | 아리코 브랜드 |
| 윰 | #00B493 (그린) | 윰스 브랜드 |

`BRAND_ORDER`의 표시 순서는 아리코 → 윰 → 건우코리아입니다.

---

## 9. 알려진 제약사항 및 주의점

1. **Vercel 60초 타임아웃**: 시트 동기화를 별도 API 호출로 분리 (`syncOnly`). 대량 데이터 업로드 시 800행 단위로 청크 분할.

2. **Turso 무료 플랜**: 500M rows read 제한. 캐시 TTL을 적절히 유지해야 함 (현재 5분).

3. **CSV 타입 분류 우선순위**: `플래그`/`flag` 헤더 체크를 `sku-remarks` 체크보다 반드시 앞에 배치. 그렇지 않으면 `비고` 컬럼이 있는 SKU관리 CSV가 `sku-remarks`로 잘못 분류되어 데이터가 저장되지 않음.

4. **캐시 무효화**: 업로드 후 반드시 `?purge=1`로 호출해야 최신 데이터 표시. `loadAll(true)` 또는 새로고침 버튼이 이를 처리.

5. **플래그 값**: 자유 텍스트. 현재 사용 중인 값: `1개 번들 중단`, `5000원▼ 모니터링` 등. `flagStyle` 객체에 정의된 스타일만 색상 적용, 미정의 플래그는 기본 회색.

6. **프론트엔드 SPA**: `public/index.html` 단일 파일에 HTML/CSS/JS 전부 포함 (약 3,100줄). 프레임워크 미사용, 바닐라 JS.

7. **sku_manage 스키마는 `lib/schema.js`에서만 정의**합니다. 과거 세 핸들러가 각각 다른 `CREATE TABLE`을 갖고 있어, 어느 API가 먼저 호출되는지에 따라 컬럼 구성이 달라지는 문제가 있었습니다. 컬럼을 추가할 때는 `SKU_MANAGE_DDL`과 `SKU_MANAGE_ALTERS`를 함께 수정하세요 (기존 배포본 호환).

8. **`dashboard.js`의 `sku_manage` 조회가 실패하면** `flags`와 `skuManageMap`이 조용히 빈 객체가 되어, 에러 없이 화면에서 플래그만 사라집니다. 이 경우 함수 로그의 `[DASH] sku_manage 조회 실패`를 확인하세요.

---

## 10. 환경변수

| 변수 | 용도 |
|------|------|
| TURSO_URL | Turso DB URL |
| TURSO_TOKEN | Turso 인증 토큰 |
| GOOGLE_CLIENT_EMAIL | Google Sheets API 서비스 계정 이메일 |
| GOOGLE_PRIVATE_KEY | Google Sheets API 비공개 키 |

---

## 11. 최근 변경 이력

- SKU 관리 CSV 업로드가 조용히 0건 처리되던 문제 수정 (헤더 정규화 + 실패를 화면에 노출)
- 날짜 없는 CSV가 판매 CSV로 오분류되지 않도록 `detectCsvType` 규칙 추가
- `sku_manage` 스키마를 `lib/schema.js`로 일원화 (부트스트랩 순서 의존 제거)
- `test-sheets.js`의 JWT positional 인자 버그 수정
- 존재하지 않던 `sku_url_map` 조회 및 응답의 `skuUrls` 제거
- 미사용 `lib/sheets.js`, `dashboard` 응답의 `insights` 필드 제거
- 캐시 purge 기능 추가 (`?purge=1`)
- CSV 타입 분류에서 `플래그` 헤더 우선 인식 (sku-remarks 오분류 수정)
- 플래그 상품 현황: 집계기간/비교기간 대비 변동률 표시
- 브랜드 인사이트: 커스텀 기간 선택 UI (집계기간 + 비교기간)
- 신규상품 감지 (최근 3일 내 첫 등장)
- Excel 내보내기 (ExcelJS + FileSaver.js)
