const { getDb } = require('../lib/db');

module.exports = async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'POST only' });

  try {
    let body = req.body;
    if (typeof body === 'string') body = JSON.parse(body);
    const skuId = body.sku_id;
    if (!skuId) return res.status(400).json({ error: 'sku_id required' });

    const db = getDb();
    const row = await db.execute({ sql: 'SELECT * FROM sku_manage WHERE sku_id = ?', args: [skuId] });
    if (!row.rows.length) return res.status(404).json({ error: 'SKU not found' });

    const sku = row.rows[0];

    const today = new Date().toISOString().split('T')[0];
    if (sku.price_checked_at && sku.price_checked_at.startsWith(today)) {
      return res.json({
        ok: true, cached: true,
        sku_id: skuId,
        base_price: sku.base_price,
        current_price: sku.current_price,
        checked_at: sku.price_checked_at
      });
    }

    let url = sku.product_url;
    if (!url && sku.pid) {
      url = 'https://www.coupang.com/vp/products/' + sku.pid;
      if (sku.iid) url += '?itemId=' + sku.iid;
      if (sku.vid) url += '&vendorItemId=' + sku.vid;
    }
    if (!url) return res.status(400).json({ error: 'No URL available for this SKU' });

    const response = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      }
    });
    const html = await response.text();

    let currentPrice = null;
    let originalPrice = null;
    let discount = null;
    let rocketDelivery = false;

    const saleMatch = html.match(/class="total-price"[^>]*>.*?<strong[^>]*>([\d,]+)/s)
      || html.match(/"salePrice":([\d]+)/)
      || html.match(/total-price[^>]*>([\d,]+)/);
    if (saleMatch) currentPrice = parseInt(saleMatch[1].replace(/,/g, ''));

    const origMatch = html.match(/class="origin-price"[^>]*>.*?([\d,]+)/s)
      || html.match(/"originalPrice":([\d]+)/);
    if (origMatch) originalPrice = parseInt(origMatch[1].replace(/,/g, ''));

    const discMatch = html.match(/class="discount-rate"[^>]*>.*?(\d+)%/s)
      || html.match(/"discountRate":(\d+)/);
    if (discMatch) discount = parseInt(discMatch[1]);

    rocketDelivery = html.includes('rocket') || html.includes('로켓배송');

    if (currentPrice === null) {
      const priceMatch = html.match(/([\d,]{3,})원/);
      if (priceMatch) currentPrice = parseInt(priceMatch[1].replace(/,/g, ''));
    }

    if (currentPrice === null) {
      return res.json({ ok: false, error: 'Could not parse price', sku_id: skuId });
    }

    const now = new Date().toISOString();
    await db.execute({
      sql: "UPDATE sku_manage SET current_price=?, price_checked_at=?, updated_at=datetime('now') WHERE sku_id=?",
      args: [currentPrice, now, skuId]
    });

    res.json({
      ok: true, cached: false,
      sku_id: skuId,
      base_price: sku.base_price,
      current_price: currentPrice,
      original_price: originalPrice,
      discount: discount,
      rocket: rocketDelivery,
      checked_at: now
    });

  } catch (e) {
    console.error('[check-price error]', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
};
