# scraper.py — Playwright scraper with fast selectors, pagination, VDP enrichment, year filtering
import re, json, asyncio, argparse, random, socket
from typing import List, Dict, Any, Optional
from urllib.parse import urlencode, urljoin, urlsplit, urlunsplit
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# -------------------- Config --------------------
DEFAULT_ZIP = "10001"
DEFAULT_RADIUS = 500
NAV_TIMEOUT = 30000         # ms for page.goto
SEL_TIMEOUT = 6000          # ms default for Playwright context selectors
TEXT_TIMEOUT = 700          # ms tiny timeout per field read (prevents "hang")
SCROLL_STEPS = 6

# -------------------- Utils --------------------
def _print_progress(msg: str):
    print(msg, flush=True)

def _dns_ok(host: str, timeout=3) -> bool:
    try:
        socket.setdefaulttimeout(timeout)
        socket.gethostbyname(host)
        return True
    except Exception:
        return False

def _to_int(s):
    if s is None: return None
    try: return int(str(s).replace(",", "").strip())
    except Exception: return None

_price_re = re.compile(r"\$[\s]*([\d,]+)")
_miles_re = re.compile(r"([\d,]+)\s*miles?", re.I)
_year_re  = re.compile(r"\b(19|20)\d{2}\b")

def parse_price(text: Optional[str]):
    if not text: return None
    m = _price_re.search(text.replace("\xa0", " "))
    return _to_int(m.group(1)) if m else None

def parse_miles(text: Optional[str]):
    if not text: return None
    m = _miles_re.search(text.replace("\xa0", " "))
    return _to_int(m.group(1)) if m else None

def parse_year_from_text(text: Optional[str]) -> Optional[int]:
    if not text: return None
    m = _year_re.search(text)
    return int(m.group(0)) if m else None

async def _scroll(page, steps=SCROLL_STEPS, delay=400):
    for _ in range(steps):
        await page.mouse.wheel(0, 1600)
        await page.wait_for_timeout(delay + int(random.random() * 150))

async def _first_text(node, selectors, timeout=TEXT_TIMEOUT) -> Optional[str]:
    """Return first available textContent among selectors without waiting long."""
    for sel in selectors:
        try:
            t = await node.locator(sel).first.text_content(timeout=timeout)
            if t:
                return t.strip()
        except Exception:
            pass
    return None

async def _first_attr(node, selectors, attr, timeout=TEXT_TIMEOUT) -> Optional[str]:
    for sel in selectors:
        try:
            v = await node.locator(sel).first.get_attribute(attr, timeout=timeout)
            if v:
                return v
        except Exception:
            pass
    return None

def _json_candidates_from_html(html: str) -> List[dict]:
    out=[]
    for m in re.finditer(r'__NEXT_DATA__\s*=\s*({.*?})\s*[,;]<', html, re.S):
        try: out.append(json.loads(m.group(1)))
        except Exception: pass
    for m in re.finditer(r'type=["\']application/(?:json|ld\+json)["\']>\s*(\{.*?\}|\[.*?\])\s*</script>', html, re.S):
        try: out.append(json.loads(m.group(1)))
        except Exception: pass
    return out

def _walk_find_listings(obj: Any) -> List[Dict[str, Any]]:
    KEYS={"price","listPrice","primaryPrice","mileage","miles","year","make","model","title","heading","vdpUrl","url","vin"}
    out=[]
    def looks(d): return len({k.lower() for k in d.keys()} & {k.lower() for k in KEYS})>=3
    def rec(x):
        if isinstance(x, dict):
            if looks(x): out.append(x)
            for v in x.values(): rec(v)
        elif isinstance(x, list):
            for v in x: rec(v)
    rec(obj); return out

def _abs(base: str, h: Optional[str]) -> Optional[str]:
    if not h: return None
    h = h.strip()
    if h.startswith("//"):  return "https:" + h
    if h.startswith(("http://","https://")): return h
    if h.startswith("www."): return "https://" + h
    if h.startswith("/"):   return urljoin(base, h)
    return urljoin(base, "/" + h)

def _normalize_url(u: Optional[str]) -> Optional[str]:
    if not u: return u
    parts = list(urlsplit(u)); parts[3]=parts[4]=""
    return urlunsplit(parts)

def _dedupe_and_trim(rows: List[Dict[str,Any]], k:int) -> List[Dict[str,Any]]:
    uniq, seen=[], set()
    for r in rows:
        u=_normalize_url(r.get("url"))
        if not u: continue
        if u in seen: continue
        seen.add(u); r["url"]=u; uniq.append(r)
        if len(uniq)>=k: break
    return uniq

def _enforce_year(rows: List[Dict[str,Any]], target_year: int) -> List[Dict[str,Any]]:
    filtered = [r for r in rows if r.get("year") == target_year]
    if not filtered:
        _print_progress(f"[FILTER] No exact year {target_year} results. Increase --radius or try another ZIP/year.")
    return filtered

# -------------------- Coercion --------------------
def _coerce_listing(d: Dict[str,Any], source: str, base: str) -> Dict[str,Any]:
    title = d.get("title") or d.get("heading")
    price = d.get("price") or d.get("listPrice") or d.get("primaryPrice")
    miles = d.get("miles") or d.get("mileage")
    url = d.get("url") or d.get("vdpUrl") or d.get("link")
    dealer = d.get("dealerName") or d.get("sellerName") or d.get("storeName")
    loc = d.get("location") or d.get("city")
    year_val = d.get("year")
    if isinstance(year_val, str): year_val = _to_int(year_val)
    if not year_val: year_val = parse_year_from_text(title)
    if isinstance(price,str): price = parse_price(price) or _to_int(price)
    if isinstance(miles,str): miles = parse_miles(miles) or _to_int(miles)
    if url and isinstance(url,str): url = _abs(base, url)
    return {"source":source,"title":title,"price":price if isinstance(price,int) else None,
            "miles":miles if isinstance(miles,int) else None,"year":year_val,
            "location":loc,"dealer":dealer,"url":url}

# -------------------- Site scrapers --------------------
async def _autotrader_deep_scroll(page, selector: str, max_rounds: int):
    last_n, stable = -1, 0
    while stable < max_rounds:
        await _scroll(page, steps=1, delay=500)
        try: n = await page.locator(selector).count()
        except Exception: n = 0
        if n == last_n: stable += 1
        else: stable, last_n = 0, n

async def scrape_autotrader(page, make, model, year, zip_code, radius, max_results,
                            debug=False, scroll_rounds=12):
    make_code = make.upper().replace(" ", "")
    model_code = model.upper().replace(" ", "_")
    params = {
        "searchRadius": radius, "zip": zip_code,
        "makeCodeList": make_code, "modelCodeList": f"{make_code}_{model_code}",
        "startYear": year, "endYear": year, "marketExtension":"include", "isNewSearch":"true",
    }
    url = "https://www.autotrader.com/cars-for-sale/all-cars?" + urlencode(params)
    _print_progress(f"[AT] Navigating: {url}")
    try: await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    except PWTimeout: _print_progress("[AT] Navigation timed out; continuing.")
    # infinite-scroll pagination
    cards_sel = "[data-cmp='inventoryListing'], div.inventory-listing"
    await _autotrader_deep_scroll(page, cards_sel, max_rounds=scroll_rounds)

    rows=[]
    try:
        cards = page.locator(cards_sel)
        n = await cards.count()
        _print_progress(f"[AT] Cards after deep scroll: {n}")
        for i in range(min(n, max_results*5)):
            c = cards.nth(i)
            title   = await _first_text(c, ["[data-cmp='subheading']", "[data-cmp='heading']"])
            link    = await _first_attr(c, ["a"], "href")
            link    = _abs("https://www.autotrader.com", link) if link else None
            price_t = await _first_text(c, ["[data-cmp='price']", ".first-price", ".price"])
            miles_t = await _first_text(c, ["[data-cmp='mileage']", ".mileage", ".item-card-specifications"])
            year_v  = parse_year_from_text(title)
            rows.append({"source":"Autotrader","title":title,
                         "price":parse_price(price_t or ""), "miles":parse_miles(miles_t or ""),
                         "year":year_v, "location":None,"dealer":None,"url":link})
            if (i+1) % 10 == 0: _print_progress(f"[AT] Processed {i+1}/{n} cards…")
            if len(rows)>=max_results: break
    except Exception as e:
        _print_progress(f"[AT] DOM scrape error: {e}")

    if len(rows) < max_results:
        _print_progress("[AT] Falling back to embedded JSON")
        html = await page.content()
        for obj in _json_candidates_from_html(html):
            for c in _walk_find_listings(obj):
                rows.append(_coerce_listing(c,"Autotrader","https://www.autotrader.com"))
                if len(rows)>=max_results: break
            if len(rows)>=max_results: break

    rows = _enforce_year(rows, year)
    rows = _dedupe_and_trim(rows, max_results)
    _print_progress(f"[AT] Returning {len(rows)} rows")
    return rows

async def scrape_cars(page, make, model, year, zip_code, radius, max_results,
                      debug=False, max_pages=5):
    make_q = make.lower().replace(" ", "")
    model_q = model.lower().replace(" ", "_")
    base = "https://www.cars.com"
    rows: List[Dict[str, Any]] = []
    seen_urls = set()

    for page_no in range(1, max_pages + 1):
        params = {
            "stock_type": "used",
            "makes[]": make_q,
            "models[]": f"{make_q}-{model_q}",
            "years[]": year,
            "year_min": year,
            "year_max": year,
            "maximum_distance": radius,
            "zip": zip_code,
            "page": page_no,
            "page_size": 20,
        }
        url = f"{base}/shopping/results/?" + urlencode(params, doseq=True)
        _print_progress(f"[CARS] Navigating page {page_no}: {url}")
        try: await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        except PWTimeout: _print_progress(f"[CARS] Nav timeout on page {page_no}; continuing.")
        await _scroll(page, 4)

        try:
            cards = page.locator("div.vehicle-card, article.vehicle-card")
            n = await cards.count()
            _print_progress(f"[CARS] Page {page_no} DOM cards: {n}")
            if n == 0 and page_no > 1: break
            for i in range(n):
                c = cards.nth(i)
                title   = await _first_text(c, ["h2.title", "h2.vehicle-card-title"])
                href    = await _first_attr(c, ["a.vehicle-card-link","a"], "href")
                href    = _abs(base, href) if href else None
                price_t = await _first_text(c, ["[data-test='vehicleCardPriceAmount']", ".primary-price"])
                miles_t = await _first_text(c, ["[data-test='vehicleMileage']", ".mileage", ".vehicle-mileage"])
                year_v  = parse_year_from_text(title)
                norm    = _normalize_url(href)
                if not norm or norm in seen_urls: continue
                seen_urls.add(norm)
                rows.append({"source":"Cars.com","title":title,
                             "price":parse_price(price_t or ""), "miles":parse_miles(miles_t or ""),
                             "year":year_v, "location":None,"dealer":None,"url":norm})
                if (i+1) % 10 == 0: _print_progress(f"[CARS] Page {page_no}: processed {i+1}/{n}")
                if len(rows)>=max_results: break
            if len(rows)>=max_results: break
        except Exception as e:
            _print_progress(f"[CARS] DOM scrape error on page {page_no}: {e}")

        if len(rows) == 0:
            _print_progress(f"[CARS] Page {page_no} falling back to embedded JSON")
            html = await page.content()
            for obj in _json_candidates_from_html(html):
                for c in _walk_find_listings(obj):
                    r = _coerce_listing(c, "Cars.com", base)
                    if not r.get("url"): continue
                    norm = _normalize_url(r["url"])
                    if norm in seen_urls: continue
                    seen_urls.add(norm); rows.append(r)
                    if len(rows)>=max_results: break
                if len(rows)>=max_results: break

    rows = _enforce_year(rows, year)
    rows = _dedupe_and_trim(rows, max_results)
    _print_progress(f"[CARS] Returning {len(rows)} rows")
    return rows

# -------------------- VDP enrichment --------------------
async def enrich_vdp(context, row: Dict[str, Any]) -> Dict[str, Any]:
    url=row.get("url"); 
    if not url: return row
    page = await context.new_page()
    try:
        _print_progress(f"[VDP] Visiting: {url}")
        try: await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
        except PWTimeout: _print_progress("[VDP] Navigation timeout; continuing.")
        await _scroll(page, steps=2, delay=250)
        html = await page.content()

        price = row.get("price"); miles = row.get("miles"); title = row.get("title"); year_v = row.get("year")

        if "cars.com" in url:
            sels_price = "[data-test='vdp-price'], .vehicle-info__price-display, .primary-price"
            sels_miles = "[data-test='mileage'], .mileage, .vehicle-mileage"
        elif "autotrader.com" in url:
            sels_price = "[data-cmp='stylePrice'], [data-cmp='firstPrice'], [data-cmp='price']"
            sels_miles = "[data-cmp='odometer'], [data-cmp='mileage']"
        else:
            sels_price = "h1, h2, .price, [class*='price']"
            sels_miles = ".mileage, [class*='mileage']"

        try:
            ptxt = await page.locator(sels_price).first.text_content(timeout=TEXT_TIMEOUT)
            price = price or parse_price(ptxt)
        except Exception: pass
        try:
            mtxt = await page.locator(sels_miles).first.text_content(timeout=TEXT_TIMEOUT)
            miles = miles or parse_miles(mtxt)
        except Exception: pass
        if not title:
            try: title = (await page.locator("h1, h2").first.text_content(timeout=TEXT_TIMEOUT)).strip()
            except Exception: pass
        if not year_v:
            year_v = parse_year_from_text(title) or parse_year_from_text(html)
        if not price or not miles:
            price = price or parse_price(html); miles = miles or parse_miles(html)

        row.update({"price":price,"miles":miles,"title":title,"year":year_v})
        return row
    finally:
        await page.close()

# -------------------- Orchestrator --------------------
async def query_listings_async(
    make: str, model: str, year: int, zip_code: str = DEFAULT_ZIP, radius_miles: int = DEFAULT_RADIUS,
    max_results_each: int = 8, headed: bool = False, enrich: bool = False,
    site: str = "both", debug: bool = False, max_pages: int = 5, scroll_rounds: int = 12
) -> List[Dict[str, Any]]:
    for host in ("www.autotrader.com","www.cars.com"):
        if not _dns_ok(host): _print_progress(f"[WARN] DNS lookup failed for {host}.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headed,
            args=["--disable-blink-features=AutomationControlled","--no-sandbox","--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
            viewport={"width":1280,"height":900}, locale="en-US"
        )
        context.set_default_timeout(SEL_TIMEOUT)
        context.set_default_navigation_timeout(NAV_TIMEOUT)
        await context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        if debug:
            context.on("request", lambda req: _print_progress(f"[REQ] {req.method} {req.url}"))
            context.on("requestfailed", lambda req: _print_progress(f"[REQ-FAIL] {req.url} {req.failure}"))
            context.on("response", lambda res: _print_progress(f"[RES] {res.status} {res.url}"))

        tasks=[]
        if site in ("both","autotrader"):
            page_at = await context.new_page()
            tasks.append(scrape_autotrader(page_at, make, model, year, zip_code, radius_miles,
                                           max_results_each, debug, scroll_rounds))
        if site in ("both","cars"):
            page_cars = await context.new_page()
            tasks.append(scrape_cars(page_cars, make, model, year, zip_code, radius_miles,
                                     max_results_each, debug, max_pages))

        _print_progress("[MAIN] Starting site tasks...")
        out=[]
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception): _print_progress(f"[MAIN] Task error: {r}")
                else: out.extend(r or [])
        finally:
            for pg in list(context.pages):
                try: await pg.close()
                except Exception: pass

        if enrich:
            _print_progress(f"[MAIN] Enriching {len(out)} rows via VDP...")
            enriched=[]
            for i, r in enumerate(out, 1):
                if r.get("price") and r.get("miles") and r.get("title") and r.get("year"):
                    enriched.append(r); continue
                try: enriched.append(await enrich_vdp(context, r))
                except Exception as e:
                    _print_progress(f"[VDP] Enrich error: {e}"); enriched.append(r)
                if i % 5 == 0: _print_progress(f"[MAIN] Enriched {i}/{len(out)}")
                await asyncio.sleep(0.2)
            out = _enrich_filter_year(enriched := enriched, year) if False else enriched  # keep line for clarity
            out = _enforce_year(out, year)

        await context.close(); await browser.close()

        out = [r for r in out if r.get("url")]
        out.sort(key=lambda r: (10**9 if r.get("price") is None else r["price"], r.get("miles") or 10**9))
        _print_progress(f"[MAIN] Done. {len(out)} rows total.")
        return out

# -------------------- Output --------------------
def print_rows(rows: List[Dict[str, Any]], k: int = 24):
    print(f"Got {len(rows)} rows")
    for r in rows[:k]:
        price = f"${r['price']:,}" if r.get("price") is not None else "N/A"
        miles = f"{r['miles']:,} mi" if r.get("miles") is not None else "—"
        yr = r.get("year") or "?"
        title = r.get("title") or "(no title)"
        url = r.get("url") or ""
        print(f"[{r.get('source')}] {yr} | {price} | {miles} | {title}")
        print(f"    {url}")

# -------------------- CLI --------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("make", nargs="?", default="Toyota")
    ap.add_argument("model", nargs="?", default="Camry")
    ap.add_argument("year", nargs="?", type=int, default=2015)
    ap.add_argument("zip_code", nargs="?", default=DEFAULT_ZIP)
    ap.add_argument("--radius", type=int, default=DEFAULT_RADIUS)
    ap.add_argument("--max", type=int, default=8, help="max results per site")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--enrich", action="store_true")
    ap.add_argument("--site", choices=["both","autotrader","cars"], default="both")
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--max-pages", type=int, default=5, help="Cars.com pages to fetch")
    ap.add_argument("--scroll-rounds", type=int, default=12, help="extra scroll rounds on Autotrader")
    args = ap.parse_args()

    rows = asyncio.run(
        query_listings_async(
            args.make, args.model, args.year,
            zip_code=args.zip_code, radius_miles=args.radius,
            max_results_each=args.max, headed=args.headed, enrich=args.enrich,
            site=args.site, debug=args.debug, max_pages=args.max_pages, scroll_rounds=args.scroll_rounds,
        )
    )
    print_rows(rows)
