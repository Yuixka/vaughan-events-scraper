import os
import re
import json
import time
from dataclasses import dataclass
from typing import Optional, Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from openai import OpenAI

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
DEBUG_DIR = os.environ.get("SCRAPER_DEBUG_DIR", "scrape_debug")
SAVE_DEBUG = os.environ.get("SAVE_DEBUG_HTML", "1") == "1"
HEADLESS = os.environ.get("HEADLESS", "1") == "1"
MAX_DETAIL_PAGES_PER_SITE = int(os.environ.get("MAX_DETAIL_PAGES_PER_SITE", "40"))
MAX_SCROLL_CYCLES = int(os.environ.get("MAX_SCROLL_CYCLES", "8"))
MAX_PAGINATION_CLICKS = int(os.environ.get("MAX_PAGINATION_CLICKS", "6"))
MAX_GOTO_TIMEOUT_MS = int(os.environ.get("MAX_GOTO_TIMEOUT_MS", "60000"))
MAX_PAGE_TOTAL_MS = int(os.environ.get("MAX_PAGE_TOTAL_MS", "90000"))
MAX_SITE_SECONDS = int(os.environ.get("MAX_SITE_SECONDS", "120"))

SITE_CONFIGS: list[dict[str, Any]] = [
    {"url": "https://trca.ca/events-calendar/", "kind": "listing"},
    {"url": "https://visitvaughan.ca/calendar/", "kind": "listing"},
    {"url": "https://childslife.ca/events/category/kids-programs-and-workshops/nature-wildlife-programs/", "kind": "listing"},
    {"url": "https://kortright.org/whats-on/calendar/", "kind": "listing"},
    {"url": "https://kortright.org/about-us/volunteer/", "kind": "single_or_listing"},
    {"url": "https://www.vaughan.ca/about-city-vaughan/departments/environmental-sustainability/green-guardians/community-tree-planting", "kind": "single_or_listing"},
    {"url": "https://www.yourleaf.org/events", "kind": "listing"},
    {"url": "https://www.vaughan.ca/news/plant-new-roots-your-community", "kind": "single_or_listing"},
    {"url": "https://www.york.ca/newsroom/campaigns-projects/york-regional-forest-walks-and-events", "kind": "single_or_listing"},
    {"url": "https://www.evergreen.ca/evergreen-brick-works/whats-on/", "kind": "listing"},
    {"url": "https://vaughanpl.info/programs", "kind": "listing"},
    {"url": "https://www.vaughan.ca/residential/recreation-programs-and-fitness/service-registration/youth-week", "kind": "single_or_listing"},
    {"url": "https://www.todocanada.ca/city/toronto/events/", "kind": "listing"},
    {"url": "https://www.eventbrite.ca/d/canada--york/free--travel-and-outdoor--events/?page=1", "kind": "listing"},
    {"url": "https://www.cambridgebutterfly.com/upcoming-events/", "kind": "listing"},
    {"url": "https://ontarionature.org/events/annual-gathering/", "kind": "single_or_listing"},
    {"url": "https://www.hnpcanada.ca/upcoming-events", "kind": "listing"},
    {"url": "https://hamiltonnature.org/event-calendar/month/2026-04/", "kind": "listing"},
    {"url": "https://downsviewpark.ca/events", "kind": "listing"},
    {"url": "https://tpl.bibliocommons.com/v2/events/69b2f414491b809c6f1fe7e0", "kind": "single_or_listing"},
    {"url": "https://www.toronto.ca/explore-enjoy/parks-recreation/program-activities/arts-hobbies-interests/nature-eco-programs/forestry-talks-tours/forestry-talks-tours-calendar/", "kind": "listing"},
    {"url": "https://www.torontozoo.com/events", "kind": "listing"},
    {"url": "https://stellasplace.ca/program-participants/program-directory/", "kind": "listing"},
    {"url": "https://highparknaturecentre.com/nature-clubs-youth/", "kind": "single_or_listing"},
    {"url": "https://bramlib.libnet.info/event/15927469", "kind": "single_or_listing"},
    {"url": "https://cvc.ca/events/list/", "kind": "listing"},
    {"url": "https://youthhubs.ca/site/maple-youth-wellness-hub", "kind": "single_or_listing"},
    {"url": "https://www.vaughanmills.com/events/play-it-forward-for-mental-health/", "kind": "single_or_listing"},
    {"url": "https://www.mackenziehealth.ca/support-us/foundation/events/community-events", "kind": "listing"},
    {"url": "https://downsviewpark.ca/events/earth-day-downsview-park", "kind": "single_or_listing"},
    {"url": "https://www.scarbenv.ca", "kind": "listing"},
    {"url": "https://torontofieldnaturalists.org", "kind": "listing"},
    {"url": "https://www.toronto.ca/explore-enjoy/parks-recreation/places-spaces/parks-and-recreation-facilities/", "kind": "listing"},
    {"url": "https://stridestoronto.ca/program-service/whats-up-walk-in/", "kind": "single_or_listing"},
    {"url": "https://www.eventbrite.ca/d/canada--vaughan/events/", "kind": "listing"},
    {"url": "https://www.vaughanpl.info/events_calendars/calendar", "kind": "listing"},
    {"url": "https://www.vaughan.ca/upcoming-events", "kind": "listing"},
    {"url": "https://vaughanbusiness.ca/events/", "kind": "listing"},
    {"url": "https://assemblypark.ca/events/", "kind": "listing"},
    {"url": "https://www.meetup.com/find/ca--on--vaughan/", "kind": "listing"},
]

POSITIVE_HINTS = [
    "nature", "outdoor", "forest", "tree", "plant", "gardening", "garden", "wildlife",
    "conservation", "hike", "hiking", "bird", "birding", "park", "trail", "ravine",
    "earth day", "cleanup", "walk", "walks", "zoo", "camp", "campfire", "scavenger",
    "volunteer", "volunteering", "eco", "environment", "butterfly", "naturalist", "forestry",
    "green", "community planting", "watershed"
]
DATE_PATTERNS = [
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,\s*\d{4})?(?:[^A-Za-z0-9]{1,15}\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?)?",
    r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?\b",
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
]


@dataclass
class Event:
    title: str
    start: Optional[str]
    end: Optional[str]
    location: Optional[str]
    url: Optional[str]
    source: str
    description: Optional[str]
    nature_based: Optional[bool] = None
    teen_ok_13_17: Optional[bool] = None
    nature_reason: Optional[str] = None
    teen_reason: Optional[str] = None
    nature_tags: Optional[list[str]] = None


def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def slugify(url: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", url).strip("_").lower()[:120]


def try_parse_date(text: str) -> Optional[str]:
    text = norm(text)
    if not text:
        return None
    try:
        dt = dtparser.parse(text, fuzzy=True)
        return dt.isoformat(timespec="minutes")
    except Exception:
        return None


def first_date(text: str) -> Optional[str]:
    text = norm(text)
    for pat in DATE_PATTERNS:
        m = re.search(pat, text, re.I)
        if m:
            parsed = try_parse_date(m.group(0))
            if parsed:
                return parsed
    return None


def infer_location(text: str) -> Optional[str]:
    text = norm(text)
    for pat in [
        r"\b(Vaughan[^|•\n]{0,80})",
        r"\b(Woodbridge[^|•\n]{0,80})",
        r"\b(Maple[^|•\n]{0,80})",
        r"\b(Thornhill[^|•\n]{0,80})",
        r"\b(Toronto[^|•\n]{0,80})",
        r"\b(Scarborough[^|•\n]{0,80})",
        r"\b(Brampton[^|•\n]{0,80})",
        r"\b(Hamilton[^|•\n]{0,80})",
    ]:
        m = re.search(pat, text, re.I)
        if m:
            return norm(m.group(1))[:140]
    return None


def relevant_text(text: str) -> bool:
    t = norm(text).lower()
    return len(t) >= 6 and any(k in t for k in POSITIVE_HINTS)


def event_title_from_node(node) -> str:
    for sel in ["h1", "h2", "h3", "h4", ".title", ".event-title", ".tribe-events-calendar-list__event-title", ".entry-title"]:
        el = node.select_one(sel)
        if el:
            txt = norm(el.get_text(" "))
            if len(txt) >= 4:
                return txt
    a = node.select_one("a[href]")
    return norm(a.get_text(" ")) if a else ""


def accept_cookies(page):
    selectors = [
        "#onetrust-accept-btn-handler", ".onetrust-accept-btn-handler",
        "button:has-text('Accept')", "button:has-text('Accept All')", "button:has-text('Accept all')",
        "button:has-text('I agree')", "button:has-text('Agree')", "button:has-text('OK')",
        "button:has-text('Continue')", "a:has-text('Accept')"
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible():
                loc.click(timeout=2500)
                page.wait_for_timeout(1000)
                return True
        except Exception:
            pass
    return False


def hydrate_page(page):
    start = time.monotonic()
    accept_cookies(page)

    stable_rounds = 0
    last_height = -1
    for _ in range(MAX_SCROLL_CYCLES):
        if (time.monotonic() - start) * 1000 > MAX_PAGE_TOTAL_MS:
            break
        try:
            page.mouse.wheel(0, 2200)
        except Exception:
            try:
                page.evaluate("window.scrollBy(0, 2200)")
            except Exception:
                pass
        try:
            page.wait_for_timeout(700)
            height = page.evaluate("document.body ? document.body.scrollHeight : 0")
        except Exception:
            height = last_height
        if height == last_height:
            stable_rounds += 1
        else:
            stable_rounds = 0
        last_height = height
        if stable_rounds >= 2:
            break

    for _ in range(MAX_PAGINATION_CLICKS):
        if (time.monotonic() - start) * 1000 > MAX_PAGE_TOTAL_MS:
            break
        clicked = False
        for sel in [
            "button:has-text('Load more')", "button:has-text('Load More')", "a:has-text('Load more')",
            "button:has-text('Show more')", "a:has-text('Show more')", "button:has-text('Next')",
            "a:has-text('Next')", "[aria-label='Next']", ".load-more", ".pagination-next a"
        ]:
            try:
                loc = page.locator(sel).first
                if loc.count() and loc.is_visible():
                    loc.scroll_into_view_if_needed(timeout=1200)
                    loc.click(timeout=2000)
                    page.wait_for_timeout(1200)
                    clicked = True
                    break
            except Exception:
                pass
        if not clicked:
            break

    try:
        page.wait_for_timeout(1000)
    except Exception:
        pass


def new_context(browser):
    return browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1440, "height": 2400},
        locale="en-CA",
        timezone_id="America/Toronto",
        java_script_enabled=True,
        extra_http_headers={"Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8"},
    )


def fetch_html(context, url: str) -> str:
    page = context.new_page()
    page.set_default_timeout(12000)
    page.set_default_navigation_timeout(MAX_GOTO_TIMEOUT_MS)
    last_error = None
    started = time.monotonic()
    for wait_until in ["domcontentloaded", "load"]:
        try:
            if (time.monotonic() - started) * 1000 > MAX_PAGE_TOTAL_MS:
                raise TimeoutError(f"page budget exceeded for {url}")
            page.goto(url, wait_until=wait_until, timeout=MAX_GOTO_TIMEOUT_MS)
            hydrate_page(page)
            html = page.content()
            page.close()
            return html
        except Exception as ex:
            last_error = ex
            try:
                page.wait_for_timeout(1200)
            except Exception:
                pass
    try:
        html = page.content()
        page.close()
        return html
    except Exception:
        page.close()
        raise last_error


def generic_listing_extract(soup: BeautifulSoup, base_url: str) -> list[Event]:
    host = urlparse(base_url).netloc.replace("www.", "")
    nodes = []
    for sel in [
        "article", ".event", ".tribe-events-calendar-list__event-row", ".tribe-common-g-row",
        ".event-card", ".card", ".views-row", ".program-item", ".entry", ".listing", ".result"
    ]:
        nodes.extend(soup.select(sel))
    if not nodes:
        nodes = list(soup.select("a[href]"))

    results: list[Event] = []
    seen: set[tuple[str, str]] = set()
    for node in nodes[:1500]:
        a = node if getattr(node, "name", "") == "a" else node.select_one("a[href]")
        if not a:
            continue
        href = (a.get("href") or "").strip()
        if not href:
            continue
        title = event_title_from_node(node) or norm(a.get_text(" "))
        if len(title) < 4:
            continue
        full = urljoin(base_url, href)
        text = norm(node.get_text(" "))[:3000]
        if not relevant_text(text + " " + title) and "/event" not in full and "/events" not in full:
            continue
        key = (title.lower(), full)
        if key in seen:
            continue
        seen.add(key)
        results.append(Event(
            title=title[:180],
            start=first_date(text),
            end=None,
            location=infer_location(text),
            url=full,
            source=host,
            description=text[:1000] or None,
        ))
    return results


def single_page_extract(soup: BeautifulSoup, url: str) -> list[Event]:
    host = urlparse(url).netloc.replace("www.", "")
    title = ""
    for sel in ["h1", ".entry-title", ".page-title", "title", "meta[property='og:title']"]:
        el = soup.select_one(sel)
        if not el:
            continue
        title = norm(el.get("content")) if el.name == "meta" else norm(el.get_text(" "))
        if title:
            break
    if len(title) < 4:
        return []
    text = norm(soup.get_text(" "))[:7000]
    return [Event(
        title=title[:180],
        start=first_date(text),
        end=None,
        location=infer_location(text),
        url=url,
        source=host,
        description=text[:1200] or None,
    )]


def site_specific_extract(url: str, html: str) -> list[Event]:
    soup = BeautifulSoup(html, "lxml")
    host = urlparse(url).netloc

    if "trca.ca" in host:
        nodes = soup.select("article, .tribe-events-calendar-list__event-row, .tribe-events-event, .tribe-common-g-row")
        if nodes:
            return generic_listing_extract(soup, url)

    if "visitvaughan.ca" in host:
        nodes = soup.select(".mec-event-article, article, .event-item, .mec-wrap")
        if nodes:
            return generic_listing_extract(soup, url)

    if "eventbrite.ca" in host:
        results = []
        seen = set()
        for a in soup.select("a[href*='/e/'], a[href*='eventbrite']"):
            href = urljoin(url, a.get("href"))
            title = norm(a.get_text(" "))
            if len(title) < 4:
                continue
            block = a.parent
            for _ in range(3):
                if getattr(block, 'parent', None):
                    block = block.parent
            text = norm(block.get_text(" "))[:2500]
            key = (title.lower(), href)
            if key in seen:
                continue
            seen.add(key)
            results.append(Event(title=title[:180], start=first_date(text), end=None, location=infer_location(text), url=href, source="eventbrite.ca", description=text[:1000] or None))
        if results:
            return results

    if "evergreen.ca" in host or "cvc.ca" in host or "downsviewpark.ca" in host or "vaughanpl.info" in host:
        return generic_listing_extract(soup, url)

    return generic_listing_extract(soup, url)


def enrich_detail_pages(context, events: list[Event], site_deadline: Optional[float] = None) -> list[Event]:
    out = []
    for i, e in enumerate(events[:MAX_DETAIL_PAGES_PER_SITE]):
        if site_deadline and time.monotonic() >= site_deadline:
            out.extend(events[i:])
            return out
        if not e.url:
            out.append(e)
            continue
        try:
            html = fetch_html(context, e.url)
            soup = BeautifulSoup(html, "lxml")
            text = norm(soup.get_text(" "))[:9000]
            if not e.start:
                e.start = first_date(text)
            if not e.location:
                e.location = infer_location(text)
            if not e.description or len(e.description) < 200:
                e.description = text[:1200] or e.description
        except Exception:
            pass
        out.append(e)
    out.extend(events[MAX_DETAIL_PAGES_PER_SITE:])
    return out


def dedupe(events: list[Event]) -> list[Event]:
    d: dict[tuple[str, str, str], Event] = {}
    for e in events:
        key = (e.title.lower(), e.start or "", e.url or "")
        if key not in d:
            d[key] = e
    return list(d.values())


def classify(events: list[Event], batch_size: int = 25) -> list[Event]:
    if not client or not events:
        return events
    for i in range(0, len(events), batch_size):
        group = events[i:i + batch_size]
        payload = [{
            "title": e.title, "start": e.start, "location": e.location,
            "url": e.url, "source": e.source, "description": e.description,
        } for e in group]
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {"role": "system", "content": (
                    "Classify events for a youth nature events calendar. "
                    "nature_based=true for outdoors, parks, trails, conservation, wildlife, tree planting, gardening, hikes, environmental volunteering, eco education, camping, birding, forestry, zoo or similar. "
                    "teen_ok_13_17=true when safe/appropriate for ages 13-17, or youth/family/all-ages. Return strict JSON only."
                )},
                {"role": "user", "content": json.dumps({"events": payload}, ensure_ascii=False)},
            ],
            text={"format": {"type": "json_schema", "name": "event_classification", "schema": {
                "type": "object", "properties": {
                    "results": {"type": "array", "items": {"type": "object", "properties": {
                        "title": {"type": "string"}, "url": {"type": ["string", "null"]}, "source": {"type": "string"},
                        "nature_based": {"type": "boolean"}, "teen_ok_13_17": {"type": "boolean"},
                        "nature_reason": {"type": "string"}, "teen_reason": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}}
                    }, "required": ["title", "url", "source", "nature_based", "teen_ok_13_17", "nature_reason", "teen_reason", "tags"], "additionalProperties": False}}
                }, "required": ["results"], "additionalProperties": False
            }}}
        )
        data = json.loads(resp.output_text)
        lookup = {(r["title"].strip().lower(), (r.get("url") or "").strip()): r for r in data["results"]}
        for e in group:
            r = lookup.get((e.title.strip().lower(), (e.url or "").strip()))
            if r:
                e.nature_based = r["nature_based"]
                e.teen_ok_13_17 = r["teen_ok_13_17"]
                e.nature_reason = r["nature_reason"][:220]
                e.teen_reason = r["teen_reason"][:220]
                e.nature_tags = [t[:24] for t in r.get("tags", [])][:4]
    return events


def main():
    os.makedirs(DEBUG_DIR, exist_ok=True)
    all_events: list[Event] = []
    site_counts = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-blink-features=AutomationControlled"])
        context = new_context(browser)
        for site in SITE_CONFIGS:
            url = site["url"]
            site_started = time.monotonic()
            site_deadline = site_started + MAX_SITE_SECONDS
            print(f"Scraping: {url}")
            try:
                html = fetch_html(context, url)
                if SAVE_DEBUG:
                    with open(os.path.join(DEBUG_DIR, f"{slugify(url)}.html"), "w", encoding="utf-8") as f:
                        f.write(html)
                soup = BeautifulSoup(html, "lxml")
                events = site_specific_extract(url, html)
                if site["kind"] != "listing":
                    events.extend(single_page_extract(soup, url))
                events = dedupe(events)
                events = enrich_detail_pages(context, events, site_deadline=site_deadline)
                events = dedupe(events)
                elapsed = round(time.monotonic() - site_started, 1)
                print(f"  found ~{len(events)} candidates in {elapsed}s")
                all_events.extend(events)
                status = "ok" if elapsed < MAX_SITE_SECONDS else "partial_timeout"
                site_counts.append({"url": url, "candidate_count": len(events), "status": status, "elapsed_seconds": elapsed})
                with open(os.path.join(DEBUG_DIR, f"{slugify(url)}.json"), "w", encoding="utf-8") as f:
                    json.dump([e.__dict__ for e in events], f, ensure_ascii=False, indent=2)
            except Exception as ex:
                elapsed = round(time.monotonic() - site_started, 1)
                print(f"  FAILED after {elapsed}s: {ex}")
                site_counts.append({"url": url, "candidate_count": 0, "status": f"failed: {ex}", "elapsed_seconds": elapsed})
        context.close()
        browser.close()

    all_events = dedupe(all_events)
    with open("site_counts.json", "w", encoding="utf-8") as f:
        json.dump(site_counts, f, ensure_ascii=False, indent=2)
    with open("all_events_raw.json", "w", encoding="utf-8") as f:
        json.dump([e.__dict__ for e in all_events], f, ensure_ascii=False, indent=2)

    print(f"Total unique candidates: {len(all_events)}")
    classified = classify(all_events)
    filtered = [e for e in classified if e.nature_based is True and e.teen_ok_13_17 is not False]
    with open("nature_events.json", "w", encoding="utf-8") as f:
        json.dump([
            {
                "title": e.title,
                "start": e.start,
                "end": e.end,
                "url": e.url,
                "extendedProps": {
                    "location": e.location,
                    "source": e.source,
                    "nature_reason": e.nature_reason,
                    "teen_reason": e.teen_reason,
                    "tags": e.nature_tags or [],
                },
            }
            for e in filtered
        ], f, ensure_ascii=False, indent=2)
    print(f"Filtered nature/youth events: {len(filtered)}")
    print("Wrote nature_events.json")


if __name__ == "__main__":
    main()
