import os
import re
import json
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse
from typing import Optional, Any

from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from openai import OpenAI

# -------- CONFIG --------
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
DEBUG_DIR = os.environ.get("SCRAPER_DEBUG_DIR", "scrape_debug")
SAVE_DEBUG_HTML = os.environ.get("SAVE_DEBUG_HTML", "1") == "1"
MAX_PAGINATION_CLICKS = int(os.environ.get("MAX_PAGINATION_CLICKS", "20"))
SCROLL_PASSES = int(os.environ.get("SCROLL_PASSES", "12"))

SITE_CONFIGS: list[dict[str, Any]] = [
    {"url": "https://trca.ca/events-calendar/", "kind": "listing", "tags": ["nature", "toronto", "vaughan"]},
    {"url": "https://visitvaughan.ca/calendar/", "kind": "listing", "tags": ["vaughan"]},
    {"url": "https://childslife.ca/events/category/kids-programs-and-workshops/nature-wildlife-programs/", "kind": "listing", "tags": ["nature", "kids"]},
    {"url": "https://kortright.org/whats-on/calendar/", "kind": "listing", "tags": ["vaughan", "nature"]},
    {"url": "https://kortright.org/about-us/volunteer/", "kind": "single_or_listing", "tags": ["volunteer", "nature"]},
    {"url": "https://www.vaughan.ca/about-city-vaughan/departments/environmental-sustainability/green-guardians/community-tree-planting", "kind": "single_or_listing", "tags": ["vaughan", "tree planting", "volunteer"]},
    {"url": "https://www.yourleaf.org/events", "kind": "listing", "tags": ["tree planting", "nature"]},
    {"url": "https://www.vaughan.ca/news/plant-new-roots-your-community", "kind": "single_or_listing", "tags": ["vaughan", "tree planting"]},
    {"url": "https://www.york.ca/newsroom/campaigns-projects/york-regional-forest-walks-and-events", "kind": "single_or_listing", "tags": ["forest", "walks"]},
    {"url": "https://www.evergreen.ca/evergreen-brick-works/whats-on/", "kind": "listing", "tags": ["nature", "toronto"]},
    {"url": "https://vaughanpl.info/programs", "kind": "listing", "tags": ["library", "teen"]},
    {"url": "https://www.vaughan.ca/residential/recreation-programs-and-fitness/service-registration/youth-week", "kind": "single_or_listing", "tags": ["youth"]},
    {"url": "https://www.todocanada.ca/city/toronto/events/", "kind": "listing", "tags": ["toronto"]},
    {"url": "https://www.eventbrite.ca/d/canada--york/free--travel-and-outdoor--events/?page=2", "kind": "listing", "tags": ["eventbrite", "outdoor"]},
    {"url": "https://www.cambridgebutterfly.com/upcoming-events/", "kind": "listing", "tags": ["butterfly", "nature"]},
    {"url": "https://ontarionature.org/events/annual-gathering/", "kind": "single_or_listing", "tags": ["nature"]},
    {"url": "https://www.hnpcanada.ca/upcoming-events", "kind": "listing", "tags": ["nature"]},
    {"url": "https://hamiltonnature.org/event-calendar/month/2026-04/", "kind": "listing", "tags": ["nature", "hamilton"]},
    {"url": "https://downsviewpark.ca/events", "kind": "listing", "tags": ["nature", "toronto"]},
    {"url": "https://tpl.bibliocommons.com/v2/events/69b2f414491b809c6f1fe7e0", "kind": "single_or_listing", "tags": ["library", "teen"]},
    {"url": "https://www.toronto.ca/explore-enjoy/parks-recreation/program-activities/arts-hobbies-interests/nature-eco-programs/forestry-talks-tours/forestry-talks-tours-calendar/", "kind": "listing", "tags": ["forestry", "nature"]},
    {"url": "https://www.torontozoo.com/events", "kind": "listing", "tags": ["zoo", "nature"]},
    {"url": "https://stellasplace.ca/program-participants/program-directory/", "kind": "listing", "tags": ["mental health", "youth"]},
    {"url": "https://highparknaturecentre.com/nature-clubs-youth/", "kind": "single_or_listing", "tags": ["youth", "nature"]},
    {"url": "https://bramlib.libnet.info/event/15927469", "kind": "single_or_listing", "tags": ["library", "nature", "teen"]},
    {"url": "https://cvc.ca/events/list/", "kind": "listing", "tags": ["conservation", "nature"]},
    {"url": "https://youthhubs.ca/site/maple-youth-wellness-hub", "kind": "single_or_listing", "tags": ["mental health", "youth"]},
    {"url": "https://www.vaughanmills.com/events/play-it-forward-for-mental-health/", "kind": "single_or_listing", "tags": ["mental health", "youth"]},
    {"url": "https://www.mackenziehealth.ca/support-us/foundation/events/community-events", "kind": "listing", "tags": ["community", "health"]},
    {"url": "https://downsviewpark.ca/events/earth-day-downsview-park", "kind": "single_or_listing", "tags": ["earth day", "nature"]},
    {"url": "https://www.scarbenv.ca", "kind": "listing", "tags": ["environment", "cleanup"]},
    {"url": "https://torontofieldnaturalists.org", "kind": "listing", "tags": ["nature walks", "wildlife"]},
    {"url": "https://www.toronto.ca/explore-enjoy/parks-recreation/places-spaces/parks-and-recreation-facilities/", "kind": "listing", "tags": ["parks"]},
    {"url": "https://stridestoronto.ca/program-service/whats-up-walk-in/", "kind": "single_or_listing", "tags": ["mental health", "youth"]},
    {"url": "https://www.eventbrite.ca/d/canada--vaughan/events/", "kind": "listing", "tags": ["eventbrite", "vaughan"]},
    {"url": "https://www.vaughanpl.info/events_calendars/calendar", "kind": "listing", "tags": ["library", "calendar"]},
    {"url": "https://www.vaughan.ca/upcoming-events", "kind": "listing", "tags": ["vaughan"]},
    {"url": "https://vaughanbusiness.ca/events/", "kind": "listing", "tags": ["business"]},
    {"url": "https://assemblypark.ca/events/", "kind": "listing", "tags": ["community"]},
    {"url": "https://www.meetup.com/find/ca--on--vaughan/", "kind": "listing", "tags": ["meetup"]},
]

POSITIVE_KEYWORDS = {
    "nature", "outdoor", "forest", "tree", "plant", "gardening", "garden", "wildlife",
    "conservation", "hike", "hiking", "bird", "birding", "park", "trail", "ravine",
    "earth day", "cleanup", "walk", "walks", "zoo", "camp", "campfire", "scavenger",
    "volunteer", "volunteering", "eco", "environment", "butterfly", "naturalist", "forestry"
}
NEGATIVE_KEYWORDS = {
    "casino", "nightclub", "nightlife", "bar", "cocktail", "networking", "sales", "real estate",
    "business expo", "conference", "wedding show", "marketplace"
}
YOUTH_HINTS = {"teen", "teens", "youth", "13-17", "12-25", "family", "all ages", "student"}
LOCATION_HINTS = {"vaughan", "woodbridge", "maple", "thornhill", "richmond hill", "york", "toronto", "scarborough", "brampton", "hamilton"}


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


def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def safe_slug(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text).strip("_").lower()
    return text[:80] or "site"


def try_parse_date(text: str) -> Optional[str]:
    text = normalize_whitespace(text)
    if not text:
        return None
    try:
        dt = dtparser.parse(text, fuzzy=True)
        return dt.isoformat(timespec="minutes")
    except Exception:
        return None


def infer_location(text: str) -> Optional[str]:
    txt = normalize_whitespace(text)
    for hint in LOCATION_HINTS:
        m = re.search(rf"\b({re.escape(hint)}[^|•\n]{{0,80}})", txt, re.I)
        if m:
            return normalize_whitespace(m.group(1))[:140]
    return None


def looks_relevant(text: str) -> bool:
    txt = normalize_whitespace(text).lower()
    if len(txt) < 6:
        return False
    if any(k in txt for k in NEGATIVE_KEYWORDS):
        return False
    return any(k in txt for k in POSITIVE_KEYWORDS) or any(k in txt for k in YOUTH_HINTS)


def accept_cookies(page):
    selectors = [
        "button:has-text('Accept')",
        "button:has-text('I agree')",
        "button:has-text('Got it')",
        "button:has-text('Allow all')",
        "button:has-text('Accept All')",
        "button:has-text('Accept all')",
        "button:has-text('Agree')",
        "button:has-text('OK')",
        "button:has-text('Ok')",
        "button:has-text('Continue')",
        "a:has-text('Accept')",
        "a:has-text('I agree')",
        "[id*='accept']",
        "[class*='accept']",
        "#onetrust-accept-btn-handler",
        ".onetrust-accept-btn-handler",
    ]
    for selector in selectors:
        try:
            el = page.locator(selector).first
            if el.count() > 0 and el.is_visible():
                el.click(timeout=2000)
                page.wait_for_timeout(800)
                return True
        except Exception:
            continue
    return False


def scroll_to_bottom_until_stable(page, passes: int = SCROLL_PASSES):
    last_height = -1
    stable_rounds = 0
    for _ in range(passes):
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            page.mouse.wheel(0, 2200)
        page.wait_for_timeout(1200)
        try:
            new_height = page.evaluate("document.body.scrollHeight")
        except Exception:
            new_height = None
        if new_height == last_height:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_height = new_height
        if stable_rounds >= 2:
            break


def click_pagination_until_done(page):
    selectors = [
        "button:has-text('Load more')",
        "button:has-text('Load More')",
        "a:has-text('Load more')",
        "a:has-text('Load More')",
        "button:has-text('Show more')",
        "a:has-text('Show more')",
        "button:has-text('More')",
        "button:has-text('Next')",
        "a:has-text('Next')",
        "[aria-label='Next']",
        "[aria-label='next']",
        ".pagination-next a",
        ".next a",
        ".load-more",
        ".btn-load-more",
    ]
    clicks = 0
    for _ in range(MAX_PAGINATION_CLICKS):
        did_click = False
        for selector in selectors:
            try:
                el = page.locator(selector).first
                if el.count() == 0 or not el.is_visible():
                    continue
                el.scroll_into_view_if_needed(timeout=1500)
                el.click(timeout=2500)
                page.wait_for_load_state("domcontentloaded", timeout=5000)
                page.wait_for_timeout(1800)
                scroll_to_bottom_until_stable(page, passes=4)
                did_click = True
                clicks += 1
                break
            except Exception:
                continue
        if not did_click:
            break
    return clicks


def render_page_html(url: str, timeout_ms: int = 120000) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 2200},
            locale="en-CA",
            timezone_id="America/Toronto",
            java_script_enabled=True,
            extra_http_headers={
                "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        page = context.new_page()
        page.set_default_timeout(12000)
        page.set_default_navigation_timeout(timeout_ms)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(2500)
            accept_cookies(page)
            scroll_to_bottom_until_stable(page)
            click_pagination_until_done(page)
            page.wait_for_timeout(2000)
            html = page.content()
        finally:
            context.close()
            browser.close()
    return html


def extract_event_cards(soup: BeautifulSoup, base_url: str) -> list[Event]:
    host = urlparse(base_url).netloc.replace("www.", "")
    events: list[Event] = []
    seen: set[tuple[str, str]] = set()

    card_selectors = [
        "article", ".event", ".events-card", ".tribe-events-event", ".event-card",
        ".calendar-event", ".search-result", ".listing", ".views-row", ".program-item",
        ".event-item", ".post", ".card", ".result", ".entry"
    ]

    candidate_nodes = []
    for selector in card_selectors:
        candidate_nodes.extend(soup.select(selector))
    if not candidate_nodes:
        candidate_nodes = list(soup.select("a[href]"))

    for node in candidate_nodes[:800]:
        a = node if getattr(node, "name", "") == "a" else node.select_one("a[href]")
        if not a:
            continue
        href = (a.get("href") or "").strip()
        title = normalize_whitespace(a.get_text(" "))
        if not href or not title or len(title) < 4:
            continue
        full_url = urljoin(base_url, href)
        block_text = normalize_whitespace(node.get_text(" "))[:2000]
        if not looks_relevant(block_text + " " + title):
            continue

        date_guess = None
        patterns = [
            r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,\s*\d{4})?(?:[^A-Za-z0-9]{1,10}\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?)?",
            r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?\b",
            r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
        ]
        for pat in patterns:
            m = re.search(pat, block_text, re.I)
            if m:
                date_guess = try_parse_date(m.group(0))
                if date_guess:
                    break

        location = infer_location(block_text)
        key = (title.lower(), full_url)
        if key in seen:
            continue
        seen.add(key)

        events.append(Event(
            title=title[:180],
            start=date_guess,
            end=None,
            location=location,
            url=full_url,
            source=host,
            description=block_text[:900] if block_text else None,
        ))

    return events


def extract_single_event_page(soup: BeautifulSoup, base_url: str) -> list[Event]:
    host = urlparse(base_url).netloc.replace("www.", "")
    title = None
    for selector in ["h1", "meta[property='og:title']", "title"]:
        node = soup.select_one(selector)
        if not node:
            continue
        if node.name == "meta":
            title = normalize_whitespace(node.get("content", ""))
        else:
            title = normalize_whitespace(node.get_text(" "))
        if title:
            break
    if not title:
        return []

    text = normalize_whitespace(soup.get_text(" "))[:5000]

    date_guess = None
    for pat in [
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}(?:,\s*\d{4})?(?:[^A-Za-z0-9]{1,10}\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?)?",
        r"\b\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2})?\b",
        r"\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    ]:
        m = re.search(pat, text, re.I)
        if m:
            date_guess = try_parse_date(m.group(0))
            if date_guess:
                break

    return [Event(
        title=title[:180],
        start=date_guess,
        end=None,
        location=infer_location(text),
        url=base_url,
        source=host,
        description=text[:900],
    )]


def extract_events_for_site(url: str, html: str, kind: str) -> list[Event]:
    soup = BeautifulSoup(html, "lxml")
    if kind == "listing":
        events = extract_event_cards(soup, url)
        if events:
            return events
        return extract_single_event_page(soup, url)
    return extract_single_event_page(soup, url) + extract_event_cards(soup, url)


def dedupe_events(events: list[Event]) -> list[Event]:
    dedup: dict[tuple[str, str, str], Event] = {}
    for e in events:
        key = (e.title.lower(), e.start or "", e.url or "")
        if key not in dedup:
            dedup[key] = e
    return list(dedup.values())


def classify_nature_and_teen(events: list[Event], batch_size: int = 25) -> list[Event]:
    if not client or not events:
        return events

    for i in range(0, len(events), batch_size):
        group = events[i:i + batch_size]
        payload = [
            {
                "title": e.title,
                "start": e.start,
                "location": e.location,
                "url": e.url,
                "source": e.source,
                "description": e.description,
            }
            for e in group
        ]

        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[
                {
                    "role": "system",
                    "content": (
                        "Classify events for a youth nature-events calendar. "
                        "nature_based=true only for outdoors, parks, trails, conservation, wildlife, tree planting, gardening, hikes, environmental volunteering, eco education, camping, birding, forestry, zoo, ravine or similar nature activities. "
                        "teen_ok_13_17=true only when it appears safe/appropriate for ages 13-17 or clearly family/all-ages/youth. "
                        "Return strict JSON only."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps({
                        "events": payload,
                        "task": "For each event, set nature_based, teen_ok_13_17, a short nature_reason, a short teen_reason, and 1-4 tags."
                    }, ensure_ascii=False),
                },
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "event_classification",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "results": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "title": {"type": "string"},
                                        "source": {"type": "string"},
                                        "url": {"type": ["string", "null"]},
                                        "nature_based": {"type": "boolean"},
                                        "teen_ok_13_17": {"type": "boolean"},
                                        "nature_reason": {"type": "string"},
                                        "teen_reason": {"type": "string"},
                                        "tags": {"type": "array", "items": {"type": "string"}},
                                    },
                                    "required": [
                                        "title", "source", "url", "nature_based", "teen_ok_13_17",
                                        "nature_reason", "teen_reason", "tags"
                                    ],
                                    "additionalProperties": False,
                                },
                            }
                        },
                        "required": ["results"],
                        "additionalProperties": False,
                    },
                }
            },
        )

        data = json.loads(resp.output_text)
        lookup = {(r["title"].strip().lower(), (r.get("url") or "").strip()): r for r in data["results"]}
        for e in group:
            r = lookup.get((e.title.strip().lower(), (e.url or "").strip()))
            if not r:
                continue
            e.nature_based = bool(r["nature_based"])
            e.teen_ok_13_17 = bool(r["teen_ok_13_17"])
            e.nature_reason = r["nature_reason"][:220]
            e.teen_reason = r["teen_reason"][:220]
            e.nature_tags = [t[:24] for t in (r.get("tags") or [])][:4]

    return events


def main():
    os.makedirs(DEBUG_DIR, exist_ok=True)
    all_events: list[Event] = []
    site_summaries: list[dict[str, Any]] = []

    for site in SITE_CONFIGS:
        url = site["url"]
        slug = safe_slug(urlparse(url).netloc + "_" + urlparse(url).path)
        print(f"Scraping: {url}")
        try:
            html = render_page_html(url)
            if SAVE_DEBUG_HTML:
                with open(os.path.join(DEBUG_DIR, f"{slug}.html"), "w", encoding="utf-8") as f:
                    f.write(html)

            events = extract_events_for_site(url, html, site["kind"])
            events = dedupe_events(events)
            print(f"  found ~{len(events)} candidates")
            all_events.extend(events)

            with open(os.path.join(DEBUG_DIR, f"{slug}.json"), "w", encoding="utf-8") as f:
                json.dump([
                    {
                        "title": e.title,
                        "start": e.start,
                        "location": e.location,
                        "url": e.url,
                        "source": e.source,
                        "description": e.description,
                    }
                    for e in events
                ], f, ensure_ascii=False, indent=2)

            site_summaries.append({
                "url": url,
                "kind": site["kind"],
                "candidate_count": len(events),
                "status": "ok",
            })
        except PlaywrightTimeoutError as ex:
            print(f"  TIMEOUT: {ex}")
            site_summaries.append({"url": url, "kind": site["kind"], "candidate_count": 0, "status": f"timeout: {ex}"})
        except Exception as ex:
            print(f"  FAILED: {ex}")
            site_summaries.append({"url": url, "kind": site["kind"], "candidate_count": 0, "status": f"failed: {ex}"})

    all_events = dedupe_events(all_events)
    print(f"Total unique candidates: {len(all_events)}")

    with open("site_counts.json", "w", encoding="utf-8") as f:
        json.dump(site_summaries, f, ensure_ascii=False, indent=2)
    print("Wrote site_counts.json")

    with open("all_events_raw.json", "w", encoding="utf-8") as f:
        json.dump([
            {
                "title": e.title,
                "start": e.start,
                "end": e.end,
                "location": e.location,
                "url": e.url,
                "source": e.source,
                "description": e.description,
            }
            for e in all_events
        ], f, ensure_ascii=False, indent=2)
    print("Wrote all_events_raw.json")

    classified = classify_nature_and_teen(all_events)
    filtered = [e for e in classified if e.nature_based is True and (e.teen_ok_13_17 is not False)]
    print(f"Filtered nature/youth events: {len(filtered)}")

    website_json = []
    for e in filtered:
        website_json.append({
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
        })

    with open("nature_events.json", "w", encoding="utf-8") as f:
        json.dump(website_json, f, ensure_ascii=False, indent=2)
    print("Wrote nature_events.json")


if __name__ == "__main__":
    main()
