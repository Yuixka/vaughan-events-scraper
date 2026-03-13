# scrape_3sites_teens.py
# pip install playwright beautifulsoup4 lxml python-dateutil openai
# playwright install chromium
# export OPENAI_API_KEY="YOUR_KEY"
# python3 scrape_3sites_teens.py

import os, re, json
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, urljoin
from dateutil import parser as dtparser
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from openai import OpenAI

# Env var wins; fallback optional
HARDCODED_OPENAI_API_KEY = ""
client = None

SITES = [
    "https://trca.ca/events-calendar/",
    "https://www.eventbrite.ca/d/canada--york/outdoor-events/",
    "https://visitvaughan.ca/calendar/2026/04/01/",
    "https://childslife.ca/events/category/kids-programs-and-workshops/nature-wildlife-programs/",
    "https://kortright.org/whats-on/calendar/",
    "https://www.evergreen.ca/evergreen-brick-works/whats-on/",
]

OUT_JSON = "nature_teens_events.json"

COMMON_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
)

# -------------------------
# Data model
# -------------------------
@dataclass
class RawEvent:
    source: str
    title: str
    url: Optional[str] = None
    when_text: Optional[str] = None
    time_text: Optional[str] = None
    location_text: Optional[str] = None
    venue_text: Optional[str] = None
    price_text: Optional[str] = None
    age_text: Optional[str] = None
    description_text: Optional[str] = None

@dataclass
class NormalizedEvent:
    title: str
    start: Optional[str]
    end: Optional[str]
    url: Optional[str]
    location: Optional[str]
    venue: Optional[str]
    source: str
    price: Optional[str]
    age_info: Optional[str]
    description: Optional[str]
    tags: List[str]
    nature_based: bool
    teen_ok_13_17: bool
    teen_reason: str
    nature_reason: str

# -------------------------
# Utils
# -------------------------
def get_client() -> OpenAI:
    global client
    api_key = (os.environ.get("OPENAI_API_KEY") or HARDCODED_OPENAI_API_KEY).strip()
    if not api_key:
        raise SystemExit("Missing OPENAI_API_KEY (or set HARDCODED_OPENAI_API_KEY in file).")
    if client is None:
        client = OpenAI(api_key=api_key)
    return client

def host(url: str) -> str:
    return urlparse(url).netloc.replace("www.", "")

def norm_ws(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def safe_abs(base: str, href: str) -> str:
    return urljoin(base, href)

def try_parse_datetime(date_text: Optional[str], time_text: Optional[str]) -> Optional[str]:
    d = norm_ws(date_text)
    t = norm_ws(time_text)
    if not d and not t:
        return None
    joined = " ".join([x for x in [d, t] if x])
    try:
        dt = dtparser.parse(joined, fuzzy=True)
        if d and not t:
            return dt.date().isoformat()
        return dt.isoformat(timespec="minutes")
    except Exception:
        if d:
            try:
                dt = dtparser.parse(d, fuzzy=True)
                return dt.date().isoformat()
            except Exception:
                return None
        return None

# -------------------------
# Playwright helpers
# -------------------------
def goto(page, url: str, timeout_ms: int = 90000):
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

def click_if_exists(page, selectors: List[str], timeout_ms: int = 1500) -> bool:
    for sel in selectors:
        try:
            el = page.locator(sel)
            if el.count() > 0 and el.first.is_visible():
                el.first.click(timeout=timeout_ms)
                return True
        except Exception:
            continue
    return False

def scroll_to_bottom_until_stable(page, max_rounds: int = 10, pause_ms: int = 650):
    last_h = 0
    stable = 0
    for _ in range(max_rounds):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(pause_ms)
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            stable += 1
            if stable >= 2:
                break
        else:
            stable = 0
            last_h = h

def try_accept_cookies(page):
    click_if_exists(page, [
        "button:has-text('Accept all')",
        "button:has-text('Accept All')",
        "button:has-text('Accept')",
        "button:has-text('I agree')",
        "button:has-text('Agree')",
        "button:has-text('OK')",
        "button:has-text('Got it')",
        "button[aria-label*='Accept']",
        "button[aria-label*='agree']",
        "text=Accept all",
        "text=Accept",
    ], timeout_ms=1200)

# -------------------------
# Site A: TRCA
# -------------------------
def scrape_trca(page, url: str, max_clicks: int = 20) -> List[RawEvent]:
    goto(page, url)
    page.wait_for_timeout(600)
    try_accept_cookies(page)

    for _ in range(max_clicks):
        scroll_to_bottom_until_stable(page, max_rounds=2, pause_ms=500)
        clicked = click_if_exists(page, [
            "button:has-text('Load More')",
            "button:has-text('Load more')",
            "a:has-text('Load More')",
            "a:has-text('Load more')",
            "[aria-label*='Load']",
        ])
        if not clicked:
            break
        page.wait_for_timeout(900)

    soup = BeautifulSoup(page.content(), "lxml")
    events: List[RawEvent] = []

    for a in soup.select("a[href*='/event/'], a[href*='/events/']"):
        title = norm_ws(a.get_text(" "))
        href = a.get("href") or ""
        if not title or len(title) < 6:
            continue

        abs_url = safe_abs(url, href)

        card = a
        for _ in range(6):
            if getattr(card, "parent", None):
                card = card.parent

        block = norm_ws(card.get_text(" ")) or ""
        block = block[:1200]

        when_text = None
        time_text = None
        location_text = None
        price_text = None

        m = re.search(r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b[^|•\n]{0,50}\d{1,2}(?:[^0-9]{0,10}\d{4})?)", block, re.I)
        if m:
            when_text = m.group(1)

        tm = re.search(r"(\b\d{1,2}:\d{2}\s?(?:AM|PM)\b(?:\s?[-–]\s?\d{1,2}:\d{2}\s?(?:AM|PM))?)", block, re.I)
        if tm:
            time_text = tm.group(1)

        lm = re.search(r"\b(Vaughan|Woodbridge|Bolton|Kleinburg|King City|Ontario|ON|Toronto)\b[^|•\n]{0,80}", block, re.I)
        if lm:
            location_text = norm_ws(lm.group(0))

        pm = re.search(r"(\$\s?\d+(?:\.\d{2})?|\bFree\b)", block, re.I)
        if pm:
            price_text = pm.group(1)

        events.append(RawEvent(
            source=host(url),
            title=title[:160],
            url=abs_url,
            when_text=norm_ws(when_text),
            time_text=norm_ws(time_text),
            location_text=norm_ws(location_text),
            price_text=norm_ws(price_text),
            description_text=norm_ws(block),
        ))

    uniq: Dict[tuple, RawEvent] = {}
    for e in events:
        k = (e.title.lower(), (e.url or "").lower())
        if k not in uniq:
            uniq[k] = e
    return list(uniq.values())[:250]

# -------------------------
# Site B: Eventbrite
# -------------------------
def scrape_eventbrite(page, url: str, max_pages: int = 5) -> List[RawEvent]:
    goto(page, url)
    page.wait_for_timeout(900)
    try_accept_cookies(page)

    events: List[RawEvent] = []
    visited_pages = 0

    while visited_pages < max_pages:
        scroll_to_bottom_until_stable(page, max_rounds=7, pause_ms=700)
        soup = BeautifulSoup(page.content(), "lxml")

        for a in soup.select("a[href*='/e/']"):
            href = a.get("href") or ""
            abs_url = safe_abs(url, href)
            title = norm_ws(a.get_text(" "))
            if not title or len(title) < 6:
                continue

            card = a
            for _ in range(7):
                if getattr(card, "parent", None):
                    card = card.parent

            block = norm_ws(card.get_text(" ")) or ""
            block = block[:1200]

            when_text = None
            time_text = None
            location_text = None

            m = re.search(
                r"(\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\b.*?\d{4}|\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b.*?\d{4})",
                block, re.I
            )
            if m:
                when_text = m.group(1)

            tm = re.search(r"(\b\d{1,2}:\d{2}\s?(?:AM|PM)\b)", block, re.I)
            if tm:
                time_text = tm.group(1)

            lm = re.search(r"\b(Vaughan|York|Woodbridge|Richmond Hill|Markham|Toronto|Ontario|ON)\b[^|•\n]{0,80}", block, re.I)
            if lm:
                location_text = norm_ws(lm.group(0))

            events.append(RawEvent(
                source=host(url),
                title=title[:160],
                url=abs_url,
                when_text=norm_ws(when_text),
                time_text=norm_ws(time_text),
                location_text=norm_ws(location_text),
                description_text=norm_ws(block),
            ))

        next_clicked = click_if_exists(page, [
            "a[rel='next']",
            "button[aria-label='Next']",
            "a[aria-label='Next']",
            "a:has-text('Next')",
            "button:has-text('Next')",
            "a:has-text('›')",
            "button:has-text('›')",
        ], timeout_ms=2200)

        if not next_clicked:
            break

        visited_pages += 1
        page.wait_for_timeout(1400)
        try_accept_cookies(page)

    uniq: Dict[tuple, RawEvent] = {}
    for e in events:
        k = (e.title.lower(), (e.url or "").lower())
        if k not in uniq:
            uniq[k] = e
    return list(uniq.values())[:400]

# -------------------------
# Site C: VisitVaughan Calendar
# -------------------------
def scrape_visitvaughan(page, url: str, months_forward: int = 3) -> List[RawEvent]:
    goto(page, url)
    page.wait_for_timeout(900)
    try_accept_cookies(page)

    events: List[RawEvent] = []

    def parse_current():
        soup = BeautifulSoup(page.content(), "lxml")
        for a in soup.select("a[href*='/calendar/'], a[href*='/event/'], a[href*='/events/']"):
            title = norm_ws(a.get_text(" "))
            href = a.get("href") or ""
            if not title or len(title) < 6:
                continue

            abs_url = safe_abs(url, href)

            card = a
            for _ in range(7):
                if getattr(card, "parent", None):
                    card = card.parent

            block = norm_ws(card.get_text(" ")) or ""
            block = block[:1400]

            when_text = None
            time_text = None
            location_text = None

            m = re.search(r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b[^|•\n]{0,50}\d{1,2}(?:[^0-9]{0,10}\d{4})?)", block, re.I)
            if m:
                when_text = m.group(1)

            tm = re.search(r"(\b\d{1,2}:\d{2}\s?(?:AM|PM)\b(?:\s?[-–]\s?\d{1,2}:\d{2}\s?(?:AM|PM))?)", block, re.I)
            if tm:
                time_text = tm.group(1)

            lm = re.search(r"\b(Vaughan|Woodbridge|Ontario|ON|Toronto)\b[^|•\n]{0,80}", block, re.I)
            if lm:
                location_text = norm_ws(lm.group(0))

            events.append(RawEvent(
                source=host(url),
                title=title[:160],
                url=abs_url,
                when_text=norm_ws(when_text),
                time_text=norm_ws(time_text),
                location_text=norm_ws(location_text),
                description_text=norm_ws(block),
            ))

    for _ in range(months_forward + 1):
        for _k in range(10):
            scroll_to_bottom_until_stable(page, max_rounds=2, pause_ms=600)
            clicked = click_if_exists(page, [
                "button:has-text('LOAD MORE EVENTS')",
                "button:has-text('Load more events')",
                "a:has-text('LOAD MORE EVENTS')",
                "a:has-text('Load more events')",
                "button:has-text('Load More')",
                "a:has-text('Load More')",
            ], timeout_ms=1700)
            if not clicked:
                break
            page.wait_for_timeout(900)

        parse_current()

        moved = click_if_exists(page, [
            "button[aria-label*='Next']",
            "a[aria-label*='Next']",
            "button:has-text('›')",
            "a:has-text('›')",
            "button:has-text('>')",
            "a:has-text('>')",
        ], timeout_ms=2200)

        if not moved:
            try:
                toolbar = page.locator("header, .calendar, .tribe-events-c-top-bar, .fc-toolbar")
                if toolbar.count() > 0:
                    btns = toolbar.first.locator("button, a")
                    if btns.count() > 0:
                        btns.nth(btns.count() - 1).click(timeout=1500)
                        moved = True
            except Exception:
                pass

        if not moved:
            break

        page.wait_for_timeout(1200)
        try_accept_cookies(page)

    uniq: Dict[tuple, RawEvent] = {}
    for e in events:
        k = (e.title.lower(), (e.url or "").lower())
        if k not in uniq:
            uniq[k] = e
    return list(uniq.values())[:400]

# -------------------------
# Site D: Child's Life
# -------------------------
def scrape_childslife(page, url: str) -> List[RawEvent]:
    goto(page, url)
    page.wait_for_timeout(900)
    try_accept_cookies(page)
    scroll_to_bottom_until_stable(page, max_rounds=5, pause_ms=700)

    soup = BeautifulSoup(page.content(), "lxml")
    events: List[RawEvent] = []

    for art in soup.select("article, .tribe-events-calendar-list__event-row, .type-tribe_events, .tribe-common-g-row"):
        a = art.select_one("h3 a, h2 a, h4 a, a[href*='/event/'], a[href*='/events/']")
        if not a:
            continue

        title = norm_ws(a.get_text(" "))
        href = a.get("href") or ""
        if not title or len(title) < 6:
            continue

        block = norm_ws(art.get_text(" ")) or ""
        when_text = None
        time_text = None
        location_text = None
        price_text = None
        age_text = None

        m = re.search(
            r"((?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|Mon|Tue|Wed|Thu|Fri|Sat|Sun)[^|•\n]{0,120})",
            block,
            re.I
        )
        if m:
            when_text = m.group(1)

        tm = re.search(r"(\b\d{1,2}(?::\d{2})?\s?(?:AM|PM)\b(?:\s?[-–]\s?\d{1,2}(?::\d{2})?\s?(?:AM|PM))?)", block, re.I)
        if tm:
            time_text = tm.group(1)

        lm = re.search(r"([A-Z][^|•\n]{0,100}(?:Vaughan|Woodbridge|Toronto|Rexdale|Midland|Ontario|ON|Canada)[^|•\n]{0,100})", block, re.I)
        if lm:
            location_text = norm_ws(lm.group(1))

        pm = re.search(r"(\$\s?\d+(?:\.\d{2})?|\bFree\b)", block, re.I)
        if pm:
            price_text = pm.group(1)

        am = re.search(r"(Ages?\s*\d+\s*[-–]\s*\d+|Ages?\s*\d+\+|All ages|Family friendly|Teens?)", block, re.I)
        if am:
            age_text = am.group(1)

        events.append(RawEvent(
            source=host(url),
            title=title[:160],
            url=safe_abs(url, href),
            when_text=norm_ws(when_text),
            time_text=norm_ws(time_text),
            location_text=norm_ws(location_text),
            price_text=norm_ws(price_text),
            age_text=norm_ws(age_text),
            description_text=block[:1400],
        ))

    uniq: Dict[tuple, RawEvent] = {}
    for e in events:
        k = (e.title.lower(), (e.url or "").lower())
        if k not in uniq:
            uniq[k] = e
    return list(uniq.values())[:250]

# -------------------------
# Site E: Kortright
# -------------------------
def scrape_kortright(page, url: str, max_clicks: int = 8) -> List[RawEvent]:
    goto(page, url)
    page.wait_for_timeout(1000)
    try_accept_cookies(page)

    for _ in range(max_clicks):
        scroll_to_bottom_until_stable(page, max_rounds=2, pause_ms=700)
        clicked = click_if_exists(page, [
            "button:has-text('Load More')",
            "button:has-text('Load more')",
            "a:has-text('Load More')",
            "a:has-text('Load more')",
        ], timeout_ms=1800)
        if not clicked:
            break
        page.wait_for_timeout(1000)

    soup = BeautifulSoup(page.content(), "lxml")
    events: List[RawEvent] = []

    for a in soup.select("a[href*='/event/'], a[href*='/events/'], a[href*='/program/'], a[href*='/family-programs/'], a[href*='/adult-programs/'], a[href*='/camps/']"):
        title = norm_ws(a.get_text(" "))
        href = a.get("href") or ""
        if not title or len(title) < 6:
            continue

        abs_url = safe_abs(url, href)

        card = a
        for _ in range(7):
            if getattr(card, "parent", None):
                card = card.parent

        block = norm_ws(card.get_text(" ")) or ""
        if len(block) < 20:
            continue
        block = block[:1400]

        when_text = None
        time_text = None
        location_text = None
        price_text = None

        m = re.search(r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b[^|•\n]{0,60}\d{1,2}(?:[^0-9]{0,12}\d{4})?)", block, re.I)
        if m:
            when_text = m.group(1)

        tm = re.search(r"(\b\d{1,2}:\d{2}\s?(?:AM|PM)\b(?:\s?[-–]\s?\d{1,2}:\d{2}\s?(?:AM|PM))?)", block, re.I)
        if tm:
            time_text = tm.group(1)

        lm = re.search(r"(Kortright Centre[^|•\n]{0,100}|9550 Pine Valley Drive[^|•\n]{0,100}|Woodbridge[^|•\n]{0,100}|Vaughan[^|•\n]{0,100})", block, re.I)
        if lm:
            location_text = norm_ws(lm.group(1))

        pm = re.search(r"(\$\s?\d+(?:\.\d{2})?|\bFree\b)", block, re.I)
        if pm:
            price_text = pm.group(1)

        events.append(RawEvent(
            source=host(url),
            title=title[:160],
            url=abs_url,
            when_text=norm_ws(when_text),
            time_text=norm_ws(time_text),
            location_text=norm_ws(location_text),
            price_text=norm_ws(price_text),
            description_text=block,
        ))

    uniq: Dict[tuple, RawEvent] = {}
    for e in events:
        k = (e.title.lower(), (e.url or "").lower())
        if k not in uniq:
            uniq[k] = e
    return list(uniq.values())[:250]

# -------------------------
# Site F: Evergreen
# -------------------------
def scrape_evergreen(page, url: str, max_clicks: int = 8) -> List[RawEvent]:
    goto(page, url)
    page.wait_for_timeout(1000)
    try_accept_cookies(page)

    for _ in range(max_clicks):
        scroll_to_bottom_until_stable(page, max_rounds=2, pause_ms=700)
        clicked = click_if_exists(page, [
            "button:has-text('Load More')",
            "button:has-text('Load more')",
            "a:has-text('Load More')",
            "a:has-text('Load more')",
            "button:has-text('Show More')",
        ], timeout_ms=1800)
        if not clicked:
            break
        page.wait_for_timeout(1000)

    soup = BeautifulSoup(page.content(), "lxml")
    events: List[RawEvent] = []

    for a in soup.select("a[href*='/whats-on/'], a[href*='/event/'], article a, section a"):
        title = norm_ws(a.get_text(" "))
        href = a.get("href") or ""
        if not title or len(title) < 6:
            continue

        abs_url = safe_abs(url, href)

        card = a
        for _ in range(8):
            if getattr(card, "parent", None):
                card = card.parent

        block = norm_ws(card.get_text(" ")) or ""
        if len(block) < 20:
            continue
        block = block[:1400]

        when_text = None
        time_text = None
        location_text = None
        price_text = None
        age_text = None

        m = re.search(
            r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b[^|•\n]{0,60}\d{1,2}(?:[^0-9]{0,12}\d{4})?|Every Saturday 9am-1pm|Weekdays from 4pm to dusk, weekends 8am to dusk|Visit page for dates and times|Saturdays and by request|All of April 2026|Weekends throughout September|Mondays to Fridays from October to June|Fall season: Oct - Dec)",
            block,
            re.I
        )
        if m:
            when_text = m.group(1)

        tm = re.search(r"(\b\d{1,2}(?::\d{2})?\s?(?:AM|PM|am|pm)\b(?:\s?[-–]\s?\d{1,2}(?::\d{2})?\s?(?:AM|PM|am|pm))?)", block, re.I)
        if tm:
            time_text = tm.group(1)

        lm = re.search(r"(Evergreen Brick Works[^|•\n]{0,100}|Toronto[^|•\n]{0,100}|Bayview[^|•\n]{0,100})", block, re.I)
        if lm:
            location_text = norm_ws(lm.group(1))

        pm = re.search(r"(\$\s?\d+(?:\.\d{2})?|\bFree\b)", block, re.I)
        if pm:
            price_text = pm.group(1)

        am = re.search(r"(children aged\s*\d+\s*[-–]\s*\d+|Ages?\s*\d+\s*[-–]\s*\d+|Ages?\s*\d+\+|All ages welcome|kids|children|youth|teens?)", block, re.I)
        if am:
            age_text = am.group(1)

        events.append(RawEvent(
            source=host(url),
            title=title[:160],
            url=abs_url,
            when_text=norm_ws(when_text),
            time_text=norm_ws(time_text),
            location_text=norm_ws(location_text),
            price_text=norm_ws(price_text),
            age_text=norm_ws(age_text),
            description_text=block,
        ))

    uniq: Dict[tuple, RawEvent] = {}
    for e in events:
        k = (e.title.lower(), (e.url or "").lower())
        if k not in uniq:
            uniq[k] = e
    return list(uniq.values())[:250]

# -------------------------
# Optional: Enrich by opening event detail pages
# -------------------------
def enrich_details(page, raw: RawEvent, timeout_ms: int = 45000) -> RawEvent:
    if not raw.url:
        return raw
    try:
        goto(page, raw.url, timeout_ms=timeout_ms)
        page.wait_for_timeout(700)
        try_accept_cookies(page)

        soup = BeautifulSoup(page.content(), "lxml")
        text = norm_ws(soup.get_text(" ")) or ""
        text = text[:3500]

        loc = raw.location_text
        mloc = re.search(r"(Location|Where)\s*[:\-]\s*([^\n•|]{6,120})", text, re.I)
        if mloc:
            loc = norm_ws(mloc.group(2)) or loc

        age = raw.age_text
        mage = re.search(r"(All ages|Ages?\s*\d+\s*[-–]\s*\d+|Ages?\s*\d+\+|Family friendly|Youth|Teens?)", text, re.I)
        if mage:
            age = norm_ws(mage.group(0)) or age

        price = raw.price_text
        mprice = re.search(r"(\bFree\b|\$\s?\d+(?:\.\d{2})?)", text, re.I)
        if mprice and not price:
            price = mprice.group(1)

        when = raw.when_text
        mdate = re.search(r"(\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b[^|•\n]{0,50}\d{1,2}(?:[^0-9]{0,10}\d{4})?)", text, re.I)
        if mdate and not when:
            when = mdate.group(1)

        ttxt = raw.time_text
        mt = re.search(r"(\b\d{1,2}:\d{2}\s?(?:AM|PM)\b(?:\s?[-–]\s?\d{1,2}:\d{2}\s?(?:AM|PM))?)", text, re.I)
        if mt and not ttxt:
            ttxt = mt.group(1)

        raw.when_text = norm_ws(when) or raw.when_text
        raw.time_text = norm_ws(ttxt) or raw.time_text
        raw.location_text = norm_ws(loc) or raw.location_text
        raw.price_text = norm_ws(price) or raw.price_text
        raw.age_text = norm_ws(age) or raw.age_text

        if not raw.description_text:
            raw.description_text = text[:900]
        return raw
    except Exception:
        return raw

# -------------------------
# OpenAI normalize + filter
# -------------------------
def ai_normalize_and_filter(batch: List[RawEvent]) -> List[NormalizedEvent]:
    c = get_client()

    payload = [{
        "source": e.source,
        "title": e.title,
        "url": e.url,
        "when_text": e.when_text,
        "time_text": e.time_text,
        "location_text": e.location_text,
        "venue_text": e.venue_text,
        "price_text": e.price_text,
        "age_text": e.age_text,
        "description_text": e.description_text,
    } for e in batch]

    schema = {
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
                        "start": {"type": ["string", "null"]},
                        "end": {"type": ["string", "null"]},
                        "location": {"type": ["string", "null"]},
                        "venue": {"type": ["string", "null"]},
                        "price": {"type": ["string", "null"]},
                        "age_info": {"type": ["string", "null"]},
                        "description": {"type": ["string", "null"]},
                        "tags": {"type": "array", "items": {"type": "string"}},
                        "nature_based": {"type": "boolean"},
                        "teen_ok_13_17": {"type": "boolean"},
                        "nature_reason": {"type": "string"},
                        "teen_reason": {"type": "string"},
                    },
                    "required": [
                        "title", "source", "url", "start", "end", "location", "venue", "price", "age_info",
                        "description", "tags", "nature_based", "teen_ok_13_17", "nature_reason", "teen_reason"
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["results"],
        "additionalProperties": False,
    }

    resp = c.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role": "system", "content":
                "You normalize event info and classify:\n"
                "1) nature_based: outdoors/nature (parks, trails, conservation, wildlife, gardening, hiking, paddling, birding, outdoor volunteering/education)\n"
                "2) teen_ok_13_17: suitable for ages 13–17 (safe; not 18+/nightlife/alcohol/casino/explicit)\n"
                "Return strict JSON only matching the schema.\n"
                "Dates: output ISO strings if you can; if unsure keep null.\n"
                "If time missing but date exists, output start as YYYY-MM-DD."
            },
            {"role": "user", "content": json.dumps({
                "task": "Normalize each event and classify nature_based + teen_ok_13_17. Keep tags short.",
                "events": payload
            }, ensure_ascii=False)}
        ],
        text={"format": {
            "type": "json_schema",
            "name": "normalized_events",
            "schema": schema
        }}
    )

    data = json.loads(resp.output_text)
    out: List[NormalizedEvent] = []
    for r in data.get("results", []):
        out.append(NormalizedEvent(
            title=r["title"],
            start=r["start"],
            end=r["end"],
            url=r["url"],
            location=r["location"],
            venue=r["venue"],
            source=r["source"],
            price=r["price"],
            age_info=r["age_info"],
            description=r["description"],
            tags=(r["tags"] or [])[:6],
            nature_based=bool(r["nature_based"]),
            teen_ok_13_17=bool(r["teen_ok_13_17"]),
            teen_reason=(r["teen_reason"] or "")[:240],
            nature_reason=(r["nature_reason"] or "")[:240],
        ))
    return out

def to_fullcalendar_json(evts: List[NormalizedEvent]) -> List[Dict[str, Any]]:
    return [{
        "title": e.title,
        "start": e.start,
        "end": e.end,
        "url": e.url,
        "extendedProps": {
            "location": e.location,
            "venue": e.venue,
            "source": e.source,
            "price": e.price,
            "age_info": e.age_info,
            "tags": e.tags,
            "nature_reason": e.nature_reason,
            "teen_reason": e.teen_reason,
            "description": e.description,
        }
    } for e in evts]

# -------------------------
# Main
# -------------------------
def main():
    all_raw: List[RawEvent] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent=COMMON_UA,
            viewport={"width": 1280, "height": 800},
            locale="en-CA",
        )
        page = context.new_page()

        for url in SITES:
            print("Scraping:", url)
            try:
                if "trca.ca" in url:
                    raw = scrape_trca(page, url)
                elif "eventbrite" in url:
                    raw = scrape_eventbrite(page, url)
                elif "visitvaughan.ca" in url:
                    raw = scrape_visitvaughan(page, url, months_forward=3)
                elif "childslife.ca" in url:
                    raw = scrape_childslife(page, url)
                elif "kortright.org" in url:
                    raw = scrape_kortright(page, url)
                elif "evergreen.ca" in url:
                    raw = scrape_evergreen(page, url)
                else:
                    raw = []
                print("  candidates:", len(raw))
                all_raw.extend(raw)
            except PWTimeout as ex:
                print("  FAILED timeout:", ex)
            except Exception as ex:
                print("  FAILED:", ex)

        uniq: Dict[tuple, RawEvent] = {}
        for e in all_raw:
            k = (e.source, e.title.lower(), (e.url or "").lower())
            if k not in uniq:
                uniq[k] = e
        all_raw = list(uniq.values())
        print("Total candidates:", len(all_raw))

        with open("raw_candidates.json", "w", encoding="utf-8") as f:
            json.dump([asdict(e) for e in all_raw], f, ensure_ascii=False, indent=2)
        print("Saved raw_candidates.json:", len(all_raw))

        ENRICH_N = min(80, len(all_raw))
        for i in range(ENRICH_N):
            all_raw[i] = enrich_details(page, all_raw[i])

        browser.close()

    normalized: List[NormalizedEvent] = []
    BATCH = 25
    for i in range(0, len(all_raw), BATCH):
        chunk = all_raw[i:i+BATCH]
        normed = ai_normalize_and_filter(chunk)
        normalized.extend(normed)

    final = [e for e in normalized if e.nature_based and e.teen_ok_13_17]

    if not final:
        print("Warning: 0 final events after filtering. (Could be overly strict or site blocking.)")

    payload = to_fullcalendar_json(final)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print("Wrote:", OUT_JSON, "events:", len(payload))

if __name__ == "__main__":
    main()
