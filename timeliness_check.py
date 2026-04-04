#!/usr/bin/env python3
"""
Timeliness checker for CTFG civic tech project listings.
Processes BATCH_SIZE projects per run, cycling through all 14k+ records over time.

Usage:
    export AIRTABLE_PAT=your_personal_access_token
    export GITHUB_TOKEN=your_github_token      # optional but strongly recommended
    export YOUTUBE_API_KEY=your_yt_api_key     # optional, enables YouTube last-video date
    python timeliness_check.py

Dependencies:
    pip install requests feedparser

Airtable fields written:
    - Liveliness score    (0–100 numeric)
    - Activity status     (Active / Likely Active / Possibly Inactive / Inactive / Unknown)
    - Last activity date  (most recent signal found)
    - Last timeliness check (today's date, used to advance the rolling queue)

Social recency is checked (most recent post date) for:
    YouTube (needs YOUTUBE_API_KEY), Bluesky, Medium, Reddit, Substack,
    Mastodon / fediverse (any /@username URL on a non-major-platform domain).
    Twitter/X, LinkedIn, Facebook, Instagram: URL-alive only (no post date).
"""

import os
import sys
import re
import time
import json
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests

try:
    import feedparser
    HAS_FEEDPARSER = True
except ImportError:
    HAS_FEEDPARSER = False
    print("Warning: feedparser not installed — blog feed checks disabled.", file=sys.stderr)
    print("Install with: pip install feedparser", file=sys.stderr)


# ── Config ────────────────────────────────────────────────────────────────────

AIRTABLE_PAT = os.environ.get("AIRTABLE_PAT") or os.environ.get("AIRTABLE_API_KEY")
if not AIRTABLE_PAT:
    sys.exit("Error: set AIRTABLE_PAT environment variable")

GITHUB_TOKEN   = os.environ.get("GITHUB_TOKEN", "")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
BASE_ID       = "appYHxsLYleU2RVYk"
LISTINGS_TABLE = "tblELFP9tGX07UZDo"
LINKS_TABLE    = "tblpRr3lPFncgTS8y"
BATCH_SIZE     = 10

# ── Airtable field IDs — Listings ─────────────────────────────────────────────

F_NAME        = "fldc8kUYwodsQJvIy"  # Project name
F_WEBSITE     = "fldiaFhY8seaUpS6j"  # Website URL
F_GITHUB      = "fldp5H4hR4wDAc43i"  # Github URL
F_BLOG_1      = "fldd2Z5guupRDy19d"  # Blog feed URL
F_BLOG_2      = "fldsO2OQvw1mxqc6l"  # Blog feed (second field)
F_NEW_LAUNCH  = "fldDItqXavISnFFTy"  # New launch? (multilineText, "x" = new this year)
F_LAUNCH_DATE = "fldOymIEO9nuUItWv"  # Requested launch date
F_LINKS       = "flduR7cgq36S9SM4H"  # Linked records in Links table
F_TYPE        = "fld85Qsj7sU56liv9"  # Type (multipleSelects)
F_FORMATS     = "fldSJjJ4rbDBS8849"  # Formats (multipleRecordLinks → Format table)
F_CATEGORIES  = "fldXGB674po9h9xtB"  # Categories (multipleRecordLinks)

FORMAT_TABLE  = "tblDRByL15hmKr0sJ"
FORMAT_F_NAME = "fldSJg1rpaHBme1FE"

# Output fields
F_STATUS          = "fldw9vTztFwBOrcue"  # Status — existing field (Active / Inactive / N/A)
F_ACTIVITY_STATUS = "fld8cGHyyU0CffP2s"  # Activity status — full scale (created 2026-04-01)
F_LIVELINESS      = "fldbzhgtmZEjH7yaK"  # Liveliness score (created 2026-04-01)
F_LAST_ACTIVITY   = "fldeRTiBRsmhQJhxx"  # Last activity date (created 2026-04-01)
F_LAST_CHECK      = "fld8pjefvIqtFYxxO"  # Last timeliness check (created 2026-04-01)

# ── Airtable field IDs — Links table ─────────────────────────────────────────

FL_URL     = "fldfJ5N0rECMxNiw5"  # Link URL
FL_LISTING = "fldQ9ByIfzzFqvJcy"  # Parent Listing (linked records)
FL_TYPE    = "fldZD5TbZ8P2cp39U"  # Link type (singleSelect)

# ── Airtable REST helpers ─────────────────────────────────────────────────────

AT_BASE    = f"https://api.airtable.com/v0/{BASE_ID}"
AT_HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_PAT}",
    "Content-Type":  "application/json",
}


def at_get(table, params):
    params = {**params, "returnFieldsByFieldId": "true"}
    r = requests.get(f"{AT_BASE}/{table}", headers=AT_HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def at_get_record(table, record_id):
    r = requests.get(f"{AT_BASE}/{table}/{record_id}", headers=AT_HEADERS,
                     params={"returnFieldsByFieldId": "true"}, timeout=10)
    if r.status_code == 200:
        return r.json()
    return None


def at_patch(table, records):
    r = requests.patch(
        f"{AT_BASE}/{table}",
        headers=AT_HEADERS,
        json={"records": records},
        timeout=15,
    )
    r.raise_for_status()
    return r.json()


# ── Fetch batch ───────────────────────────────────────────────────────────────

SOURCE_FIELDS = [
    F_NAME, F_WEBSITE, F_GITHUB, F_BLOG_1, F_BLOG_2,
    F_NEW_LAUNCH, F_LAUNCH_DATE, F_LINKS,
    F_TYPE, F_FORMATS, F_CATEGORIES,
    F_LAST_CHECK,
]

CURRENT_YEAR = str(datetime.now().year)

_format_name_cache = None  # {record_id: name}


def get_format_names():
    """Fetch Format table once per run and return a {record_id: name} map."""
    global _format_name_cache
    if _format_name_cache is not None:
        return _format_name_cache
    try:
        data = at_get(FORMAT_TABLE, {"pageSize": 100})
        _format_name_cache = {
            rec["id"]: (rec.get("fields", {}).get(FORMAT_F_NAME) or "").strip().lower()
            for rec in data.get("records", [])
        }
    except Exception as e:
        print(f"  Warning: could not fetch Format table: {e}", file=sys.stderr)
        _format_name_cache = {}
    return _format_name_cache


def is_excluded(rec):
    """
    Returns True if the record should be skipped:
    - New launch this year (marked 'x')
    - Type contains 'document'
    - Formats contains 'books'
    """
    f = rec.get("fields", {})

    # New launch this year
    new_launch  = (f.get(F_NEW_LAUNCH) or "").strip().lower()
    launch_date = (f.get(F_LAUNCH_DATE) or "")
    if new_launch == "x" and str(launch_date).startswith(CURRENT_YEAR):
        return True

    # Type = document
    type_values = f.get(F_TYPE) or []
    if any("document" in str(t).lower() for t in type_values):
        return True

    # Format = books
    format_ids    = f.get(F_FORMATS) or []
    format_names  = get_format_names()
    if any(format_names.get(fid, "") == "books" for fid in format_ids):
        return True

    # No categories assigned
    if not f.get(F_CATEGORIES):
        return True

    return False


def fetch_batch():
    """
    Return the next BATCH_SIZE records to check.
    Priority: never-checked first (empty Last timeliness check),
              then oldest-checked first.
    Excludes new launches from the current year.
    """
    collected = []

    # Pass 1: never checked
    data = at_get(LISTINGS_TABLE, {
        "filterByFormula": f'{{{_field_name(F_LAST_CHECK)}}} = ""',
        "pageSize": BATCH_SIZE * 4,
    })
    collected.extend(data.get("records", []))

    # Pass 2: oldest checked (if we still need records)
    if len(collected) < BATCH_SIZE * 2:
        data2 = at_get(LISTINGS_TABLE, {
            "filterByFormula": f'{{{_field_name(F_LAST_CHECK)}}} != ""',
            f"sort[0][field]": F_LAST_CHECK,
            f"sort[0][direction]": "asc",
            "pageSize": BATCH_SIZE * 4,
        })
        collected.extend(data2.get("records", []))

    eligible = [r for r in collected if not is_excluded(r)]
    return eligible[:BATCH_SIZE]


# Field name cache (Airtable formula filters use field names, not IDs)
_FIELD_NAME_MAP = {
    F_LAST_CHECK: "Last timeliness check",
}

def _field_name(fid):
    return _FIELD_NAME_MAP.get(fid, fid)


# ── HTTP session ──────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "civictech.guide timeliness-checker/1.0 (+https://civictech.guide)"})


# ── URL classification ────────────────────────────────────────────────────────

# Domains that are definitely not a project homepage
_ARTICLE_DOMAINS = {
    "techcrunch.com", "theverge.com", "wired.com", "bloomberg.com",
    "reuters.com", "theguardian.com", "nytimes.com", "washingtonpost.com",
    "forbes.com", "businessinsider.com", "politico.com", "npr.org",
    "bbc.com", "bbc.co.uk", "apnews.com", "axios.com", "vice.com",
    "fastcompany.com", "engadget.com", "gizmodo.com", "arstechnica.com",
    "theatlantic.com", "vox.com", "slate.com", "salon.com",
    "govtech.com", "statescoop.com", "fedscoop.com", "nextgov.com",
    "citylab.com", "smartcitiesdive.com", "govinsider.asia",
}

# Domains and patterns we should skip entirely (no useful signal)
_SKIP_DOMAINS = {"bing.com", "google.com"}


def resolve_duckduckgo(url):
    """
    Follow a DuckDuckGo !ducky (I'm Feeling Ducky) URL to its final destination.
    Returns the resolved URL, or None if resolution failed or stayed on DuckDuckGo.
    """
    try:
        r = SESSION.get(url, timeout=10, allow_redirects=True)
        final = r.url
        if "duckduckgo.com" not in final:
            return final
    except Exception:
        pass
    return None


def classify_url(url):
    """
    Returns one of: "skip", "article", "homepage".
    "skip"     → placeholder / search engine URL, ignore completely.
    "article"  → known media domain; try to find real URL.
    "homepage" → treat as the project's actual website.
    Note: DuckDuckGo URLs are resolved before this is called.
    """
    if not url:
        return "skip"
    parsed = urlparse(url)
    netloc = parsed.netloc.lower().lstrip("www.")

    if netloc in _SKIP_DOMAINS:
        return "skip"

    if netloc in _ARTICLE_DOMAINS:
        return "article"

    return "homepage"


# ── Homepage discovery from article pages ─────────────────────────────────────

from html.parser import HTMLParser as _HTMLParser

class _LinkExtractor(_HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []  # list of (href, anchor_text)
        self._current_anchor = None

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            self._current_anchor = attrs_dict.get("href", "")

    def handle_data(self, data):
        if self._current_anchor is not None:
            self.links.append((self._current_anchor, data.strip()))

    def handle_endtag(self, tag):
        if tag == "a":
            self._current_anchor = None


_SKIP_LINK_DOMAINS = {
    "twitter.com", "x.com", "facebook.com", "instagram.com", "linkedin.com",
    "youtube.com", "tiktok.com", "google.com", "apple.com", "amazon.com",
    "duckduckgo.com", "bing.com",
}
_SKIP_LINK_DOMAINS |= _ARTICLE_DOMAINS


def find_homepage_in_article(article_url):
    """
    Fetch a news article and look for external links that are likely the
    project's actual homepage. Returns a URL string or None.
    """
    try:
        r = SESSION.get(article_url, timeout=12, allow_redirects=True)
        if r.status_code != 200:
            return None

        article_domain = urlparse(article_url).netloc.lower().lstrip("www.")
        parser = _LinkExtractor()
        parser.feed(r.text)

        candidates = []
        for href, text in parser.links:
            if not href.startswith("http"):
                continue
            parsed_link = urlparse(href)
            link_domain = parsed_link.netloc.lower().lstrip("www.")

            # Must be a different domain from the article
            if link_domain == article_domain:
                continue
            # Skip known non-project domains
            if link_domain in _SKIP_LINK_DOMAINS:
                continue
            # Skip very long paths (probably deep links, not homepages)
            path_depth = len([p for p in parsed_link.path.strip("/").split("/") if p])
            if path_depth > 3:
                continue

            # Prefer anchor text that suggests it's a project link
            text_lower = text.lower()
            priority = 0
            if any(w in text_lower for w in ("website", "site", "platform", "tool",
                                              "project", "learn more", "visit", "here")):
                priority = 2
            elif path_depth <= 1:
                priority = 1  # short path = likely homepage

            candidates.append((priority, href))

        # Sort by priority descending, then check if alive
        candidates.sort(key=lambda x: -x[0])
        seen = set()
        for _, href in candidates[:10]:
            domain = urlparse(href).netloc.lower()
            if domain in seen:
                continue
            seen.add(domain)
            if check_url_alive(href) is True:
                return href
    except Exception:
        pass
    return None


# ── Individual signal checks ──────────────────────────────────────────────────

def _extract_archive_original(url):
    """
    Extract the original URL from a web.archive.org link.
    e.g. https://web.archive.org/web/20180217125651/http://thesponge.eu/
    returns http://thesponge.eu/
    """
    m = re.search(r'web\.archive\.org/web/\d+[^/]*/(.+)', url)
    if m:
        return m.group(1)
    return None


def _try_fetch(url):
    """Attempt a GET and return (is_alive, exception_type)."""
    try:
        r = SESSION.get(url, timeout=10, allow_redirects=True, stream=True)
        r.close()
        return (200 <= r.status_code < 400), None
    except requests.exceptions.SSLError:
        return True, None   # broken SSL but site exists
    except requests.exceptions.Timeout:
        return None, "timeout"
    except requests.exceptions.TooManyRedirects:
        return None, "redirects"
    except Exception:
        return False, "error"


def check_website(url):
    """
    Returns (is_alive: bool | None, is_archived: bool).
    For web archive URLs, tries the original URL first; only treats as
    archived/dead if the original URL also fails.
    None means we couldn't determine (timeout / network error).
    """
    if not url:
        return None, False

    is_archive_url = "web.archive.org" in url or "waybackmachine.org" in url

    if is_archive_url:
        original = _extract_archive_original(url)
        if original:
            print(f"    website  → archive URL, trying original: {original[:60]}")
            alive, _ = _try_fetch(original)
            if alive is True:
                return True, False   # original site is live — not archived
            if alive is None:
                return None, True    # couldn't determine
        # Original is down or couldn't be extracted — genuinely archived
        return False, True

    return _try_fetch(url)[0], False


def check_github(url):
    """
    Returns pushed_at as a timezone-aware datetime, or None.
    Handles repo URLs (github.com/org/repo) and org/user profile URLs.
    """
    if not url:
        return None

    parsed     = urlparse(url)
    if "github.com" not in parsed.netloc:
        return None

    parts = [p for p in parsed.path.strip("/").split("/") if p]

    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    try:
        if len(parts) >= 2:
            owner, repo = parts[0], re.sub(r"\.git$", "", parts[1])
            r = requests.get(
                f"https://api.github.com/repos/{owner}/{repo}",
                headers=headers, timeout=10,
            )
            if r.status_code == 200:
                pushed = r.json().get("pushed_at")
                if pushed:
                    return datetime.fromisoformat(pushed.replace("Z", "+00:00"))

        elif len(parts) == 1:
            owner = parts[0]
            for entity in ("orgs", "users"):
                r = requests.get(
                    f"https://api.github.com/{entity}/{owner}/repos",
                    headers=headers,
                    params={"sort": "pushed", "per_page": 1},
                    timeout=10,
                )
                if r.status_code == 200 and r.json():
                    pushed = r.json()[0].get("pushed_at")
                    if pushed:
                        return datetime.fromisoformat(pushed.replace("Z", "+00:00"))
                    break
    except Exception:
        pass

    return None


def check_blog_feed(url):
    """Returns most recent post date as a timezone-aware datetime, or None."""
    return _parse_feed_latest(url) if url else None


def check_url_alive(url):
    """Quick HEAD check. Returns True / False / None (error)."""
    if not url:
        return None
    try:
        r = SESSION.head(url, timeout=5, allow_redirects=True)
        return 200 <= r.status_code < 400
    except Exception:
        return None


def fetch_links_for_record(linked_ids):
    """
    Fetch URL and type from the Links table for the given linked record IDs.
    Returns list of {"url": str, "type": str}. Caps at 8 records to stay fast.
    """
    results = []
    for rid in linked_ids[:8]:
        rec = at_get_record(LINKS_TABLE, rid)
        if rec:
            f = rec.get("fields", {})
            link_url  = f.get(FL_URL)
            link_type = f.get(FL_TYPE, {})
            if isinstance(link_type, dict):
                link_type = link_type.get("name", "")
            if link_url:
                results.append({"url": link_url, "type": link_type or ""})
        time.sleep(0.1)
    return results


# ── Social recency checkers ───────────────────────────────────────────────────

def _parse_feed_latest(url, extra_headers=None):
    """Fetch a feed URL and return the most recent entry datetime, or None."""
    if not HAS_FEEDPARSER:
        return None
    try:
        headers = {"User-Agent": "civictech.guide timeliness-checker/1.0"}
        if extra_headers:
            headers.update(extra_headers)
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return None
        feed   = feedparser.parse(r.content)
        latest = None
        for entry in feed.entries:
            for attr in ("published_parsed", "updated_parsed"):
                t = getattr(entry, attr, None)
                if t:
                    dt = datetime(*t[:6], tzinfo=timezone.utc)
                    if latest is None or dt > latest:
                        latest = dt
        return latest
    except Exception:
        return None


def _check_youtube(parsed):
    """Return most recent video date from a YouTube channel, or None."""
    if not YOUTUBE_API_KEY:
        return None
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if not parts:
        return None

    try:
        # Determine how to look up the channel
        if parts[0] == "channel" and len(parts) >= 2:
            chan_params = {"id": parts[1], "part": "contentDetails", "key": YOUTUBE_API_KEY}
        elif parts[0].startswith("@"):
            chan_params = {"forHandle": parts[0][1:], "part": "contentDetails", "key": YOUTUBE_API_KEY}
        elif parts[0] in ("c", "user") and len(parts) >= 2:
            chan_params = {"forUsername": parts[1], "part": "contentDetails", "key": YOUTUBE_API_KEY}
        else:
            return None

        r = requests.get("https://www.googleapis.com/youtube/v3/channels",
                         params=chan_params, timeout=10)
        if r.status_code != 200:
            return None
        items = r.json().get("items", [])
        if not items:
            return None

        uploads_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

        r2 = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",
                          params={"playlistId": uploads_id, "part": "snippet",
                                  "maxResults": 1, "key": YOUTUBE_API_KEY},
                          timeout=10)
        if r2.status_code != 200:
            return None
        items2 = r2.json().get("items", [])
        if not items2:
            return None
        pub = items2[0]["snippet"].get("publishedAt")
        if pub:
            return datetime.fromisoformat(pub.replace("Z", "+00:00"))
    except Exception:
        pass
    return None


def _check_bluesky(path):
    """Return most recent post date from a Bluesky profile, or None."""
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) < 2 or parts[0] != "profile":
        return None
    actor = parts[1]
    try:
        r = requests.get(
            "https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed",
            params={"actor": actor, "limit": 1},
            timeout=10,
        )
        if r.status_code == 200:
            feed = r.json().get("feed", [])
            if feed:
                indexed_at = feed[0].get("post", {}).get("indexedAt")
                if indexed_at:
                    return datetime.fromisoformat(indexed_at.replace("Z", "+00:00"))
    except Exception:
        pass
    return None


def _check_medium(url, parsed):
    """Return most recent post date from a Medium profile/publication, or None."""
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if parts and parts[0].startswith("@"):
        feed_url = f"https://medium.com/feed/{parts[0]}"
    elif parsed.netloc != "medium.com":
        feed_url = f"{parsed.scheme}://{parsed.netloc}/feed"
    else:
        feed_url = f"https://medium.com/feed/{parsed.path.strip('/')}"
    return _parse_feed_latest(feed_url)


def _check_reddit(parsed):
    """Return most recent post date from a Reddit subreddit or user page, or None."""
    clean_path = parsed.path.rstrip("/")
    feed_url   = f"https://www.reddit.com{clean_path}.rss"
    return _parse_feed_latest(feed_url)


def _check_substack(parsed):
    """Return most recent post date from a Substack newsletter, or None."""
    feed_url = f"{parsed.scheme}://{parsed.netloc}/feed"
    return _parse_feed_latest(feed_url)


def _check_fediverse(url):
    """Return most recent post date from a Mastodon/fediverse profile, or None."""
    feed_url = url.rstrip("/") + ".rss"
    return _parse_feed_latest(feed_url)


# Domains where we know RSS / API isn't available (check alive only)
_NO_DATE_DOMAINS = {
    "twitter.com", "x.com", "facebook.com", "instagram.com",
    "linkedin.com", "tiktok.com", "signal.org",
}

# Regex for fediverse-style /@username paths
_FEDIVERSE_PATH = re.compile(r"/+@[^/]+/?$")


def check_social_recency(url):
    """
    Try to retrieve the most recent post date from a social URL.
    Returns (datetime | None, platform_label: str | None).
    """
    if not url:
        return None, None

    parsed = urlparse(url)
    netloc = parsed.netloc.lower().lstrip("www.")

    if "youtube.com" in netloc or "youtu.be" in netloc:
        return _check_youtube(parsed), "YouTube"

    if "bsky.app" in netloc:
        return _check_bluesky(parsed.path), "Bluesky"

    if "medium.com" in netloc or netloc.endswith(".medium.com"):
        return _check_medium(url, parsed), "Medium"

    if "reddit.com" in netloc:
        return _check_reddit(parsed), "Reddit"

    if "substack.com" in netloc:
        return _check_substack(parsed), "Substack"

    # Fediverse: any /@username URL on a domain not in the known-no-date list
    if netloc not in _NO_DATE_DOMAINS and _FEDIVERSE_PATH.search(parsed.path):
        return _check_fediverse(url), "Fediverse"

    return None, None


# ── Scoring ───────────────────────────────────────────────────────────────────

def recency_base_score(dt, now):
    """
    Score 0–85 for GitHub / blog signals based on recency.
    Primary tunable parameter of the algorithm.
    """
    if dt is None:
        return 0
    age_days = max(0, (now - dt).days)
    if age_days <= 90:   return 85
    if age_days <= 180:  return 80
    if age_days <= 365:  return 70
    if age_days <= 730:  return 55
    if age_days <= 1095: return 35
    if age_days <= 1825: return 15
    return 5


def social_recency_score(dt, now):
    """
    Score 0–55 for social media signals. Same time brackets as recency_base_score
    but capped at 55 and zeroed beyond 1 year.
    """
    if dt is None:
        return 0
    age_days = max(0, (now - dt).days)
    if age_days <= 90:  return 55
    if age_days <= 180: return 50
    if age_days <= 365: return 45
    return 0  # beyond 1 year: no social score


def score_to_activity_status(score):
    """Full 5-value scale for the Activity status field."""
    if score >= 70: return "Active"
    if score >= 45: return "Likely Active"
    if score >= 20: return "Possibly Inactive"
    return "Inactive"


def score_to_status(score):
    """
    Definitive write to the existing Status field.
    Only returns a value when confidence is high; None = don't touch Status.
    """
    if score >= 70: return "Active"
    if score < 20:  return "Inactive"
    return None


# ── Main computation ──────────────────────────────────────────────────────────

def compute_liveliness(rec):
    """
    Runs all signal checks and returns:
      score (float), last_activity_date (ISO str | None), status (str)
    """
    fields = rec.get("fields", {})
    name   = fields.get(F_NAME, rec["id"])
    now    = datetime.now(timezone.utc)

    print(f"\n  [{name}]")

    # ── Website ──────────────────────────────────────────────────────────────
    raw_url = fields.get(F_WEBSITE)
    if raw_url and "duckduckgo.com" in raw_url and "!ducky" in raw_url:
        resolved = resolve_duckduckgo(raw_url)
        if resolved:
            print(f"    website  → resolved DDG URL to: {resolved[:70]}")
            raw_url = resolved
        else:
            print(f"    website  → DDG URL could not be resolved, skipping")
            raw_url = None
    url_class  = classify_url(raw_url)
    website_url = raw_url
    discovered_url = None

    if url_class == "skip":
        print(f"    website  → no usable URL ({(raw_url or 'none')[:60]})")
        website_alive, is_archived = None, False
    elif url_class == "article":
        print(f"    website  → article URL detected, searching for homepage…")
        discovered_url = find_homepage_in_article(raw_url)
        if discovered_url:
            print(f"    website  → found homepage: {discovered_url}")
            website_url = discovered_url
            website_alive, is_archived = check_website(discovered_url)
        else:
            print(f"    website  → could not find homepage from article")
            website_alive, is_archived = None, False
    else:
        website_alive, is_archived = check_website(raw_url)
        print(f"    website  → alive={website_alive}  archived={is_archived}")

    # ── GitHub ───────────────────────────────────────────────────────────────
    github_date = check_github(fields.get(F_GITHUB))
    print(f"    github   → last push: {github_date}")
    time.sleep(0.3)  # respect GitHub rate limits

    # ── Blog feeds ───────────────────────────────────────────────────────────
    blog_date = None
    for feed_url in filter(None, [fields.get(F_BLOG_1), fields.get(F_BLOG_2)]):
        d = check_blog_feed(feed_url)
        if d and (blog_date is None or d > blog_date):
            blog_date = d
    print(f"    blog     → latest post: {blog_date}")

    # ── Social links: recency + accessibility (from Links table only) ───────────
    linked_ids = [r if isinstance(r, str) else r.get("id") for r in (fields.get(F_LINKS) or [])]
    link_items = fetch_links_for_record(linked_ids) if linked_ids else []
    all_social_urls = [item["url"] for item in link_items]
    print(f"    links    → {len(link_items)} records fetched")

    accessible_count  = 0
    social_dated      = []  # list of (datetime, platform_label)

    for url in all_social_urls:
        alive = check_url_alive(url)
        if alive is True:
            accessible_count += 1
        dt, platform = check_social_recency(url)
        if dt:
            social_dated.append((dt, platform))
            print(f"    social   → {platform}: last post {dt.date()}")

    accessible_count = min(accessible_count, 5)  # cap alive bonus

    # ── Combine all signals ───────────────────────────────────────────────────
    best_score = 0
    best_date  = None  # most recent date found across all sources

    for dt in filter(None, [github_date, blog_date]):
        s = recency_base_score(dt, now)
        if s > best_score:
            best_score = s
        if best_date is None or dt > best_date:
            best_date = dt

    for dt, platform in social_dated:
        s = social_recency_score(dt, now)
        if s > best_score:
            best_score = s
        if best_date is None or dt > best_date:
            best_date = dt

    score = float(best_score)

    # Website modifiers
    if is_archived:
        score = min(score, 10)      # almost certainly dead if pointing to web archive
    elif website_alive is True:
        score = min(score + 15, 100)
    elif website_alive is False:
        score = max(score - 50, 0)  # strong signal of death

    # Social presence bonus: +1 per accessible link, up to +5
    score = min(score + accessible_count * 1, 100)

    # Floor: website up but no dated signals → benefit of the doubt
    if best_date is None and website_alive is True and not is_archived:
        score = max(score, 25)

    # Fully unknown: nothing could be checked at all
    no_signals = (best_date is None and website_alive is None and accessible_count == 0)

    score  = round(score, 1)
    activity_status = "Unknown" if no_signals else score_to_activity_status(score)
    status          = None      if no_signals else score_to_status(score)

    last_activity_str = best_date.strftime("%Y-%m-%d") if best_date else None
    print(f"    → score={score}  activity={activity_status}  status={status or '(no change)'}  last_activity={last_activity_str}")
    if discovered_url:
        print(f"    → discovered homepage: {discovered_url}  (original was article URL)")

    return {
        "score":              score,
        "last_activity_date": last_activity_str,
        "activity_status":    activity_status,
        "status":             status,
        "discovered_url":     discovered_url,
    }


# ── Entry point ───────────────────────────────────────────────────────────────

def fetch_by_ids(record_ids):
    """Fetch specific records by ID (for targeted test runs)."""
    data = at_get(LISTINGS_TABLE, {
        "recordIds[]": record_ids,
    })
    return data.get("records", [])


def main():
    import argparse
    parser = argparse.ArgumentParser(description="CTFG timeliness checker")
    parser.add_argument("--records", nargs="+", metavar="recXXX",
                        help="Specific record IDs to check (skips normal batch queue)")
    args = parser.parse_args()

    if args.records:
        print(f"Fetching {len(args.records)} specified records...")
        records = fetch_by_ids(args.records)
    else:
        print(f"Fetching next {BATCH_SIZE} projects to check...")
        records = fetch_batch()
    if not records:
        print("No eligible records found.")
        return

    print(f"Got {len(records)} records.\n")
    today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    updates = []

    for rec in records:
        result = compute_liveliness(rec)
        update = {
            "id": rec["id"],
            "fields": {
                F_LIVELINESS:      result["score"],
                F_ACTIVITY_STATUS: result["activity_status"],
                F_LAST_CHECK:      today,
            },
        }
        if result["status"]:                    # only when score is definitively high or low
            update["fields"][F_STATUS] = result["status"]
        if result["last_activity_date"]:
            update["fields"][F_LAST_ACTIVITY] = result["last_activity_date"]
        update["_discovered_url"] = result.get("discovered_url")  # stored locally, not sent to Airtable
        updates.append(update)
        time.sleep(0.5)

    print(f"\nWriting {len(updates)} results to Airtable...")
    for i in range(0, len(updates), 10):
        # Strip local-only keys before sending to Airtable
        chunk = [{"id": u["id"], "fields": u["fields"]} for u in updates[i : i + 10]]
        at_patch(LISTINGS_TABLE, chunk)
        print(f"  Updated records {i+1}–{i+len(chunk)}")
        time.sleep(0.2)

    print("\n✓ Done.\n")
    print(f"{'Record ID':<20} {'Score':>7}  {'Activity status':<20}  {'Status':<10}  Last activity")
    print("-" * 80)
    for u in updates:
        f    = u["fields"]
        disc = u.get("_discovered_url") or ""
        print(
            f"{u['id']:<20} "
            f"{str(f[F_LIVELINESS]):>7}  "
            f"{f.get(F_ACTIVITY_STATUS, ''):<20}  "
            f"{f.get(F_STATUS, '(no change)'):<10}  "
            f"{f.get(F_LAST_ACTIVITY, 'n/a')}"
            + (f"  → {disc}" if disc else "")
        )


if __name__ == "__main__":
    main()
