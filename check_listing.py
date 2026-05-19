#!/usr/bin/env python3
"""Timeliness check for Amandla Mobi (recwTaePISopZifF2)."""

import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import re

TIMEOUT = 10
TODAY = "2026-05-19"

HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
           "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

def fetch(url, method="GET"):
    try:
        r = requests.request(method, url, timeout=TIMEOUT,
                             allow_redirects=True, headers=HEADERS)
        return r
    except Exception as e:
        print(f"    FAIL {method} {url}: {e}")
        return None

def parse_date(s):
    if not s:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S",
                "%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S %Z",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass
    try:
        return parsedate_to_datetime(s)
    except Exception:
        pass
    m = re.search(r'(\d{4}-\d{2}-\d{2})', s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None

def days_ago(dt):
    if dt is None:
        return None
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt).days

def extract_dates_from_html(html_text):
    """Return the most recent plausible date found in HTML."""
    dates = []
    for m in re.finditer(r'(\d{4}-\d{2}-\d{2})', html_text):
        dt = parse_date(m.group(1))
        if dt and 2018 <= dt.year <= 2026:
            dates.append(dt)
    months_re = r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    for m in re.finditer(rf'\b(\d{{1,2}})\s+{months_re}\s+(\d{{4}})', html_text):
        try:
            day, mon, year = m.group(1), m.group(2), m.group(3)
            s = f"{day} {mon} {year}"
            for fmt in ("%d %B %Y", "%d %b %Y"):
                try:
                    dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                    if 2018 <= dt.year <= 2026:
                        dates.append(dt)
                    break
                except ValueError:
                    pass
        except Exception:
            pass
    for m in re.finditer(rf'\b{months_re}\s+(\d{{1,2}}),?\s+(\d{{4}})', html_text):
        try:
            mon, day, year = m.group(1), m.group(2), m.group(3)
            s = f"{mon} {day} {year}"
            for fmt in ("%B %d %Y", "%b %d %Y"):
                try:
                    dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                    if 2018 <= dt.year <= 2026:
                        dates.append(dt)
                    break
                except ValueError:
                    pass
        except Exception:
            pass
    return max(dates) if dates else None

def try_parse_feed(r):
    """Try to parse response as RSS/Atom; return latest entry datetime or None."""
    content = r.content
    if not content:
        return None
    text_start = content[:200].decode("utf-8", errors="ignore").strip()
    if not text_start.startswith("<"):
        return None
    try:
        root = ET.fromstring(content)
        dates = []
        for item in root.iter("item"):
            pub = (item.findtext("pubDate") or
                   item.findtext("{http://purl.org/dc/elements/1.1/}date"))
            if pub:
                dt = parse_date(pub)
                if dt:
                    dates.append(dt)
        for entry in root.iter("{http://www.w3.org/2005/Atom}entry"):
            updated = (entry.findtext("{http://www.w3.org/2005/Atom}updated") or
                       entry.findtext("{http://www.w3.org/2005/Atom}published"))
            if updated:
                dt = parse_date(updated)
                if dt:
                    dates.append(dt)
        return max(dates) if dates else None
    except ET.ParseError:
        return None

results = {}  # label -> datetime

# ── 1. Website ─────────────────────────────────────────────────────────────────
print("\n=== 1. Website check: https://www.amandla.mobi/ ===")
r = fetch("https://www.amandla.mobi/")
if r is not None:
    print(f"  Status: {r.status_code}")
    lm = r.headers.get("Last-Modified") or r.headers.get("last-modified")
    if lm:
        dt = parse_date(lm)
        print(f"  Last-Modified header: {lm} → {dt}")
        if dt:
            results["website Last-Modified"] = dt
    if r.status_code == 200:
        dt = extract_dates_from_html(r.text)
        if dt:
            print(f"  Latest date in HTML: {dt.strftime('%Y-%m-%d')}")
            results["website_html"] = dt
        else:
            print("  No date found in HTML.")
    else:
        print("  Not 200 — skipping HTML scrape.")
else:
    print("  Could not reach website.")

# ── 2. GitHub ──────────────────────────────────────────────────────────────────
print("\n=== 2. GitHub: none ===")

# ── 3. Blog feeds ─────────────────────────────────────────────────────────────
print("\n=== 3. Blog feed: https://www.amandla.mobi/blog ===")
feed_candidates = [
    "https://www.amandla.mobi/blog",
    "https://www.amandla.mobi/feed",
    "https://www.amandla.mobi/feed.xml",
    "https://www.amandla.mobi/rss.xml",
    "https://www.amandla.mobi/blog/feed",
]
for url in feed_candidates:
    r = fetch(url)
    if r is None:
        continue
    ct = r.headers.get("content-type", "?")[:60]
    print(f"  {url} → {r.status_code} [{ct}]")
    if r.status_code == 200:
        dt = try_parse_feed(r)
        if dt:
            print(f"    Feed entry date: {dt.strftime('%Y-%m-%d')}")
            results["blog_feed"] = dt
            break
        dt = extract_dates_from_html(r.text)
        if dt:
            print(f"    HTML date: {dt.strftime('%Y-%m-%d')}")
            results.setdefault("blog_html", dt)
            if dt > results.get("blog_html", dt):
                results["blog_html"] = dt
            break

# ── 4. Social / linked URLs ────────────────────────────────────────────────────
print("\n=== 4. Social links (alive-check only) ===")
social_alive = [
    ("Twitter/X", "https://twitter.com/amandlamobi"),
    ("Facebook",  "https://www.facebook.com/amandla.mobi/"),
]
for label, url in social_alive:
    r = fetch(url, method="HEAD")
    if r is None:
        r = fetch(url)
    if r is not None:
        alive = r.status_code in (200, 301, 302, 403, 429)
        print(f"  {label}: HTTP {r.status_code} → {'ALIVE' if alive else 'possibly dead'}")
    else:
        print(f"  {label}: unreachable")

# ── 5. YouTube ─────────────────────────────────────────────────────────────────
print("\n=== 5. YouTube: none ===")

# ── Final calculation ─────────────────────────────────────────────────────────
print("\n=== Results ===")
for label, dt in sorted(results.items(), key=lambda x: x[1], reverse=True):
    d = days_ago(dt)
    print(f"  {label}: {dt.strftime('%Y-%m-%d')} ({d} days ago)")

if results:
    most_recent_dt = max(results.values())
    most_recent_label = max(results, key=lambda k: results[k])
    d = days_ago(most_recent_dt)
    print(f"\nMost recent: {most_recent_dt.strftime('%Y-%m-%d')} ({d} days ago) [{most_recent_label}]")
    if d <= 30:
        score = 90
    elif d <= 90:
        score = 70
    elif d <= 180:
        score = 50
    elif d <= 365:
        score = 30
    else:
        score = 10
    last_activity_date = most_recent_dt.strftime('%Y-%m-%d')
else:
    d = None
    score = 0
    last_activity_date = None
    print("\nNo parseable activity dates found across all signals.")

if score >= 70:
    status = "Active"
elif score >= 40:
    status = "Likely Active"
elif score >= 20:
    status = "Possibly Inactive"
elif score >= 1:
    status = "Inactive"
else:
    status = "Unknown"

print(f"\nLiveliness score:   {score}")
print(f"Activity status:    {status}")
print(f"Last activity date: {last_activity_date or 'N/A'}")
print(f"Check date:         {TODAY}")
