import os
os.system("cls" if os.name == "nt" else "clear")
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright
from google.oauth2.service_account import Credentials
import gspread
from gspread_formatting import (
    CellFormat, TextFormat, Border, Borders, format_cell_range,
    set_column_width, batch_updater, Color
)

# =========================
# === CONFIG (editable) ===
# =========================
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "credentials.json"

HEADLESS = os.environ.get("HEADLESS", "1") == "1"
FORCE_WEEK_TABLE = os.environ.get("FORCE_WEEK_TABLE", "1") == "1"  # ← default ON (Step 14)
TIMEZONE = "America/Detroit"

PHASE_CFB = "regular"     # regular | bowls
PHASE_NFL = "regular"     # regular | playoffs
COLLEGE_INCLUDE_ALL = False

# Week flow toggles for this run
include_nfl = True
include_college = True

TEST_MODE_IGNORE_LOCKS = False  # set to False in real runs

# --- Optional week targeting (kept for manual overrides; mapping wins if FORCE_WEEK_TABLE=1) ---
NFL_YEAR = None
NFL_WEEK = None
CFB_YEAR = None
CFB_WEEK = None

A1_RANGE_RE = re.compile(r"^(?:[^!]+!)?([A-Z]+)(\d+):([A-Z]+)(\d+)$")

SPACER_ROWS_BETWEEN_LEAGUES = 4

# =========================
# === NFL WEEK MAPPING  ===
# =========================
# Step 7: Deterministic Tue→Tue window for 2025 regular season.
# Week 1 starts Tue 2025-09-02 00:00 America/Detroit. Each week is 7 days.
# If you need 18 weeks (typical regular season), set REG_WEEKS=18.
REG_SEASON_START_LOCAL = datetime(2025, 9, 2, 0, 0, tzinfo=ZoneInfo(TIMEZONE))
REG_WEEKS = 18

def _build_week_table(start_dt: datetime, weeks: int):
    table = []
    for wk in range(1, weeks + 1):
        s = start_dt + timedelta(days=(wk - 1) * 7)
        e = s + timedelta(days=7)
        # Season year tagging rule: Sep–Dec -> same year; Jan–Aug -> previous year
        season_year = s.year if s.month >= 9 else (s.year - 1)
        table.append({
            "year": season_year,
            "week": wk,
            "window_start": s,    # inclusive
            "window_end": e       # exclusive
        })
    return table

NFL_WEEK_TABLE_2025 = _build_week_table(REG_SEASON_START_LOCAL, REG_WEEKS)

def get_nfl_week_from_table(now_dt: datetime):
    """
    Return (year, week, start_dt, end_dt) whose window contains now_dt.
    If not inside any window, choose the nearest window by date (robustness across edges).
    """
    for rec in NFL_WEEK_TABLE_2025:
        if rec["window_start"] <= now_dt < rec["window_end"]:
            return rec["year"], rec["week"], rec["window_start"], rec["window_end"]
    # Outside windows: pick closest by absolute distance to window start
    closest = min(NFL_WEEK_TABLE_2025, key=lambda r: abs((r["window_start"] - now_dt).total_seconds()))
    return closest["year"], closest["week"], closest["window_start"], closest["window_end"]

def get_week_index_from_table(now_dt: datetime) -> int:
    """Return the deterministic week index (1..REG_WEEKS) based on the same table."""
    _, wk, _, _ = get_nfl_week_from_table(now_dt)
    return wk

def week_tag_from_table(league: str, now_dt: datetime) -> str:
    """
    Return YYYY-NFL-WkN or YYYY-CFB-WkN using the deterministic table.
    CFB shares the same week index and season-year as NFL for purge lockstep (Step 12).
    """
    year, wk, _, _ = get_nfl_week_from_table(now_dt)
    if league == "nfl":
        return f"{year}-NFL-Wk{wk}"
    else:
        return f"{year}-CFB-Wk{wk}"

def _strip_rank(name: str) -> str:
    # remove leading numeric rank like "12 Ohio State" -> "Ohio State"
    return re.sub(r"^\s*\d{1,2}\s+", "", (name or "")).strip().upper()

# =========================
# === NORMALIZATION ===
# =========================
def _a1_last_row(a1_range: str) -> int:
    m = A1_RANGE_RE.match(a1_range)
    if not m:
        raise ValueError(f"Unrecognized A1 range: {a1_range}")
    return int(m.group(4))

def _a1_first_row(a1_range: str) -> int:
    m = A1_RANGE_RE.match(a1_range)
    if not m:
        raise ValueError(f"Unrecognized A1 range: {a1_range}")
    return int(m.group(2))

def _normalize_pair_alignment(queued_ranges):
    adjusted = []
    for item in queued_ranges:
        a1 = item["range"]
        m = A1_RANGE_RE.match(a1)
        if not m:
            raise ValueError(f"Unrecognized A1 range: {a1}")
        col1, r1, col2, r2 = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        if r1 % 2 == 1:
            r1 += 1
            r2 += 1
        prefix = ""
        if "!" in a1:
            prefix = a1.split("!", 1)[0] + "!"
        new_range = f"{prefix}{col1}{r1}:{col2}{r2}"
        adjusted.append({"range": new_range, "values": item["values"]})
    return adjusted

def compute_max_row_needed(queued_ranges) -> int:
    return max(_a1_last_row(item["range"]) for item in queued_ranges) if queued_ranges else 0

def normalize_rows_to_AH(rows):
    """
    Ensure every row is exactly 8 columns matching:
    [Logo, Team, Pick#, Line, Pick#, O/U, Date, Time]
    """
    norm = []
    for r in rows:
        if r is None:
            continue
        r = list(r)
        while len(r) > 8 and (r[-1] is None or str(r[-1]).strip() == ""):
            r.pop()
        if len(r) == 8:
            norm.append([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]])
        elif len(r) == 6:
            logo, team, date_text, time_text, line, ou = r
            norm.append([logo, team, "", line, "", ou, date_text, time_text])
        else:
            if len(r) >= 6 and ("AM" in str(r[3]).upper() or "PM" in str(r[3]).upper()):
                logo, team, date_text, time_text, line, ou = r[:6]
                norm.append([logo, team, "", line, "", ou, date_text, time_text])
            else:
                r = (r + [""] * 8)[:8]
                norm.append(r)
    return norm

# =========================
# === TEAM MAPS (NFL)   ===
# =========================
TEAM_ABBR = {
    "Arizona": "ARI", "Atlanta": "ATL", "Baltimore": "BAL", "Buffalo": "BUF", "Carolina": "CAR",
    "Chicago": "CHI", "Cincinnati": "CIN", "Cleveland": "CLE", "Dallas": "DAL", "Denver": "DEN",
    "Detroit": "DET", "Green Bay": "GB", "Houston": "HOU", "Indianapolis": "IND", "Jacksonville": "JAX",
    "Kansas City": "KC", "Las Vegas": "LV", "Los Angeles Rams": "LAR", "Los Angeles Chargers": "LAC",
    "Miami": "MIA", "Minnesota": "MIN", "New England": "NE", "New Orleans": "NO",
    "New York Giants": "NYG", "New York Jets": "NYJ",
    "Philadelphia": "PHI", "Pittsburgh": "PIT", "San Francisco": "SF", "Seattle": "SEA",
    "Tampa Bay": "TB", "Tennessee": "TEN", "Washington": "WSH"
}

TEAM_LOGO_URLS = {
    "ARI": "https://drive.google.com/uc?export=view&id=1G8grwM4nTcvbANf_kGr-q3MLn6_OkxnD",
    "ATL": "https://drive.google.com/uc?export=view&id=1kSlPBJm5Xr5FfkyF9MsPP0ILMr0ScVxL",
    "BAL": "https://drive.google.com/uc?export=view&id=1KsRbiCLzrRCnPUMmbdwcIjseIg0riYga",
    "BUF": "https://drive.google.com/uc?export=view&id=1EXkNcY92v2EKfaLLXxcGSX1BPGzPBh5w",
    "CAR": "https://drive.google.com/uc?export=view&id=1eOet_WJPQOCMlKkdQq63o_TrHX9pyNHz",
    "CHI": "https://drive.google.com/uc?export=view&id=1oTMQ3Cb5Et1MsYPt_aHuljX3wkriioek",
    "CIN": "https://drive.google.com/uc?export=view&id=1pXBlGEoDjHhzGVIFhECYumTxEJbPZeg2",
    "CLE": "https://drive.google.com/uc?export=view&id=1M-W_fLSAcGMLsZnQ4vbVdDp017lYAfQd",
    "DAL": "https://drive.google.com/uc?export=view&id=1Y9igMt8oIzqgDxh6XzI8qREedekcb1dx",
    "DEN": "https://drive.google.com/uc?export=view&id=1e0nvFa5RzHSgk-4HeoIgKCiYc2SEnRj9",
    "DET": "https://drive.google.com/uc?export=view&id=1KV4ou_YQUPTOUaFq9Ds6E65L_KFm3RAt",
    "GB": "https://drive.google.com/uc?export=view&id=1_hNMK-WHLGsDVNOq3MwfaKj5OAspvCbU",
    "HOU": "https://drive.google.com/uc?export=view&id=1U-1g66IUNBIu3m3YUBawICyGxDKnif-B",
    "IND": "https://drive.google.com/uc?export=view&id=1TR4Yo8dRvuzBimQaFK8oqg4xEbdMF9DT",
    "JAX": "https://drive.google.com/uc?export=view&id=12KHClgM0p39w3K5REl9dEKz8Kll8cOI1",
    "KC": "https://drive.google.com/uc?export=view&id=1oO5qOWW_O2yUwYV0JOKBABFMtaRD_7kX",
    "LAC": "https://drive.google.com/uc?export=view&id=19NpiFd5ZEE9eP3zqLfESJEjhM-99SMGP",
    "LAR": "https://drive.google.com/uc?export=view&id=1kswDxvmH-uQDXDKrLI9-nDYEqGdq7pIU",
    "LV": "https://drive.google.com/uc?export=view&id=1Y1a4QzAlc1enj_6EkBWUyKOtRPPGD6i-",
    "MIA": "https://drive.google.com/uc?export=view&id=1MRjwQEftAnevP39H83zHWtvYAxg0hAZ2",
    "MIN": "https://drive.google.com/uc?export=view&id=1F4p_Dkxzb2Z7FmJVkrfXPMhtPebz9xkD",
    "NE": "https://drive.google.com/uc?export=view&id=1SKVXhYlP7aRHPpl_gaqUwHrlXKItF2RE",
    "NO": "https://drive.google.com/uc?export=view&id=1o-9zrST5FFng9lnRaVonCQF8l6x-6B2q",
    "NYG": "https://drive.google.com/uc?export=view&id=1Fkq_DkTsyh4-8Qp-VgI1owP8ba4RUs4c",
    "NYJ": "https://drive.google.com/uc?export=view&id=1-XUFsIR6jktnXoEaCBMftirYxyVvO6NM",
    "PHI": "https://drive.google.com/uc?export=view&id=13rDw-O7XjrTnBh9sV8uzAZMsY7_zM6PB",
    "PIT": "https://drive.google.com/uc?export=view&id=1V2h1B1EnDtvgRZ5lmG1PZFQjsdfA6ldg",
    "SEA": "https://drive.google.com/uc?export=view&id=1QPfZ48n-q3XBiXH7Ho1MVIgwofmdiqRb",
    "SF": "https://drive.google.com/uc?export=view&id=1-on8faSU5D80_lzG_HFJD_BdymbkBvbS",
    "TB": "https://drive.google.com/uc?export=view&id=1tBdvan59Vm4UUqcEo3aSFNh8xYp5lbjH",
    "TEN": "https://drive.google.com/uc?export=view&id=1QQBOdLz4xme7yo0osa_JveEuInmCDkNE",
    "WSH": "https://drive.google.com/uc?export=view&id=1DBkizXYBC-w7gc1tvf8dBLIcGZuOI3R2"
}

# === COLLEGE LOGO/ABBR (loaded from your Google Sheet) ===
def build_college_logo_dict(sheet_id="1dh-IaArNHJ8UeqhZf93iPR4HpBgnr-8tmK1FhtpXj-4"):
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1
    data = sheet.get_all_values()
    logo_dict = {}
    for row in data[1:]:
        if len(row) >= 6:
            team_name = row[1].strip()
            logo_url = row[5].strip()
            if team_name and logo_url:
                logo_dict[team_name] = logo_url
    return logo_dict

def build_college_abbreviation_dict(sheet_id="1dh-IaArNHJ8UeqhZf93iPR4HpBgnr-8tmK1FhtpXj-4"):
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).sheet1
    data = sheet.get_all_values()
    abbr_dict = {}
    for row in data[1:]:
        if len(row) >= 3:
            team_name = row[1].strip()
            abbr = row[2].strip()
            if team_name and abbr:
                abbr_dict[team_name] = abbr.upper()
    return abbr_dict

college_logo_urls = build_college_logo_dict()
college_abbreviation_dict = build_college_abbreviation_dict()

# =========================
# === LOGO / ABBR HELP  ===
# =========================
def get_logo_formula(team_name, logo_url="", league="nfl"):
    if league == "nfl":
        abbr = find_abbreviation(team_name, logo_url)
        if abbr and abbr in TEAM_LOGO_URLS:
            return f'=IMAGE("{TEAM_LOGO_URLS[abbr]}", 1)'
    elif league == "college":
        if team_name in college_logo_urls:
            return f'=IMAGE("{college_logo_urls[team_name]}", 1)'
        normalized_team = re.sub(r"[^\w]", "", team_name).lower()
        for key, url in college_logo_urls.items():
            normalized_key = re.sub(r"[^\w]", "", key).lower()
            if normalized_team in normalized_key:
                return f'=IMAGE("{url}", 1)'
    return ""

def find_abbreviation(team_name, logo_url=""):
    u = (logo_url or "").lower()
    if "nyg" in u: return "NYG"
    if "nyj" in u: return "NYJ"
    if "lar" in u: return "LAR"
    if "lac" in u: return "LAC"
    for key, abbr in TEAM_ABBR.items():
        if key in team_name:
            return abbr
    return None

# =========================
# === SCRAPERS (8-col A..H output) ===
# =========================
def scrape_nfl_schedule(year: int | None = None, week: int | None = None):
    """
    Step 7+8+13:
    - Determine (yr, wk, win_start, win_end) from deterministic table if FORCE_WEEK_TABLE=1.
    - Navigate to ESPN /week/{wk}/year/{yr}.
    - After scraping, filter rows by local kickoff inside [win_start, win_end).
    - Resiliency: if 0 pairs after filter, probe week-1 and week+1 (still filter by same window).
    """
    print("Launching browser and scraping NFL schedule...")
    tz = ZoneInfo(TIMEZONE)
    now_local = datetime.now(tz)

    # Override via deterministic mapping
    if FORCE_WEEK_TABLE:
        yr_map, wk_map, win_start, win_end = get_nfl_week_from_table(now_local)
        year = yr_map
        week = wk_map
        window_bounds = (win_start, win_end)
    else:
        # Fallback: still allow explicit overrides if provided
        if year is None or week is None:
            # If missing, we still try ESPN's base page (not recommended)
            pass
        # Build a synthetic window wide enough so filter doesn't drop everything
        window_bounds = (now_local - timedelta(days=4), now_local + timedelta(days=10))

    base_url = "https://www.espn.com/nfl/schedule"
    def build_url(y, w):
        if w is not None:
            return f"{base_url}/_/week/{w}" + (f"/year/{y}" if y is not None else "")
        return base_url

    def scrape_page(url):
        all_rows = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS)
            page = browser.new_page()
            page.goto(url, timeout=60000)
            page.wait_for_selector("div.ScheduleTables", timeout=15000)

            date_sections = page.locator("div.ScheduleTables > div")
            print(f"✅ Found {date_sections.count()} game date sections\n")

            for i in range(date_sections.count()):
                section = date_sections.nth(i)
                date_header = section.locator("div.Table__Title").text_content().strip()
                rows = section.locator("tbody tr")

                for j in range(rows.count()):
                    row = rows.nth(j)
                    tds = row.locator("td")
                    if tds.count() < 2:
                        continue

                    away_team_cell = tds.nth(0)
                    home_team_cell = tds.nth(1)

                    away_team = away_team_cell.locator("span.Table__Team a").nth(1).text_content(timeout=2000).strip()
                    home_team = home_team_cell.locator("span.Table__Team a").nth(1).text_content(timeout=2000).strip()
                    game_time = tds.nth(2).text_content(timeout=1000).strip() if tds.count() > 2 else "N/A"

                    away_logo_url = away_team_cell.locator("img").get_attribute("src") or ""
                    home_logo_url = home_team_cell.locator("img").get_attribute("src") or ""

                    away_logo_formula = get_logo_formula(away_team, away_logo_url, league="nfl")
                    home_logo_formula = get_logo_formula(home_team, home_logo_url, league="nfl")

                    line, ou = "N/A", "N/A"
                    if tds.count() > 6:
                        odds_links = tds.nth(6).locator("a")
                        for k in range(odds_links.count()):
                            raw = odds_links.nth(k).text_content(timeout=1000).strip()
                            t = raw.lower().replace("\u00bd", ".5")
                            if t.startswith("line:") or t.startswith("spread:"):
                                line = raw.split(":", 1)[-1].strip()
                            elif t.startswith("o/u:") or t.startswith("total:"):
                                ou = raw.split(":", 1)[-1].strip()
                            elif re.match(r"^[A-Za-z]{2,4}\s*[+-]\d+(\.\d+)?$", raw):
                                line = raw.strip()

                    def resolve_abbreviation_by_logo(team_name, logo_url):
                        u = (logo_url or "").lower()
                        if "nyg" in u: return "NYG"
                        if "nyj" in u: return "NYJ"
                        if "lar" in u: return "LAR"
                        if "lac" in u: return "LAC"
                        return find_abbreviation(team_name)

                    away_abbr = resolve_abbreviation_by_logo(away_team, away_logo_url)
                    home_abbr = resolve_abbreviation_by_logo(home_team, home_logo_url)

                    away_line = home_line = "N/A"
                    if line != "N/A":
                        parts = line.split()
                        if len(parts) == 2:
                            favored_abbr, raw_spread = parts
                            try:
                                spread = float(raw_spread.replace("+", "").replace("-", ""))
                                spread_str = f"{spread:.1f}".rstrip('0').rstrip('.')
                                if favored_abbr == away_abbr:
                                    away_line = f"{away_abbr} -{spread_str}"
                                    home_line = f"{home_abbr} +{spread_str}"
                                elif favored_abbr == home_abbr:
                                    home_line = f"{home_abbr} -{spread_str}"
                                    away_line = f"{away_abbr} +{spread_str}"
                            except ValueError:
                                pass

                    ou_top = f"O {ou}" if ou != "N/A" else "N/A"
                    ou_bottom = f"U {ou}" if ou != "N/A" else "N/A"

                    # A..H: Logo, Team, Pick#, Line, Pick#, O/U, Date, Time
                    all_rows.append([away_logo_formula, away_team, "", away_line, "", ou_top, date_header, game_time])
                    all_rows.append([home_logo_formula, home_team, "", home_line, "", ou_bottom, date_header, game_time])

            browser.close()
        return all_rows

    target_url = build_url(year, week)
    raw_rows = scrape_page(target_url)

    # Step 8: Filter by deterministic window
    def parse_kickoff_local(date_text: str, time_text: str, tzname: str) -> datetime | None:
        if not date_text or not time_text or time_text.upper() in ("TBD","N/A","-","POSTPONED"):
            return None
        try:
            m = re.search(r"([A-Za-z]+)\s+(\d{1,2})", date_text)
            if not m:
                return None
            month, day = m.group(1), int(m.group(2))
            t = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", time_text, re.IGNORECASE)
            if not t:
                return None
            timestr = t.group(1).upper().replace(" ", "")
            now = datetime.now(ZoneInfo(tzname))
            year = now.year
            month_num = datetime.strptime(month, "%B").month
            if now.month >= 11 and month_num <= 2:
                year = now.year + 1
            dt = datetime.strptime(f"{month} {day} {year} {timestr}", "%B %d %Y %I:%M%p").replace(tzinfo=ZoneInfo(tzname))
            if (now - dt).days > 180:
                dt = dt.replace(year=dt.year + 1)
            return dt
        except Exception:
            return None

    win_start, win_end = window_bounds
    filtered = []
    kept_pairs = 0
    for i in range(0, len(raw_rows), 2):
        if i + 1 >= len(raw_rows):
            break
        a = raw_rows[i]
        h = raw_rows[i + 1]
        ko = parse_kickoff_local(a[6], a[7], TIMEZONE)
        if ko is None:
            # Step 13: keep one placeholder pair if parse fails
            filtered.extend([a, h])
            kept_pairs += 1
            continue
        if win_start <= ko < win_end:
            filtered.extend([a, h])
            kept_pairs += 1

    # Step 13 fallback: if 0 pairs, probe week-1 and week+1, still filter by the same window
    if kept_pairs == 0 and week is not None:
        for delta in (-1, +1):
            probe_w = week + delta
            probe_y = year
            if probe_w < 1:
                # no earlier week in table; skip
                continue
            if probe_w > REG_WEEKS:
                # beyond regular-season table; skip
                continue
            probe_url = build_url(probe_y, probe_w)
            alt_rows = scrape_page(probe_url)
            alt_filtered = []
            for i in range(0, len(alt_rows), 2):
                if i + 1 >= len(alt_rows):
                    break
                a = alt_rows[i]; h = alt_rows[i+1]
                ko = parse_kickoff_local(a[6], a[7], TIMEZONE)
                if ko is None or (win_start <= ko < win_end):
                    alt_filtered.extend([a, h])
            if alt_filtered:
                filtered = alt_filtered
                break

    return filtered

def _normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s&'-]", "", s)).strip().lower()

def extract_rank_from_team_cell(cell):
    for sel in ["span.TeamRank", "span.teamRank", "span.rank", "span.Rank", ":scope span"]:
        loc = cell.locator(sel)
        if loc.count() > 0:
            txt = loc.first.text_content().strip()
            if txt.isdigit():
                val = int(txt)
                if 1 <= val <= 25:
                    return val
    spans = cell.locator(":scope span")
    for i in range(spans.count()):
        t = spans.nth(i).text_content().strip()
        if t.isdigit():
            val = int(t)
            if 1 <= val <= 25:
                return val
    full = cell.text_content().strip()
    m = re.match(r"^\s*(\d{1,2})\s", full)
    if m:
        val = int(m.group(1))
        if 1 <= val <= 25:
            return val
    return None

def scrape_college_schedule(year: int | None = None, week: int | None = None):
    print("📡 Running College scraper...")
    base_url = "https://www.espn.com/college-football/schedule"
    if week is not None:
        url = f"{base_url}/_/week/{week}" + (f"/year/{year}" if year is not None else "")
    else:
        url = base_url

    all_data = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()
        page.goto(url, timeout=60000)
        page.wait_for_selector("div.ScheduleTables--ncaaf")

        date_sections = page.locator("div.ScheduleTables--ncaaf")
        print(f"✅ Found {date_sections.count()} game date sections\n")

        for i in range(date_sections.count()):
            section = date_sections.nth(i)
            try:
                date_text = section.locator("div.Table__Title").text_content().strip()
                print(f"🗓️ Date: {date_text}")
            except:
                print("❌ Could not read date title.")
                continue

            rows = section.locator("tr.Table__TR")
            print(f"  - Found {rows.count()} rows total")

            for j in range(rows.count()):
                row = rows.nth(j)
                tds = row.locator("td")
                if tds.count() < 2:
                    continue
                if tds.nth(0).locator("a").count() < 2 or tds.nth(1).locator("a").count() < 2:
                    continue

                try:
                    away_cell = tds.nth(0)
                    home_cell = tds.nth(1)

                    away_team = away_cell.locator("a").nth(1).text_content(timeout=2000).strip()
                    home_team = home_cell.locator("a").nth(1).text_content(timeout=2000).strip()
                    game_time = tds.nth(2).text_content(timeout=1000).strip() if tds.count() > 2 else "N/A"

                    away_rank = extract_rank_from_team_cell(away_cell)
                    home_rank = extract_rank_from_team_cell(home_cell)

                    if not COLLEGE_INCLUDE_ALL and (away_rank is None and home_rank is None):
                        continue

                    line, ou = "N/A", "N/A"
                    if tds.count() > 6:
                        odds_links = tds.nth(6).locator("a")
                        for k in range(odds_links.count()):
                            raw = odds_links.nth(k).text_content(timeout=1000).strip()
                            t = raw.lower().replace("\u00bd", ".5")
                            if t.startswith("line:") or t.startswith("spread:"):
                                line = raw.split(":", 1)[-1].strip()
                            elif t.startswith("o/u:") or t.startswith("total:"):
                                ou = raw.split(":", 1)[-1].strip()
                            elif re.match(r"^[A-Za-z]{2,4}\s*[+-]\d+(\.\d+)?$", raw):
                                line = raw.strip()

                    away_logo = get_logo_formula(away_team, league="college")
                    home_logo = get_logo_formula(home_team, league="college")

                    away_abbr = college_abbreviation_dict.get(away_team, "").upper()
                    home_abbr = college_abbreviation_dict.get(home_team, "").upper()

                    away_line = home_line = "N/A"
                    if line != "N/A" and away_abbr and home_abbr:
                        parts = line.split()
                        if len(parts) == 2:
                            favored_abbr, raw_spread = parts
                            favored_abbr = favored_abbr.upper()
                            try:
                                spread = float(raw_spread.replace("+", "").replace("-", ""))
                                spread_str = f"{spread:.1f}".rstrip('0').rstrip('.')
                                if favored_abbr == away_abbr:
                                    away_line = f"{away_abbr} -{spread_str}"
                                    home_line = f"{home_abbr} +{spread_str}"
                                elif favored_abbr == home_abbr:
                                    home_line = f"{home_abbr} -{spread_str}"
                                    away_line = f"{away_abbr} +{spread_str}"
                                else:
                                    away_line = "***"
                                    home_line = "***"
                            except ValueError:
                                pass

                    ou_top = f"O {ou}" if ou != "N/A" else "N/A"
                    ou_bottom = f"U {ou}" if ou != "N/A" else "N/A"

                    away_name_for_sheet = f"{away_rank} {away_team}" if away_rank else away_team
                    home_name_for_sheet = f"{home_rank} {home_team}" if home_rank else home_team

                    all_data.append([away_logo, away_name_for_sheet, "", away_line, "", ou_top, date_text, game_time])
                    all_data.append([home_logo, home_name_for_sheet, "", home_line, "", ou_bottom, date_text, game_time])

                except Exception:
                    continue

        browser.close()
        return all_data

# =========================
# === STAGING + UPSERT  ===
# =========================
HEADER_AH = ["Logo", "Team", "Pick #", "Line", "Pick #", "O/U", "Date", "Time"]
HEADER_IR = ["League","WeekTag","Phase","GameKey","KickoffLocal","ReleaseAt","FreezeAt","Locked","Status","LastUpdated"]

def ensure_headers(worksheet):
    current = worksheet.get_values("A1:R1")
    need_write = False
    if not current or len(current[0]) < 18:
        need_write = True
    else:
        expected = HEADER_AH + HEADER_IR
        for idx, val in enumerate(expected, start=1):
            if idx-1 >= len(current[0]) or current[0][idx-1] != val:
                need_write = True
                break
    if need_write:
        worksheet.update([HEADER_AH + HEADER_IR], range_name="A1:R1")

def parse_kickoff_local(date_text: str, time_text: str, tzname: str) -> datetime | None:
    if not date_text or not time_text or time_text.upper() in ("TBD","N/A","-","POSTPONED"):
        return None
    try:
        m = re.search(r"([A-Za-z]+)\s+(\d{1,2})", date_text)
        if not m:
            return None
        month, day = m.group(1), int(m.group(2))
        t = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", time_text, re.IGNORECASE)
        if not t:
            return None
        timestr = t.group(1).upper().replace(" ", "")
        now = datetime.now(ZoneInfo(tzname))
        year = now.year
        month_num = datetime.strptime(month, "%B").month
        if now.month >= 11 and month_num <= 2:
            year = now.year + 1
        dt = datetime.strptime(f"{month} {day} {year} {timestr}", "%B %d %Y %I:%M%p").replace(tzinfo=ZoneInfo(tzname))
        if (now - dt).days > 180:
            dt = dt.replace(year=dt.year + 1)
        return dt
    except Exception:
        return None

def compute_release_freeze(kickoff_dt: datetime | None, league: str, phase: str):
    if kickoff_dt is None:
        return None, None
    tz = kickoff_dt.tzinfo
    dow = kickoff_dt.weekday()  # Mon=0..Sun=6
    monday = (kickoff_dt - timedelta(days=dow)).replace(hour=0, minute=0, second=0, microsecond=0)
    if dow in (3, 4):  # Thu/Fri
        rel = (monday + timedelta(days=1)).replace(hour=0, minute=1)   # Tue 00:01
        frz = (monday + timedelta(days=1)).replace(hour=12, minute=0)  # Tue 12:00
    else:  # Sat/Sun/Mon and others -> Thu window
        rel = (monday + timedelta(days=3)).replace(hour=0, minute=1)   # Thu 00:01
        frz = (monday + timedelta(days=3)).replace(hour=12, minute=0)  # Thu 12:00
    return rel.astimezone(tz), frz.astimezone(tz)

def week_tag_explicit(league: str, kickoff_dt: datetime | None):
    """
    Step 7+9+12:
    - If FORCE_WEEK_TABLE=1: use deterministic mapping for NFL and CFB tags.
    - Else: preserve explicit override behavior (NFL_WEEK/CFB_WEEK), then fall back.
    """
    now_local = datetime.now(ZoneInfo(TIMEZONE))
    if FORCE_WEEK_TABLE:
        if league == "nfl":
            return week_tag_from_table("nfl", now_local)
        if league == "ncaaf":
            return week_tag_from_table("ncaaf", now_local)

    # Legacy explicit overrides (only when mapping is not forced)
    if league == "ncaaf" and CFB_WEEK is not None:
        yr = CFB_YEAR or (kickoff_dt.year if kickoff_dt else now_local.year)
        return f"{yr}-CFB-Wk{CFB_WEEK}"
    if league == "nfl" and NFL_WEEK is not None:
        yr = NFL_YEAR or (kickoff_dt.year if kickoff_dt else now_local.year)
        return f"{yr}-NFL-Wk{NFL_WEEK}"
    return None

def compute_week_tag(kickoff_dt: datetime | None, league: str, phase: str) -> str:
    if kickoff_dt is None:
        year = datetime.now(ZoneInfo(TIMEZONE)).year
    else:
        year = kickoff_dt.year
    league_tag = "NFL" if league == "nfl" else "CFB"
    if phase in ("playoffs", "bowls"):
        return f"{year}-{league_tag}-{phase.capitalize()}"
    wk = (kickoff_dt or datetime.now(ZoneInfo(TIMEZONE))).isocalendar().week
    return f"{year}-{league_tag}-Wk{wk}"

def normalize_team_for_key(name: str) -> str:
    name = name.strip()
    name = re.sub(r"^\d{1,2}\s+", "", name).strip()
    return re.sub(r"\s+", " ", name).upper()

def make_game_key(kickoff_dt: datetime | None,
                  away_name_display: str,
                  home_name_display: str,
                  espn_game_id: str | None = None,
                  league: str | None = None,
                  phase: str | None = None) -> str:
    """
    Stable key per matchup in the current week.
    Prefer date-based key when kickoff_dt is parsed; otherwise fall back to deterministic WeekTag.
    """
    if espn_game_id:
        return espn_game_id

    away = normalize_team_for_key(away_name_display)
    home = normalize_team_for_key(home_name_display)

    if kickoff_dt:
        dt_part = kickoff_dt.strftime("%Y-%m-%d")
        return f"{dt_part}|{away}|{home}"

    # Fallback when time is TBD / parse failed: use deterministic week tag
    now_local = datetime.now(ZoneInfo(TIMEZONE))
    if FORCE_WEEK_TABLE:
        wk_tag = week_tag_from_table(league or "nfl", now_local) if league else week_tag_from_table("nfl", now_local)
    else:
        wk_tag = week_tag_explicit(league or "nfl", now_local) or compute_week_tag(now_local, league or "nfl", phase or "regular")
    return f"{wk_tag}|{away}|{home}"

def pack_pairs_to_games(row_pairs):
    games = []
    for i in range(0, len(row_pairs), 2):
        if i + 1 >= len(row_pairs):
            break
        away = row_pairs[i]
        home = row_pairs[i + 1]
        games.append({
            "away_row": away,
            "home_row": home,
            "away_team_disp": away[1],
            "home_team_disp": home[1],
            "date_text": away[6],
            "time_text": away[7],
            "away_line": away[3],
            "home_line": home[3],
            "ou_top": away[5],
            "ou_bottom": home[5],
        })
    return games

PUBLISH_DAYS_AHEAD = 14  # unchanged policy guard

def should_publish_now(now_dt: datetime, kickoff_dt: datetime | None) -> bool:
    if kickoff_dt is None:
        return True
    return (kickoff_dt - now_dt) <= timedelta(days=PUBLISH_DAYS_AHEAD)

def _ensure_grid_capacity(ws, needed_rows: int, needed_cols: int = 18):
    cur_rows, cur_cols = ws.row_count, ws.col_count
    target_rows = max(cur_rows, int(needed_rows))
    target_cols = max(cur_cols, int(needed_cols))
    if target_rows > cur_rows or target_cols > cur_cols:
        print(f"Resizing grid: rows {cur_rows}→{target_rows}, cols {cur_cols}→{target_cols}")
        ws.resize(rows=target_rows, cols=target_cols)
    else:
        print(f"No resize needed (rows={cur_rows}, cols={cur_cols})")

def _cleanup_legacy_misaligned_rows(lines_ws):
    vals = lines_ws.get_all_values()
    if not vals or len(vals) < 2:
        return 0
    bad_tops = []
    for r in range(2, len(vals)+1, 2):
        row = vals[r-1]
        if len(row) >= 7:
            g_val = str(row[6]).strip().lower()
            if g_val in ("ncaaf", "nfl"):
                bad_tops.append(r)
    if not bad_tops:
        return 0
    blocks = []
    start = None
    prev = None
    for top in bad_tops:
        if start is None:
            start = top; prev = top
        elif top == prev + 2:
            prev = top
        else:
            blocks.append((start, prev+1))
            start = top; prev = top
    blocks.append((start, prev+1))
    deleted = 0
    for s, e in reversed(blocks):
        lines_ws.delete_rows(s, e)
        deleted += (e - s + 1)
    return deleted

def _purge_lines_to_current_week(spreadsheet, now_dt: datetime, phase_cfb: str, phase_nfl: str):
    """
    Step 9: Purge based on deterministic WeekTag for BOTH leagues.
    """
    try:
        lines_ws = spreadsheet.worksheet("Lines")
    except gspread.exceptions.WorksheetNotFound:
        return

    vals = lines_ws.get_all_values()
    if not vals or len(vals) < 2:
        return

    # Current tags from mapping (or fallback to legacy if not forced)
    if FORCE_WEEK_TABLE:
        nfl_tag = week_tag_from_table("nfl", now_dt)
        cfb_tag = week_tag_from_table("ncaaf", now_dt)
    else:
        cfb_tag = week_tag_explicit("ncaaf", now_dt) or compute_week_tag(now_dt, league="ncaaf", phase=phase_cfb)
        nfl_tag = week_tag_explicit("nfl",   now_dt) or compute_week_tag(now_dt, league="nfl",   phase=phase_nfl)

    keep_for_league = {
        "ncaaf": cfb_tag,
        "nfl":   nfl_tag,
    }

    to_delete_tops = []
    for top in range(2, len(vals) + 1, 2):
        row = vals[top - 1]
        if len(row) < 12:
            continue

        league   = (row[8]  or "").strip().lower()  # I
        weektag  = (row[9]  or "").strip()          # J
        gamekey  = (row[11] or "").strip()          # L
        locked_v = (row[15] or "").strip().upper()  # P

        if not gamekey:
            continue  # skip blanks

        # 🔒 Do NOT purge locked rows
        if locked_v == "Y":
            continue

        if league in keep_for_league and weektag != keep_for_league[league]:
            to_delete_tops.append(top)

    if not to_delete_tops:
        print("Purge: nothing to delete.")
        return

    blocks = []
    start = None
    prev = None
    for t in to_delete_tops:
        if start is None:
            start = t; prev = t
        elif t == prev + 2:
            prev = t
        else:
            blocks.append((start, prev + 1))
            start = t; prev = t
    blocks.append((start, prev + 1))

    frozen_rows = 1
    cur_rows = lines_ws.row_count
    rows_to_delete = sum((e - s + 1) for s, e in blocks)
    remaining_after = cur_rows - frozen_rows - rows_to_delete

    if remaining_after <= 1:
        need_pairs_left = 2
        additional_needed = need_pairs_left - remaining_after
        if additional_needed < 2:
            additional_needed = 2
        new_total_rows = cur_rows + additional_needed
        print(f"⚠️ Purge would delete all non-frozen rows. Pre-growing grid {cur_rows}→{new_total_rows}")
        lines_ws.resize(rows=new_total_rows, cols=lines_ws.col_count)

    for s, e in reversed(blocks):
        lines_ws.delete_rows(s, e)

    print("Purge: completed without deleting all non-frozen rows.")

def queue_pair_range(ws_title: str, top_row: int, full_away: list, full_home: list):
    a1 = f"{ws_title}!A{top_row}:R{top_row+1}"
    full_away = (full_away + [""] * 18)[:18]
    full_home = (full_home + [""] * 18)[:18]
    return {"range": a1, "values": [full_away, full_home]}

def upsert_lines_strict(lines_ws, queued_ranges, value_input_option="USER_ENTERED"):
    if not queued_ranges:
        return [], 0
    ss = lines_ws.spreadsheet
    lines_ws = ss.worksheet(lines_ws.title)
    q_adj = _normalize_pair_alignment(queued_ranges)
    print(f"First queued: {q_adj[0]['range']}")
    print(f"Last  queued: {q_adj[-1]['range']}")
    max_row_needed = compute_max_row_needed(q_adj)
    fresh_rows, fresh_cols = lines_ws.row_count, lines_ws.col_count
    print(f"Current grid rows={fresh_rows}, cols={fresh_cols}")
    print(f"Max row needed={max_row_needed}, Needed cols=18")
    grow_rows = max(max_row_needed, fresh_rows)
    grow_cols = max(18, fresh_cols)
    if grow_rows > fresh_rows or grow_cols > fresh_cols:
        print(f"Resizing grid: rows {fresh_rows}→{grow_rows}, cols {fresh_cols}→{grow_cols}")
        lines_ws.resize(rows=grow_rows, cols=grow_cols)
        lines_ws = ss.worksheet(lines_ws.title)
    else:
        print(f"No resize needed (rows={fresh_rows}, cols={fresh_cols})")
    assert lines_ws.row_count >= max_row_needed, (
        f"Grid still too small: rows={lines_ws.row_count}, needed={max_row_needed}"
    )
    body = {
        "valueInputOption": value_input_option,
        "data": [{"range": item["range"], "majorDimension": "ROWS", "values": item["values"]}
                 for item in q_adj]
    }
    print("Writing values_batch_update ...")
    ss.values_batch_update(body)
    print("Write complete.")
    return [item["range"] for item in q_adj], max_row_needed

def merge_staging_into_lines(spreadsheet, staging_rows, league: str, phase: str):
    """
    Merge normalized A..H rows into Lines with robust matching and lock safety.

    Changes:
      - Never purge locked rows (handled in _purge_lines_to_current_week).
      - Locking depends ONLY on freeze time (not publish window).
      - Secondary match key (WeekTag + teams) when GameKey doesn't line up (e.g., TBD times, manual rows).
      - Never downgrade an already locked row; outside publish window we don't update existing rows.
    """
    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)

    # Normalize rows to A..H (Logo, Team, Pick#, Line, Pick#, O/U, Date, Time)
    staging_rows = normalize_rows_to_AH(staging_rows)

    # Ensure Lines exists + headers
    try:
        lines_ws = spreadsheet.worksheet("Lines")
    except gspread.exceptions.WorksheetNotFound:
        lines_ws = spreadsheet.add_worksheet(title="Lines", rows="200", cols="18")
        lines_ws.freeze(rows=1, cols=2)
    ensure_headers(lines_ws)

    # One-time cleanup of legacy misaligned rows
    deleted = _cleanup_legacy_misaligned_rows(lines_ws)
    if deleted:
        print(f"🧹 Removed {deleted} legacy misaligned rows from Lines.")

    # Purge old weeks so Lines only contains the current week per league
    if os.environ.get("SKIP_PURGE", "1") != "1":
        _purge_lines_to_current_week(
            spreadsheet,
            now_dt=now,
            phase_cfb=phase if league == "ncaaf" else PHASE_CFB,
            phase_nfl=phase if league == "nfl"   else PHASE_NFL
        )
    else:
        print("Purge: SKIPPED (SKIP_PURGE=1)")

    # 🔄 refresh handle; row_count/col_count can be stale after deletes
    lines_ws = spreadsheet.worksheet("Lines")

    # Rebuild 'existing' AFTER purge
    existing = lines_ws.get_all_values()

    # Optional spacer before the first CFB block if NFL already exists
    spacer_rows = 0
    if league == "ncaaf":
        leagues = [(row[8] or "").strip().lower() for row in existing[1:] if len(row) >= 9]
        has_nfl = any(l == "nfl" for l in leagues)
        has_cfb = any(l == "ncaaf" for l in leagues)
        if has_nfl and not has_cfb:
            spacer_rows = SPACER_ROWS_BETWEEN_LEAGUES if "SPACER_ROWS_BETWEEN_LEAGUES" in globals() else 4

    # Primary index: GameKey -> top row
    gamekey_col = 12  # L
    index = {}
    for r in range(2, len(existing)+1, 2):
        row_vals = existing[r-1]
        if len(row_vals) >= gamekey_col:
            gk = row_vals[gamekey_col-1]
            if gk:
                index[gk] = r

    # Secondary index: (WeekTag, away_norm, home_norm) -> top row
    index2 = {}
    for r in range(2, len(existing)+1, 2):
        row_top = existing[r-1] if r-1 < len(existing) else []
        row_bot = existing[r]   if r   < len(existing) else []
        if len(row_top) < 12 or len(row_bot) < 2:
            continue
        wk   = (row_top[9]  or "").strip()      # J WeekTag
        away = _strip_rank(row_top[1] or "")    # B Team (top)
        home = _strip_rank(row_bot[1]  or "")   # B Team (bottom)
        if wk and away and home:
            index2[(wk, away, home)] = r

    games = pack_pairs_to_games(staging_rows)

    def fmt(dt):
        return dt.astimezone(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M") if dt else ""

    # Compute the next append TOP row, applying spacer and keeping it even
    last_row_with_data = len(existing) if existing else 1
    append_top = last_row_with_data + 1 + spacer_rows
    if append_top < 2:
        append_top = 2
    if append_top % 2 == 1:
        append_top += 1  # bump to even

    queued_ranges = []
    touched_new = []   # for formatting
    touched_upd = []   # for formatting

    for g in games:
        kickoff_dt = parse_kickoff_local(g["date_text"], g["time_text"], TIMEZONE)
        release_at, freeze_at = compute_release_freeze(kickoff_dt, league, phase)

        # Mapping-based WeekTag takes precedence
        week_tag = week_tag_explicit(league, kickoff_dt) or compute_week_tag(kickoff_dt, league, phase)

        # Stable key (date-based when parsed; otherwise deterministic week tag)
        game_key = make_game_key(
            kickoff_dt,
            g["away_team_disp"],
            g["home_team_disp"],
            espn_game_id=None,
            league=league,
            phase=phase
        )

        # 🔒 Locking depends ONLY on freeze time (not on publish window)
        locked_flag = "Y" if (freeze_at and now >= freeze_at) else "N"

        status = "placeholder"
        allow_lines = False

        window_ok = publish_window_allows(now, kickoff_dt)  # Tue/Thu gating for showing lines

        # Gate visible Lines/O-U by publish window + release time
        if window_ok and release_at and now >= release_at:
            status = "posted"
            allow_lines = True

        # Past freeze -> mark locked (still only show lines if window allows)
        if freeze_at and now >= freeze_at:
            status = "locked"

        # Try primary match by GameKey; fallback to (WeekTag, teams) for manual/TBD rows
        top_row = index.get(game_key)
        if top_row is None:
            alt_key = (week_tag, _strip_rank(g["away_team_disp"]), _strip_rank(g["home_team_disp"]))
            top_row = index2.get(alt_key)

        a = g["away_row"][:]  # A..H
        h = g["home_row"][:]

        if not allow_lines:
            # Only strip on brand-new rows; never touch existing
            if top_row is None:
                a[3] = ""  # D Line
                h[3] = ""
                a[5] = ""  # F O/U
                h[5] = ""
        else:
            # Normalize "N/A" to blank when we ARE allowed to publish
            for row in (a, h):
                if row[3] == "N/A": row[3] = ""
                if row[5] == "N/A": row[5] = ""

        meta = [
            ("ncaaf" if league == "ncaaf" else "nfl"),   # I League
            week_tag,                                    # J WeekTag
            phase,                                       # K Phase
            game_key,                                    # L GameKey
            fmt(kickoff_dt),                             # M KickoffLocal
            fmt(release_at),                             # N ReleaseAt
            fmt(freeze_at),                              # O FreezeAt
            locked_flag,                                 # P Locked
            status,                                      # Q Status
            fmt(now),                                    # R LastUpdated
        ]

        full_away = a + meta
        full_home = h + meta

        if top_row is None:
            # New pair
            rng = queue_pair_range(lines_ws.title, append_top, full_away, full_home)
            queued_ranges.append(rng)
            touched_new.append(rng["range"])
            index[game_key] = append_top
            append_top += 2
        else:
            # Existing pair — respect Locked=Y and publish window
            try:
                locked_cell = lines_ws.get_value(f"P{top_row}")  # 'Y' or 'N'
            except Exception:
                locked_cell = None

            # If sheet says locked, do not update — ever
            if (not TEST_MODE_IGNORE_LOCKS) and locked_cell and str(locked_cell).upper() == "Y":
                continue

            # Outside publish window? leave existing rows untouched
            if not allow_lines:
                continue

            upd = queue_pair_range(lines_ws.title, top_row, full_away, full_home)
            queued_ranges.append(upd)
            touched_upd.append(upd["range"])

    # === Strict, ordered write ===
    written_ranges, max_row_needed = upsert_lines_strict(lines_ws, queued_ranges)

    # === Formatting on the rows we just touched ===
    if written_ranges:
        _ensure_grid_capacity(lines_ws, needed_rows=max_row_needed, needed_cols=18)

        with batch_updater(lines_ws.spreadsheet) as batch:
            right_border_thick = Border("SOLID_THICK")
            light_orange = Color(1.0, 0.898, 0.8)
            light_blue = Color(0.8, 0.898, 1.0)

            for rng in written_ranges:
                m = re.match(r".*A(\d+):R(\d+)", rng)
                if not m:
                    continue
                r1 = int(m.group(1))
                r2 = int(m.group(2))
                # Perimeter across A..H
                batch.format_cell_range(lines_ws, f"A{r1}:H{r1}", CellFormat(borders=Borders(top=Border("SOLID"))))
                batch.format_cell_range(lines_ws, f"A{r2}:H{r2}", CellFormat(borders=Borders(bottom=Border("SOLID"))))
                batch.format_cell_range(lines_ws, f"A{r1}:A{r2}", CellFormat(borders=Borders(left=Border("SOLID"))))
                batch.format_cell_range(lines_ws, f"H{r1}:H{r2}", CellFormat(borders=Borders(right=Border("SOLID"))))
                # Header-like fills for C–F
                batch.format_cell_range(lines_ws, f"C{r1}:D{r2}", CellFormat(backgroundColor=light_orange))
                batch.format_cell_range(lines_ws, f"E{r1}:F{r2}", CellFormat(backgroundColor=light_blue))
                # Thick vertical borders at B, D, F
                batch.format_cell_range(lines_ws, f"B{r1}:B{r2}", CellFormat(borders=Borders(right=right_border_thick)))
                batch.format_cell_range(lines_ws, f"D{r1}:D{r2}", CellFormat(borders=Borders(right=right_border_thick)))
                batch.format_cell_range(lines_ws, f"F{r1}:F{r2}", CellFormat(borders=Borders(right=right_border_thick)))

def upload_via_staging_and_merge(data_rows, league: str, phase: str):
    data_rows = normalize_rows_to_AH(data_rows)
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet_id = os.environ["PICK_SHEET_ID"]
    spreadsheet = client.open_by_key(sheet_id)

    temp_sheet_name = "temp_Lines"
    try:
        spreadsheet.del_worksheet(spreadsheet.worksheet(temp_sheet_name))
    except gspread.exceptions.WorksheetNotFound:
        pass

    ws = spreadsheet.add_worksheet(title=temp_sheet_name, rows=str(max(len(data_rows)+5, 200)), cols="18")
    ws.freeze(rows=1, cols=2)
    ws.update([HEADER_AH], range_name="A1:H1")

    if data_rows:
        ws.update(data_rows, range_name=f"A2:H{len(data_rows)+1}", value_input_option="USER_ENTERED")
        format_cell_range(ws, "A1:H1", CellFormat(textFormat=TextFormat(bold=True), horizontalAlignment='CENTER'))
        center_align = CellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
        for col in ["C", "D", "E", "F", "G", "H"]:
            format_cell_range(ws, f"{col}2:{col}{len(data_rows)+1}", center_align)

    merge_staging_into_lines(spreadsheet, data_rows, league=league, phase=phase)

    try:
        spreadsheet.del_worksheet(ws)
    except Exception:
        pass

def publish_window_allows(now_dt: datetime, kickoff_dt: datetime | None) -> bool:
    """
    Day-of-week policy (unchanged), ensures placeholders appear even if lines are gated:
      - Tue: Thu & Fri
      - Thu: Sat, Sun, Mon
      - Other days: false
    """
    if kickoff_dt is None:
        return True
    tz = now_dt.tzinfo
    kdate = kickoff_dt.astimezone(tz).date()
    week_mon = (now_dt - timedelta(days=now_dt.weekday())).date()
    if now_dt.weekday() == 1:  # Tuesday
        start = week_mon + timedelta(days=3)  # Thu
        end   = week_mon + timedelta(days=4)  # Fri
        return start <= kdate <= end
    if now_dt.weekday() == 3:  # Thursday
        start = week_mon + timedelta(days=5)  # Sat
        end   = week_mon + timedelta(days=7)  # Mon (next week)
        return start <= kdate <= end
    return False

def apply_weekly_reset_if_complete(spreadsheet, week_tag_to_clear: str | None):
    return

# =========================
# === MAIN ORCHESTRATION ==
# =========================
if __name__ == "__main__":
    os.system("cls" if os.name == "nt" else "clear")

    nfl_rows = []
    cfb_rows = []

    if include_nfl:
        print("Running NFL scraper...")
        nfl_rows = scrape_nfl_schedule(year=NFL_YEAR, week=NFL_WEEK)

    if include_college:
        print("Running College scraper...")
        cfb_rows = scrape_college_schedule(year=CFB_YEAR, week=CFB_WEEK)

    # Upload per league so metadata (I–R) is correct
    if nfl_rows:
        upload_via_staging_and_merge(nfl_rows, league="nfl",   phase=PHASE_NFL)
    if cfb_rows:
        upload_via_staging_and_merge(cfb_rows, league="ncaaf", phase=PHASE_CFB)

    print("✅ Staged upsert complete for all selected leagues.")
