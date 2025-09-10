import os

# Read from GitHub Secrets (set in the workflow env)
PICK_SHEET_ID = os.environ.get("PICK_SHEET_ID")
COLLEGE_SHEET_ID = os.environ.get("COLLEGE_SHEET_ID")

if not PICK_SHEET_ID or not COLLEGE_SHEET_ID:
  raise RuntimeError("Missing sheet IDs. Ensure PICK_SHEET_ID and COLLEGE_SHEET_ID are set in the environment.")

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

TIMEZONE = "America/Detroit"
PHASE_CFB = "regular"     # regular | bowls
PHASE_NFL = "regular"     # regular | playoffs
COLLEGE_INCLUDE_ALL = False

# Week flow toggles for this run (Week 1 per plan: college-only)
include_nfl = True
include_college = True

TEST_MODE_IGNORE_LOCKS = False  # set to False in real runs

# --- Optional week targeting (leave as None to use ESPN's active week) ---
NFL_YEAR = None
NFL_WEEK = None        # set None to use ESPN's current "active" week

# --- Optional overrides via environment (for deterministic runs) ---
NFL_WEEK_OVERRIDE = os.environ.get("NFL_WEEK_OVERRIDE")  # e.g., "37"
NFL_YEAR_OVERRIDE = os.environ.get("NFL_YEAR_OVERRIDE")  # e.g., "2025"

if NFL_WEEK_OVERRIDE:
    NFL_WEEK = int(NFL_WEEK_OVERRIDE)
if NFL_YEAR_OVERRIDE:
    NFL_YEAR = int(NFL_YEAR_OVERRIDE)

CFB_YEAR = None
CFB_WEEK = None        # set None to use ESPN's current "active" week

A1_RANGE_RE = re.compile(r"^(?:[^!]+!)?([A-Z]+)(\d+):([A-Z]+)(\d+)$")

SPACER_ROWS_BETWEEN_LEAGUES = 4

def week_tag_explicit(league: str, kickoff_dt: datetime | None):
    # Prefer explicit ESPN week if provided; fall back to dates only if not set.
    if league == "ncaaf" and CFB_WEEK is not None:
        yr = CFB_YEAR or (kickoff_dt.year if kickoff_dt else datetime.now(ZoneInfo(TIMEZONE)).year)
        return f"{yr}-CFB-Wk{CFB_WEEK}"
    if league == "nfl" and NFL_WEEK is not None:
        yr = NFL_YEAR or (kickoff_dt.year if kickoff_dt else datetime.now(ZoneInfo(TIMEZONE)).year)
        return f"{yr}-NFL-Wk{NFL_WEEK}"
    return None

def _a1_last_row(a1_range: str) -> int:
    """
    Return the ending row number from an A1 range like 'Lines!A68:R69' or 'A68:R69'.
    """
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
    """
    Ensure each pair's TOP row is EVEN (2,4,6,...) because row 1 is the header.
    If a range starts on an odd row, bump both start and end by +1.
    Accepts a list of dicts:
      {"range": "Lines!A12:R13", "values": [[...],[...]]}
    Returns a new list with adjusted ranges.
    """
    adjusted = []
    for item in queued_ranges:
        a1 = item["range"]
        m = A1_RANGE_RE.match(a1)
        if not m:
            raise ValueError(f"Unrecognized A1 range: {a1}")
        col1, r1, col2, r2 = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))

        # If top is odd, move pair down one row (to even)
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
    """
    queued_ranges: list of {"range": "Lines!A68:R69", "values": [[...], [...]]}
    Returns the max end-row across all ranged writes.
    """
    return max(_a1_last_row(item["range"]) for item in queued_ranges) if queued_ranges else 0

# =========================
# === NORMALIZATION ===
# =========================
def normalize_rows_to_AH(rows):
    """
    Ensure every row is exactly 8 columns matching:
    [Logo, Team, Pick#, Line, Pick#, O/U, Date, Time]

    Accepts rows in either 6-col legacy form:
      [Logo, Team, Date, Time, Line, O/U]
    or already-correct 8-col form. Pads/truncates safely.
    """
    norm = []
    for r in rows:
        if r is None:
            continue
        r = list(r)

        # Trim only if longer than 8 (never shrink 8-wide rows)
        while len(r) > 8 and (r[-1] is None or str(r[-1]).strip() == ""):
            r.pop()

        if len(r) == 8:
            norm.append([r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]])
        elif len(r) == 6:
            # Legacy: [Logo, Team, Date, Time, Line, O/U]  -> remap
            logo, team, date_text, time_text, line, ou = r
            norm.append([logo, team, "", line, "", ou, date_text, time_text])
        else:
            # Heuristic: looks like [Logo, Team, Date, Time, Line, O/U, ...]
            if len(r) >= 6 and ("AM" in str(r[3]).upper() or "PM" in str(r[3]).upper()):
                logo, team, date_text, time_text, line, ou = r[:6]
                norm.append([logo, team, "", line, "", ou, date_text, time_text])
            else:
                # Fallback: pad/truncate
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
def build_college_logo_dict(sheet_id=None):
    sheet_id = sheet_id or COLLEGE_SHEET_ID
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

def build_college_abbreviation_dict(sheet_id=None):
    sheet_id = sheet_id or COLLEGE_SHEET_ID
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
    if include_nfl:
        u = (logo_url or "").lower()
        if "nyg" in u: return "NYG"
        if "nyj" in u: return "NYJ"
        if "lar" in u: return "LAR"
        if "lac" in u: return "LAC"
        for key, abbr in TEAM_ABBR.items():
            if key in team_name:
                return abbr
    return None

# >>> ADD THIS BLOCK (above scrape_nfl_schedule) >>>
def _season_year_for_date(dt: datetime) -> int:
    """
    NFL season year: Sep–Dec -> same year; Jan–Aug -> previous year.
    """
    return dt.year if dt.month >= 9 else dt.year - 1

def _int_or_none(s: str):
    try:
        return int(s)
    except Exception:
        return None
# <<< ADD THIS BLOCK <<<

# =========================
# === SCRAPERS (8-col A..H output) ===
# =========================
def scrape_nfl_schedule(year: int | None = None, week: int | None = None):
    """
    NFL scraper with robust time extraction:
      - scans each row for a time token (no fixed column)
      - navigates prev/next weeks up to 2 hops to land in the current football week (Mon..Mon)
    """
    print("Launching browser and scraping NFL schedule...")
    base_url = "https://www.espn.com/nfl/schedule"
    # >>> REPLACE THIS WHOLE url BLOCK >>>
    if week is not None:
        # explicit URL wins
        url = f"{base_url}/_/week/{week}" + (f"/year/{year}" if year is not None else "")
    else:
        # start at base; we’ll detect week/year and then jump to the explicit URL
        url = base_url
    # <<< END REPLACE <<<

    TIME_RE = re.compile(r"\b\d{1,2}:\d{2}\s*[AP]M\b", re.IGNORECASE)

    def _section_locators(page):
        for sel in ("div.ScheduleTables > div", "section.ScheduleTables > div", "div.ScheduleTables"):
            loc = page.locator(sel)
            if loc.count() > 0:
                print(f"Using sections selector: {sel} (count={loc.count()})")
                return sel, loc
        print("No obvious section wrapper found; using all TBODYs.")
        return None, page.locator("tbody")

    def _extract_rows_from_section(section):
        rows = []
        tbodys = section.locator("tbody")
        if tbodys.count() == 0:
            trs = section.locator("tr")
            for j in range(trs.count()):
                rows.append(trs.nth(j))
            return rows
        for tbi in range(tbodys.count()):
            trs = tbodys.nth(tbi).locator("tr")
            for j in range(trs.count()):
                rows.append(trs.nth(j))
        return rows

    def _get_team_text(cell):
        try:
            links = cell.locator("a[href*='/team/']")
            if links.count() == 0:
                links = cell.locator("span.Table__Team a")
            if links.count() > 0:
                return links.nth(links.count() - 1).text_content(timeout=1500).strip()
        except Exception:
            pass
        return (cell.text_content() or "").strip()

    def _get_logo_url(cell):
        try:
            img = cell.locator("img")
            if img.count() > 0:
                return img.first.get_attribute("src") or ""
        except Exception:
            pass
        return ""

    def _scan_time_from_row(row_locator):
        """Return first HH:MM AM/PM found anywhere in the row (string), else ''."""
        try:
            txt = (row_locator.text_content(timeout=800) or "").replace("\u00a0", " ")
            m = TIME_RE.search(txt)
            return m.group(0).upper().replace("  ", " ") if m else ""
        except Exception:
            return ""

    def _parse_odds_from_row(row_locator):
        line, ou = "N/A", "N/A"
        try:
            anchors = row_locator.locator("a")
            for k in range(anchors.count()):
                raw = (anchors.nth(k).text_content(timeout=600) or "").strip()
                t = raw.lower().replace("\u00bd", ".5")
                if t.startswith("line:") or t.startswith("spread:"):
                    line = raw.split(":", 1)[-1].strip()
                elif t.startswith("o/u:") or t.startswith("total:"):
                    ou = raw.split(":", 1)[-1].strip()
                elif re.match(r"^[A-Za-z]{2,4}\s*[+-]\d+(\.\d+)?$", raw):
                    line = raw.strip()
        except Exception:
            pass
        return line, ou

    def _collect_from_page(page):
        rows_out = []
        sections_sel, sections = _section_locators(page)
        if sections.count() == 0:
            return rows_out
        num_sections = sections.count() if sections_sel != "div.ScheduleTables" else 1
        for i in range(num_sections):
            section = sections.nth(i) if sections_sel != "div.ScheduleTables" else sections
            try:
                date_header = (section.locator(".Table__Title").first.text_content() or "").strip()
            except Exception:
                date_header = ""
            for row in _extract_rows_from_section(section):
                tds = row.locator("td")
                if tds.count() < 2:
                    continue
                away_cell = tds.nth(0); home_cell = tds.nth(1)
                away_team = _get_team_text(away_cell)
                home_team = _get_team_text(home_cell)
                if not away_team or not home_team:
                    continue

                # Robust time scan + aria-label fallback; skip if still missing
                game_time = _scan_time_from_row(row)
                if not game_time:
                    try:
                        aria = row.get_attribute("aria-label") or ""
                        m = TIME_RE.search(aria)
                        if m:
                            game_time = m.group(0).upper().replace("  ", " ")
                    except Exception:
                        pass
                if not game_time:
                    game_time = "N/A"


                away_logo_url = _get_logo_url(away_cell)
                home_logo_url = _get_logo_url(home_cell)
                away_logo_formula = get_logo_formula(away_team, away_logo_url, league="nfl")
                home_logo_formula = get_logo_formula(home_team, home_logo_url, league="nfl")

                line, ou = _parse_odds_from_row(row)

                def _abbr(team_name, logo_url):
                    u = (logo_url or "").lower()
                    if "nyg" in u: return "NYG"
                    if "nyj" in u: return "NYJ"
                    if "lar" in u: return "LAR"
                    if "lac" in u: return "LAC"
                    return find_abbreviation(team_name)

                away_abbr = _abbr(away_team, away_logo_url)
                home_abbr = _abbr(home_team, home_logo_url)

                away_line = home_line = "N/A"
                if line != "N/A" and away_abbr and home_abbr:
                    parts = line.split()
                    if len(parts) == 2:
                        favored_abbr, raw_spread = parts
                        try:
                            spread = float(raw_spread.replace("+", "").replace("-", ""))
                            s = f"{spread:.1f}".rstrip('0').rstrip('.')
                            if favored_abbr.upper() == away_abbr:
                                away_line = f"{away_abbr} -{s}"; home_line = f"{home_abbr} +{s}"
                            elif favored_abbr.upper() == home_abbr:
                                home_line = f"{home_abbr} -{s}"; away_line = f"{away_abbr} +{s}"
                        except ValueError:
                            pass

                ou_top = f"O {ou}" if ou != "N/A" else "N/A"
                ou_bottom = f"U {ou}" if ou != "N/A" else "N/A"

                rows_out.append([away_logo_formula, away_team, "", away_line, "", ou_top, date_header, game_time])
                rows_out.append([home_logo_formula, home_team, "", home_line, "", ou_bottom, date_header, game_time])

        print(f"Collected NFL rows: {len(rows_out)}")
        return rows_out

    def _rows_in_window(rows, start, end):
        hits = 0
        for i in range(0, len(rows), 2):
            dt = parse_kickoff_local(rows[i][6], rows[i][7], TIMEZONE)
            if dt and (start <= dt < end):
                hits += 1
        return hits

    tz = ZoneInfo(TIMEZONE)
    week_start = (datetime.now(tz) - timedelta(days=datetime.now(tz).weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    week_end = week_start + timedelta(days=7)

    print(f"DEBUG week window local: {week_start.strftime('%Y-%m-%d %H:%M')} → {week_end.strftime('%Y-%m-%d %H:%M')} ({TIMEZONE})")
    print(f"DEBUG initial target URL: {url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, timeout=60000)
        # >>> INSERT: force explicit week when none was provided >>>
        if week is None:
            # Try to read the selected week from the page (week picker/tab)
            # Fall back to computing week/year deterministically.
            try:
                page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=15000)
            except Exception:
                pass

            # Attempt DOM-based detection of week text like "Week 2"
            detected_week = None
            detected_year = None

            try:
                # common patterns: selected tab has aria-current or a selected class
                candidates = page.locator("[aria-current='true'], .is-selected, .tabs__item.is-active, .Pagination__Button--isActive")
                if candidates.count() > 0:
                    txt = (candidates.first.text_content() or "").strip()
                    # find a number inside "Week 2" etc.
                    m = re.search(r"\bWeek\s+(\d{1,2})\b", txt, flags=re.IGNORECASE)
                    if m:
                        detected_week = _int_or_none(m.group(1))
            except Exception:
                pass

            # Determine season year robustly
            now_local = datetime.now(ZoneInfo(TIMEZONE))
            detected_year = _season_year_for_date(now_local)

            # If we still did not detect week from DOM, compute week by football window (Mon..Mon) anchor = Thursday
            if detected_week is None:
                # anchor week number from ISO week of the Thursday in the current football week window
                wk_mon = (now_local - timedelta(days=now_local.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
                thursday = wk_mon + timedelta(days=3)
                # ESPN's “week” numbering isn’t ISO; but jumping to an explicit week URL is still safer than the active page.
                # If this guess is off by 1, our prev/next snapshot below will still recover.
                detected_week = thursday.isocalendar().week  # best-effort anchor

            explicit_url = f"{base_url}/_/week/{detected_week}/year/{detected_year}"
            print(f"DEBUG forcing explicit NFL URL: {explicit_url}")
            page.goto(explicit_url, timeout=60000)
            # Strong readiness: container + any row-like element
            page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=30000)
            # >>> INSERT: nudge virtualization to mount rows >>>
            try:
                page.mouse.wheel(0, 400)
                page.wait_for_timeout(150)
                page.mouse.wheel(0, -400)
                page.wait_for_timeout(150)
            except Exception:
                pass
            # <<< END INSERT <<<

            # also ensure at least one row is mounted
            page.wait_for_function(
                """() => !!document.querySelector("tbody tr, tr.Table__TR, [role='row']")""",
                timeout=10000
            )
            page.wait_for_timeout(300)
        # <<< END INSERT <<<

        page.wait_for_selector("body", timeout=15000)
        page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=30000)
        page.wait_for_timeout(500)
                # >>> INSERT: require at least one row-like element before we start collecting >>>
        try:
            page.wait_for_function(
                """() => !!document.querySelector("tbody tr, tr.Table__TR, [role='row']")""",
                timeout=8000
            )
        except Exception:
            pass
        # <<< END INSERT <<<
       
        snapshots = []

        # current
        cur = _collect_from_page(page); snapshots.append(("current", cur, 0))
        print(f"DEBUG pre-filter (current): {len(cur)} rows scraped")
        print(f"DEBUG in-window (current): {_rows_in_window(cur, week_start, week_end)} rows")

        # prev
        try:
            prv = page.locator("button[aria-label='Previous Week'], a[aria-label='Previous Week']")
            if prv.count() > 0:
                prv.first.click()
                page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=15000); page.wait_for_timeout(300)

                prev_rows = _collect_from_page(page); snapshots.append(("prev", prev_rows, -1))
                # back to current
                nxt = page.locator("button[aria-label='Next Week'], a[aria-label='Next Week']")
                if nxt.count() > 0:
                    nxt.first.click()
                    page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=15000); page.wait_for_timeout(300)

        except Exception:
            pass

        # next
        try:
            nxt = page.locator("button[aria-label='Next Week'], a[aria-label='Next Week']")
            if nxt.count() > 0:
                nxt.first.click()
                page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=15000); page.wait_for_timeout(300)

                next_rows = _collect_from_page(page); snapshots.append(("next", next_rows, +1))
                # next-next
                if nxt.count() > 0:
                    nxt.first.click()
                    page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=15000); page.wait_for_timeout(300)
                    nn_rows = _collect_from_page(page); snapshots.append(("next2", nn_rows, +2))
        except Exception:
            pass

        # choose the snapshot with the most games inside this Mon..Mon window
        scored = []
        for name, rows, offset in snapshots:
            hits = _rows_in_window(rows, week_start, week_end)
            print(f"Snapshot '{name}': {hits} in-window out of {len(rows)} rows")
            scored.append((hits, name, rows))
        scored.sort(reverse=True)

        best_hits, best_name, best_rows = scored[0] if scored else (0, "current", cur)
        print(f"Chosen snapshot: {best_name} (hits={best_hits})")
        # --- Fallback: brute-force weeks if we still have 0 hits --------------------
        if best_hits == 0:
            print("DEBUG fallback: probing explicit week URLs 1..23 for the current season…")
            now_local = datetime.now(ZoneInfo(TIMEZONE))
            season_year = _season_year_for_date(now_local)

            def _collect_for_week(w):
                try:
                    explicit = f"{base_url}/_/week/{w}/year/{season_year}"
                    print(f"DEBUG fallback probing: {explicit}")
                    page.goto(explicit, timeout=60000)
                    page.wait_for_selector("div.ScheduleTables, .ScheduleTables", state="visible", timeout=15000)
                    try:
                        page.mouse.wheel(0, 400); page.wait_for_timeout(120)
                        page.mouse.wheel(0, -400); page.wait_for_timeout(120)
                    except Exception:
                        pass
                    page.wait_for_timeout(150)
                    rows = _collect_from_page(page)
                    hits = _rows_in_window(rows, week_start, week_end)
                    print(f"DEBUG fallback week {w}: {hits} in-window out of {len(rows)} rows")
                    return hits, rows, w
                except Exception:
                    return 0, [], w

            probe_best_hits, probe_best_rows, probe_best_w = 0, [], None
            for w in range(1, 24):  # NFL regular + early postseason ceiling
                hits, rows, wnum = _collect_for_week(w)
                if hits > probe_best_hits:
                    probe_best_hits, probe_best_rows, probe_best_w = hits, rows, wnum
                    if hits >= 8:  # good enough; stop early
                        break

            if probe_best_hits > 0:
                print(f"DEBUG fallback chose week {probe_best_w} with {probe_best_hits} hits.")
                best_hits, best_name, best_rows = probe_best_hits, f"week{probe_best_w}", probe_best_rows
            else:
                print("DEBUG fallback found no in-window games across weeks 1..23.")
        # --- end fallback -----------------------------------------------------------


        browser.close()
        return best_rows

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
        browser = p.chromium.launch(headless=True)
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
                            t = raw.lower().replace("\u00bd", ".5")  # handle ½
                            # Accept multiple labels that ESPN uses
                            if t.startswith("line:") or t.startswith("spread:"):
                                line = raw.split(":", 1)[-1].strip()
                            elif t.startswith("o/u:") or t.startswith("total:"):
                                ou = raw.split(":", 1)[-1].strip()
                            # Fallback: sometimes the anchor is just "ILL -45.5"
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

                    # A..H: Logo, Team, Pick#, Line, Pick#, O/U, Date, Time
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
        # Normalize odd spacings like "A M" / "P  M", NBSPs, double spaces
        tt = (time_text or "").replace("\u00a0", " ").upper()
        tt = re.sub(r"\s+", " ", tt).strip()
        tt = tt.replace("A M", "AM").replace("P M", "PM")

        # Accept with or without a space before AM/PM
        t = re.search(r"(\d{1,2}:\d{2})\s*(AM|PM)", tt, re.IGNORECASE)
        if not t:
            return None
        timestr = f"{t.group(1)}{t.group(2)}"
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

def make_game_key(kickoff_dt: datetime | None, away_name_display: str, home_name_display: str, espn_game_id: str | None = None) -> str:
    if espn_game_id:
        return espn_game_id
    dt_part = kickoff_dt.strftime("%Y-%m-%d") if kickoff_dt else "0000-00-00"
    away = normalize_team_for_key(away_name_display)
    home = normalize_team_for_key(home_name_display)
    return f"{dt_part}|{away}|{home}"

def pack_pairs_to_games(row_pairs):
    """
    Input rows MUST be normalized to A..H:
      A Logo, B Team, C Pick#, D Line, E Pick#, F O/U, G Date, H Time
    """
    games = []
    for i in range(0, len(row_pairs), 2):
        if i + 1 >= len(row_pairs):
            break
        away = row_pairs[i]
        home = row_pairs[i + 1]
        games.append({
            "away_row": away,            # full A..H
            "home_row": home,            # full A..H
            "away_team_disp": away[1],   # B
            "home_team_disp": home[1],   # B
            "date_text": away[6],        # G
            "time_text": away[7],        # H
            "away_line": away[3],        # D
            "home_line": home[3],        # D
            "ou_top": away[5],           # F
            "ou_bottom": home[5],        # F
        })
    return games

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
    """
    Detect rows where column G ('Date') actually contains 'ncaaf'/'nfl' from old 6-col writes.
    Deletes those pairs in contiguous blocks (top+bottom row per game).
    Returns number of rows deleted.
    """
    vals = lines_ws.get_all_values()
    if not vals or len(vals) < 2:
        return 0

    bad_tops = []
    for r in range(2, len(vals)+1, 2):
        row = vals[r-1]
        if len(row) >= 7:
            g_val = str(row[6]).strip().lower()
            if g_val in ("ncaaf", "nfl"):  # League ended up under Date
                bad_tops.append(r)

    if not bad_tops:
        return 0

    # Group into contiguous blocks so we can delete in fewer calls
    blocks = []
    start = None
    prev = None
    for top in bad_tops:
        if start is None:
            start = top
            prev = top
        elif top == prev + 2:
            prev = top
        else:
            blocks.append((start, prev+1))  # include second row of last pair
            start = top
            prev = top
    blocks.append((start, prev+1))

    # Delete bottom-up so indices don't shift
    deleted = 0
    for s, e in reversed(blocks):
        lines_ws.delete_rows(s, e)
        deleted += (e - s + 1)
    return deleted

def _football_week_monday(dt: datetime) -> datetime:
    """Return Monday 00:00 of dt's week in TIMEZONE (local)."""
    dt = dt.astimezone(ZoneInfo(TIMEZONE))
    wkmon = (dt - timedelta(days=dt.weekday()))
    return wkmon.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=ZoneInfo(TIMEZONE))

def _purge_lines_to_current_week(spreadsheet, now_dt: datetime, phase_cfb: str, phase_nfl: str):
    """
    NEW POLICY:
      - Define the current football week as Monday 00:00 local -> next Monday 00:00 (exclusive).
      - KEEP any pair whose KickoffLocal (col M) falls within this window.
      - DELETE everything else (even if Locked = Y).
      - Blank/invalid KickoffLocal => delete.
    """
    try:
        lines_ws = spreadsheet.worksheet("Lines")
    except gspread.exceptions.WorksheetNotFound:
        return

    vals = lines_ws.get_all_values()
    if not vals or len(vals) < 2:
        return

    COL_KICK = 12  # M "YYYY-MM-DD HH:MM" local

    week_start = _football_week_monday(now_dt)
    week_end = week_start + timedelta(days=7)  # exclusive

    def parse_kick(s: str) -> datetime | None:
        s = (s or "").strip()
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(TIMEZONE))
        except Exception:
            return None

    to_delete_tops = []
    for top in range(2, len(vals) + 1, 2):  # pair tops
        row_top = vals[top - 1]
        if len(row_top) < COL_KICK:
            to_delete_tops.append(top)
            continue
        k = parse_kick(row_top[COL_KICK - 1])
        if not k or not (week_start <= k < week_end):
            to_delete_tops.append(top)

    if not to_delete_tops:
        print("Purge: nothing to delete (current-week filter).")
        return

    # group contiguous 2-row blocks and delete bottom-up
    blocks, start, prev = [], None, None
    for t in to_delete_tops:
        if start is None:
            start = prev = t
        elif t == prev + 2:
            prev = t
        else:
            blocks.append((start, prev + 1))
            start = prev = t
    blocks.append((start, prev + 1))

    for s, e in reversed(blocks):
        lines_ws.delete_rows(s, e)

    print("Purge: completed (kept only current football week).")

def queue_pair_range(ws_title: str, top_row: int, full_away: list, full_home: list):
    """
    Build an A1 range for a 2-row pair (A..R).
    """
    a1 = f"{ws_title}!A{top_row}:R{top_row+1}"
    # trim/pad rows to 18 cols just in case
    full_away = (full_away + [""] * 18)[:18]
    full_home = (full_home + [""] * 18)[:18]
    return {"range": a1, "values": [full_away, full_home]}

def upsert_lines_strict(lines_ws, queued_ranges, value_input_option="USER_ENTERED"):
    """
    Strict sequence: align pairs → refresh ws → compute capacity → resize → batch write.
    Returns (written_ranges, max_row_needed).
    """
    if not queued_ranges:
        return [], 0

    ss = lines_ws.spreadsheet
    # 🔄 refresh the worksheet to avoid stale row_count after purge/deletes
    lines_ws = ss.worksheet(lines_ws.title)

    # Align to even row tops (since header is row 1)
    q_adj = _normalize_pair_alignment(queued_ranges)

    # Sanity logs
    print(f"First queued: {q_adj[0]['range']}")
    print(f"Last  queued: {q_adj[-1]['range']}")

    max_row_needed = compute_max_row_needed(q_adj)
    fresh_rows, fresh_cols = lines_ws.row_count, lines_ws.col_count
    print(f"Current grid rows={fresh_rows}, cols={fresh_cols}")
    print(f"Max row needed={max_row_needed}, Needed cols=18")

    # Grow BEFORE write (use fresh counts)
    grow_rows = max(max_row_needed, fresh_rows)
    grow_cols = max(18, fresh_cols)
    if grow_rows > fresh_rows or grow_cols > fresh_cols:
        print(f"Resizing grid: rows {fresh_rows}→{grow_rows}, cols {fresh_cols}→{grow_cols}")
        lines_ws.resize(rows=grow_rows, cols=grow_cols)
        # refresh again after resize, just to be safe
        lines_ws = ss.worksheet(lines_ws.title)
    else:
        print(f"No resize needed (rows={fresh_rows}, cols={fresh_cols})")

    # Defensive assert
    assert lines_ws.row_count >= max_row_needed, (
        f"Grid still too small: rows={lines_ws.row_count}, needed={max_row_needed}"
    )

    # Batch write through the SAME spreadsheet
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
    staging_rows: flat list of rows (away, home, away, home, ...)
    Merge into 'Lines' by GameKey with placeholders, release/freeze windows, and locks.
    Now:
      - Purge keeps only current football week (Mon..Mon).
      - When appending CFB after any existing data, insert 4 blank rows.
    """
    tz = ZoneInfo(TIMEZONE)
    now = datetime.now(tz)

    # Normalize to A..H
    staging_rows = normalize_rows_to_AH(staging_rows)

    # Ensure ws + headers
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

    # Purge to current football week only (deletes even Locked=Y if out-of-week)
    _purge_lines_to_current_week(
        spreadsheet,
        now_dt=now,
        phase_cfb=phase if league == "ncaaf" else PHASE_CFB,
        phase_nfl=phase if league == "nfl"   else PHASE_NFL
    )

    # Refresh + read existing
    lines_ws = spreadsheet.worksheet("Lines")
    existing = lines_ws.get_all_values()

    # --- Spacer: if appending CFB and there is any existing data, add 4 rows ---
    spacer_rows = 0
    if league == "ncaaf" and len(existing) > 1:
        spacer_rows = SPACER_ROWS_BETWEEN_LEAGUES

    # Index existing games by GameKey (L)
    gamekey_col = 12
    index = {}
    for r in range(2, len(existing)+1, 2):
        row_vals = existing[r-1]
        if len(row_vals) >= gamekey_col:
            gk = row_vals[gamekey_col-1]
            if gk:
                index[gk] = r

    games = pack_pairs_to_games(staging_rows)

    def fmt(dt):
        return dt.astimezone(ZoneInfo(TIMEZONE)).strftime("%Y-%m-%d %H:%M") if dt else ""

    last_row_with_data = len(existing) if existing else 1
    append_top = last_row_with_data + 1 + spacer_rows
    if append_top < 2:
        append_top = 2
    if append_top % 2 == 1:
        append_top += 1

    queued_ranges, touched_new, touched_upd = [], [], []

    for g in games:
        kickoff_dt = parse_kickoff_local(g["date_text"], g["time_text"], TIMEZONE)
        release_at, freeze_at = compute_release_freeze(kickoff_dt, league, phase)
        week_tag = week_tag_explicit(league, kickoff_dt) or compute_week_tag(kickoff_dt, league, phase)
        game_key = make_game_key(kickoff_dt, g["away_team_disp"], g["home_team_disp"], espn_game_id=None)

        locked_flag = "N"
        status = "placeholder"
        allow_lines = False

        window_ok = publish_window_allows(now, kickoff_dt)

        # Window+time gates
        if window_ok and release_at and now >= release_at:
            status = "posted"; allow_lines = True
        if window_ok and freeze_at and now >= freeze_at:
            locked_flag = "Y"; status = "locked"; allow_lines = True

        a = g["away_row"][:]
        h = g["home_row"][:]

        # Strip lines/O-U for brand new rows if not allowed yet
        if not allow_lines and game_key not in index:
            a[3] = ""; h[3] = ""; a[5] = ""; h[5] = ""
        else:
            for row in (a, h):
                if row[3] == "N/A": row[3] = ""
                if row[5] == "N/A": row[5] = ""

        meta = [
            ("ncaaf" if league == "ncaaf" else "nfl"),   # I
            week_tag,                                    # J
            phase,                                       # K
            game_key,                                    # L
            fmt(kickoff_dt),                             # M
            fmt(release_at),                             # N
            fmt(freeze_at),                              # O
            locked_flag,                                 # P
            status,                                      # Q
            fmt(now),                                    # R
        ]

        full_away = a + meta
        full_home = h + meta

        if game_key not in index:
            rng = queue_pair_range(lines_ws.title, append_top, full_away, full_home)
            queued_ranges.append(rng); touched_new.append(rng["range"])
            index[game_key] = append_top
            append_top += 2
        else:
            top_row = index[game_key]
            locked_cell = None
            try:
                locked_cell = lines_ws.get_value(f"P{top_row}")
            except Exception:
                pass
            if (not TEST_MODE_IGNORE_LOCKS) and locked_cell and str(locked_cell).upper() == "Y":
                continue
            if not allow_lines:
                continue
            upd = queue_pair_range(lines_ws.title, top_row, full_away, full_home)
            queued_ranges.append(upd); touched_upd.append(upd["range"])

    # Strict, ordered write
    written_ranges, max_row_needed = upsert_lines_strict(lines_ws, queued_ranges)

    # Formatting for touched ranges
    if written_ranges:
        _ensure_grid_capacity(lines_ws, needed_rows=max_row_needed, needed_cols=18)
        with batch_updater(lines_ws.spreadsheet) as batch:
            right_border_thick = Border("SOLID_THICK")
            light_orange = Color(1.0, 0.898, 0.8)
            light_blue = Color(0.8, 0.898, 1.0)
            for rng in written_ranges:
                m = re.match(r".*A(\d+):R(\d+)", rng)
                if not m: continue
                r1, r2 = int(m.group(1)), int(m.group(2))
                batch.format_cell_range(lines_ws, f"A{r1}:H{r1}", CellFormat(borders=Borders(top=Border("SOLID"))))
                batch.format_cell_range(lines_ws, f"A{r2}:H{r2}", CellFormat(borders=Borders(bottom=Border("SOLID"))))
                batch.format_cell_range(lines_ws, f"A{r1}:A{r2}", CellFormat(borders=Borders(left=Border("SOLID"))))
                batch.format_cell_range(lines_ws, f"H{r1}:H{r2}", CellFormat(borders=Borders(right=Border("SOLID"))))
                batch.format_cell_range(lines_ws, f"C{r1}:D{r2}", CellFormat(backgroundColor=light_orange))
                batch.format_cell_range(lines_ws, f"E{r1}:F{r2}", CellFormat(backgroundColor=light_blue))
                batch.format_cell_range(lines_ws, f"B{r1}:B{r2}", CellFormat(borders=Borders(right=right_border_thick)))
                batch.format_cell_range(lines_ws, f"D{r1}:D{r2}", CellFormat(borders=Borders(right=right_border_thick)))
                batch.format_cell_range(lines_ws, f"F{r1}:F{r2}", CellFormat(borders=Borders(right=right_border_thick)))

def upload_via_staging_and_merge(data_rows, league: str, phase: str):
    """
    Create temp_Lines, write normalized A..H rows there,
    then merge into Lines and delete temp_Lines.
    """
    # ✅ Normalize first so the temp sheet mirrors what the merge expects
    data_rows = normalize_rows_to_AH(data_rows)

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(PICK_SHEET_ID)

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
        # minimal visual
        format_cell_range(ws, "A1:H1", CellFormat(textFormat=TextFormat(bold=True), horizontalAlignment='CENTER'))
        center_align = CellFormat(horizontalAlignment='CENTER', verticalAlignment='MIDDLE')
        for col in ["C", "D", "E", "F", "G", "H"]:
            format_cell_range(ws, f"{col}2:{col}{len(data_rows)+1}", center_align)

    # ✅ Merge into Lines (this was missing)
    merge_staging_into_lines(spreadsheet, data_rows, league=league, phase=phase)

    # ✅ Delete temp sheet when done (was accidentally pasted elsewhere)
    try:
        spreadsheet.del_worksheet(ws)
    except Exception:
        pass

def publish_window_allows(now_dt: datetime, kickoff_dt: datetime | None) -> bool:
    """
    Pure day-of-week policy:
      - Run on TUESDAY: permit Thu & Fri games in the same football week window.
      - Run on THURSDAY: permit Sat, Sun & *next* Monday games in the same window.
      - Other days: publish nothing new.
    """
    if kickoff_dt is None:
        return True

    tz = now_dt.tzinfo
    kdate = kickoff_dt.astimezone(tz).date()

    # Week starts Monday
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
    # Optional stub for later
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
        print(f"Using NFL year/week: {NFL_YEAR}/{NFL_WEEK} (overridden={bool(os.environ.get('NFL_YEAR_OVERRIDE') or os.environ.get('NFL_WEEK_OVERRIDE'))})")
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
