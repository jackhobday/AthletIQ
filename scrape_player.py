# scrape_player.py
# Usage:
#   pip install httpx[http2] beautifulsoup4 lxml rapidfuzz tenacity python-slugify
#   # optional search fallback:
#   # pip install google-search-results  (and set SERPAPI_KEY)
#   python scrape_player.py "Abdirasak Bulale" "St. Olaf College"

import asyncio, re, os, json
from typing import Optional, Dict, Any, List, Tuple
import httpx
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from tenacity import retry, wait_exponential, stop_after_attempt
from slugify import slugify

SPORT_PATH = "mens-soccer"
DEFAULT_HEADERS = {
    "User-Agent": "RecruitScoutBot/0.2 (+contact: you@yourdomain.com)",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = httpx.Timeout(20.0)

SCHOOL_TO_ATHLETICS = {
    "st. olaf college": "athletics.stolaf.edu",
    "saint olaf": "athletics.stolaf.edu",
    "st olaf": "athletics.stolaf.edu",
    "macalester college": "athletics.macalester.edu",
    "macalester": "athletics.macalester.edu",
    "gustavus adolphus college": "gogusties.com",
    "gustavus": "gogusties.com",
    "carleton college": "athletics.carleton.edu",
    "carleton": "athletics.carleton.edu",
    "augsburg university": "athletics.augsburg.edu",
    "augsburg": "athletics.augsburg.edu",
    "bethel university": "athletics.bethel.edu",
    "bethel": "athletics.bethel.edu",
    "hamline university": "hamlineathletics.com",
    "hamline": "hamlineathletics.com",
    "saint john's university": "gojohnnies.com",
    "saint johns": "gojohnnies.com",
    "st. john's": "gojohnnies.com",
    "st johns": "gojohnnies.com",
    "saint mary's university of minnesota": "saintmaryssports.com",
    "saint marys": "saintmaryssports.com",
    "st. mary's": "saintmaryssports.com",
    "st marys": "saintmaryssports.com",
    "the college of st. scholastica": "csssaints.com",
    "st. scholastica": "csssaints.com",
    "st scholastica": "csssaints.com",
    "scholastica": "csssaints.com",
}

# ---------------- Utilities ----------------

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def norm_key(s: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", (s or "").lower())

def feet_in_to_cm(text: str) -> Optional[int]:
    m = re.search(r"(?P<f>\d)\s*[-'’]\s*(?P<i>\d{1,2})", text)
    if not m:
        m = re.search(r"\b(?P<f>\d)\s*ft\.?\s*(?P<i>\d{1,2})?\s*in\.?\b", text, re.I)
    if not m:
        m = re.search(r"\b(?P<f>\d)\s*['’]\s*(?P<i>\d{1,2})?\b", text)
    if not m:
        return None
    f = int(m.group("f")); i = int(m.group("i") or 0)
    return round((f * 12 + i) * 2.54)

def absolutize(base: str, maybe: Optional[str]) -> Optional[str]:
    if not maybe:
        return None
    try:
        return str(httpx.URL(base).join(maybe))
    except Exception:
        return maybe

def best_match(target: str, options: List[str]) -> Optional[str]:
    target_low = target.lower()
    scored: List[Tuple[int,str]] = []
    for opt in options:
        if not opt: continue
        scored.append((fuzz.token_set_ratio(target_low, opt.lower()), opt))
    if not scored: return None
    scored.sort(reverse=True, key=lambda x: x[0])
    # require a reasonable score to avoid “St. Olaf College Athletics”
    return scored[0][1] if scored[0][0] >= 70 else None

@retry(wait=wait_exponential(multiplier=1, min=1, max=8), stop=stop_after_attempt(3))
async def fetch(client: httpx.AsyncClient, url: str) -> httpx.Response:
    r = await client.get(url, headers=DEFAULT_HEADERS, follow_redirects=True)
    r.raise_for_status()
    return r

# ---------------- Search fallback (optional) ----------------

async def search_profile_by_web(name: str, athletics_domain: str, sport_hint: str = "men's soccer") -> Optional[str]:
    key = os.environ.get("SERPAPI_KEY")
    if not key:
        return None
    import serpapi
    q = f'site:{athletics_domain} "{name}" roster {sport_hint}'
    search = serpapi.GoogleSearch({"q": q, "hl": "en", "api_key": key})
    data = search.get_dict()
    results = data.get("organic_results") or []

    def score(url: str, title: str, snippet: str) -> int:
        s = 0
        if "/sports/" in url and "/roster/" in url: s += 4
        if SPORT_PATH in url: s += 3
        txt = f"{title} {snippet}"
        if fuzz.partial_ratio(name.lower(), txt.lower()) > 90: s += 3
        return s

    ranked = sorted(((r["link"], score(r["link"], r.get("title",""), r.get("snippet",""))) for r in results),
                    key=lambda x: x[1], reverse=True)
    return ranked[0][0] if ranked and ranked[0][1] >= 4 else None

# ---------------- SIDEARM: find profile ----------------

async def sidearm_find_profile(client: httpx.AsyncClient, domain: str, name: str) -> Optional[str]:
    # 1) Site search
    qp = httpx.QueryParams({"query": name})
    search_url = f"https://{domain}/search?{qp}"
    try:
        r = await fetch(client, search_url)
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select('a[href*="/sports/"]'):
            href = a.get("href","")
            if "/roster/" in href and SPORT_PATH in href:
                if fuzz.partial_ratio(name.lower(), a.get_text(" ").lower()) >= 90:
                    return str(r.url.join(href))
    except Exception:
        pass

    # 2) Roster pages (current + recent)
    seasons = ["", "2024-25", "2023-24"]
    patterns = [
        f"https://{domain}/sports/{SPORT_PATH}/roster",
        f"https://{domain}/sports/{SPORT_PATH}/roster?view=2",
    ]
    roster_urls = set()
    for base in patterns:
        roster_urls.add(base)
        for yr in seasons:
            if yr:
                roster_urls.add(base.rstrip("/") + f"/{yr}")

    for url in roster_urls:
        try:
            r = await fetch(client, url)
        except Exception:
            continue
        soup = BeautifulSoup(r.text, "lxml")
        candidates = []
        for a in soup.select('a[href*="/sports/"][href*="/roster/"]'):
            text = norm(a.get_text(" "))
            if not text: continue
            score = fuzz.token_set_ratio(name.lower(), text.lower())
            if score >= 85:
                candidates.append((score, str(r.url.join(a.get("href")))))
        if candidates:
            candidates.sort(reverse=True)
            return candidates[0][1]
    return None

# ---------------- SIDEARM: fetch team stats for individual player ----------------

async def fetch_player_stats_from_team_page(client: httpx.AsyncClient, domain: str, player_name: str, sport_path: str = SPORT_PATH) -> List[Dict[str, Any]]:
    """Fetch individual player statistics from the team stats page for multiple seasons."""
    try:
        # Try multiple seasons: 2024, 2023, 2022
        seasons = ["2024", "2023", "2022"]
        all_stats_rows = []
        
        for season in seasons:
            try:
                # Try season-specific URL
                stats_url = f"https://{domain}/sports/{sport_path}/stats/{season}"
                print(f"DEBUG: Trying season {season} at {stats_url}")
                
                r = await fetch(client, stats_url)
                soup = BeautifulSoup(r.text, "lxml")
                
                # Look for individual offensive statistics table
                for table in soup.select("table"):
                    # Check if this table has the right headers for individual stats
                    headers = [norm(th.get_text(" ")).lower() for th in table.select("thead th")]
                    if not headers:
                        headers = [norm(th.get_text(" ")).lower() for th in table.select("tr th")]
                    
                    # Look for tables with individual player stats (should have jersey numbers and player names)
                    if any(h in ["#", "player", "name"] for h in headers) and any(h in ["gp", "g", "a", "pts"] for h in headers):
                        for tr in table.select("tbody tr"):
                            tds = tr.select("td")
                            if not tds or len(tds) < 3:
                                continue
                            
                            cells = [norm(td.get_text(" ")) for td in tds]
                            if len(cells) < len(headers):
                                continue
                            
                            # Check if this row contains our player
                            row_text = " ".join(cells).lower()
                            player_name_lower = player_name.lower()
                            
                            # Try to match player name in the row
                            if (fuzz.partial_ratio(player_name_lower, row_text) >= 85 or 
                                any(name_part in row_text for name_part in player_name_lower.split())):
                                
                                # Create a stats row with the available data
                                row_data = dict(zip(headers[:len(cells)], cells))
                                
                                # Add season info
                                row_data["_season"] = season
                                row_data["_source"] = "team_stats_page"
                                
                                all_stats_rows.append(row_data)
                                print(f"DEBUG: Found stats for {season}")
                                break  # Found our player for this season
                
            except Exception as e:
                print(f"DEBUG: Error fetching season {season}: {e}")
                continue
        
        # If no season-specific data found, try the main stats page
        if not all_stats_rows:
            print(f"DEBUG: No season-specific data found, trying main stats page...")
            stats_url = f"https://{domain}/sports/{sport_path}/stats"
            r = await fetch(client, stats_url)
            soup = BeautifulSoup(r.text, "lxml")
            
            for table in soup.select("table"):
                headers = [norm(th.get_text(" ")).lower() for th in table.select("thead th")]
                if not headers:
                    headers = [norm(th.get_text(" ")).lower() for th in table.select("tr th")]
                
                if any(h in ["#", "player", "name"] for h in headers) and any(h in ["gp", "g", "a", "pts"] for h in headers):
                    for tr in table.select("tbody tr"):
                        tds = tr.select("td")
                        if not tds or len(tds) < 3:
                            continue
                        
                        cells = [norm(td.get_text(" ")) for td in tds]
                        if len(cells) < len(headers):
                            continue
                        
                        row_text = " ".join(cells).lower()
                        player_name_lower = player_name.lower()
                        
                        if (fuzz.partial_ratio(player_name_lower, row_text) >= 85 or 
                            any(name_part in row_text for name_part in player_name_lower.split())):
                            
                            row_data = dict(zip(headers[:len(cells)], cells))
                            row_data["_season"] = "2024"  # Assume current season
                            row_data["_source"] = "team_stats_page"
                            
                            all_stats_rows.append(row_data)
                            print(f"DEBUG: Found stats from main page")
                            break
        
        return all_stats_rows
        
    except Exception as e:
        print(f"Error fetching from team stats page: {e}")
        return []

def guess_provider_from_html(html: str) -> Optional[str]:
    low = html.lower()
    if "sidearm" in low: return "sidearm"
    if "prestosports" in low or "presto" in low: return "presto"
    return None

# ---------------- SIDEARM: parse profile ----------------

def parse_sidearm_profile(html_text: str, page_url: str, input_name: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text, "lxml")

    # --- Name: try many sources, then pick the one closest to input_name
    # First, try the specific Sidearm player name structure
    player_name_span = soup.select_one('.sidearm-roster-player-name')
    if player_name_span:
        first_name_span = player_name_span.select_one('.sidearm-roster-player-first-name')
        last_name_span = player_name_span.select_one('.sidearm-roster-player-last-name')
        if first_name_span and last_name_span:
            first_name = norm(first_name_span.get_text(" "))
            last_name = norm(last_name_span.get_text(" "))
            player_name = f"{first_name} {last_name}"
            if player_name:
                name_final = player_name
            else:
                # Fallback to the full span text
                name_final = norm(player_name_span.get_text(" "))
        else:
            # Fallback to the full span text
            name_final = norm(player_name_span.get_text(" "))
    else:
        # Fallback to other sources if the specific structure isn't found
        h1 = soup.select_one("h1")
        og_title = soup.select_one('meta[property="og:title"]')
        twitter_title = soup.select_one('meta[name="twitter:title"]')

        # JSON-LD person?
        jsonld_names = []
        for tag in soup.select('script[type="application/ld+json"]'):
            try:
                data = json.loads(tag.text)
                if isinstance(data, dict) and data.get("@type") in ("Person","Athlete"):
                    if "name" in data: jsonld_names.append(norm(data["name"]))
                if isinstance(data, list):
                    for d in data:
                        if isinstance(d, dict) and d.get("@type") in ("Person","Athlete") and "name" in d:
                            jsonld_names.append(norm(d["name"]))
            except Exception:
                pass

        # breadcrumb last item
        crumb = None
        crumbs = soup.select('nav[aria-label*="breadcrumb"] li, .breadcrumb li, .breadcrumbs li')
        if crumbs:
            crumb = norm(crumbs[-1].get_text(" "))

        title_texts = []
        if h1: title_texts.append(norm(h1.get_text(" ")))
        if og_title and og_title.get("content"): title_texts.append(norm(og_title["content"].split(" - ")[0]))
        if twitter_title and twitter_title.get("content"): title_texts.append(norm(twitter_title["content"].split(" - ")[0]))
        title_texts.extend(jsonld_names)
        if crumb: title_texts.append(crumb)

        name_final = best_match(input_name, title_texts) or (title_texts[0] if title_texts else None)

    # --- Attributes
    meta_text = " ".join(x.get_text(" ") for x in soup.select(
        ".sidearm-roster-player-attributes li, .c-roster-bio__info li, .sidearm-roster__player-details li, .c-player-bio__list li"
    ))
    height_cm = feet_in_to_cm(meta_text) or feet_in_to_cm(soup.get_text(" "))

    def find_label_value(labels: List[str]) -> Optional[str]:
        for lab in labels:
            node = soup.find(string=re.compile(rf"\b{lab}\b", re.I))
            if node:
                nxt = node.find_next()
                if nxt:
                    return norm(nxt.get_text(" "))
        return None

    # Sidearm also encodes some attrs as labeled items with classes
    position = find_label_value(["Position", "Pos.", "POS", "Position(s)"])
    if not position:
        clspos = soup.select_one('[class*="position"]')
        if clspos: position = norm(clspos.get_text(" "))

    class_year = find_label_value(["Class", "Year", "Academic Year"])
    hometown = find_label_value(["Hometown"])

    # --- Headshot: src, data-src, or srcset; prefer image in header/figure
    headshot = None
    img_candidates = []
    img_candidates += soup.select('.sidearm-roster-player img, .c-player-bio img, figure img, .c-player-header img, img[alt*="headshot" i]')
    for img in img_candidates:
        src = img.get("src") or img.get("data-src")
        if not src:
            srcset = img.get("srcset")
            if srcset:
                src = srcset.split(",")[0].strip().split(" ")[0]
        if src:
            headshot = absolutize(page_url, src)
            break

    # --- Stats: accept any sidearm-like table with common soccer columns
    common_cols = {"gp","g","a","pts","min","gs","sog","gw","sh","yc","rc"}
    stats: List[Dict[str, Any]] = []
    for table in soup.select("table"):
        # Get headers
        headers = [norm(th.get_text(" ")) for th in table.select("thead th")]
        if not headers:
            headers = [norm(th.get_text(" ")) for th in table.select("tr th")]
        if not headers or len(headers) < 3:
            continue
        header_keys = {h.lower().strip(".") for h in headers}
        # Heuristic: treat as stats if there's overlap with common soccer stat columns
        if len(header_keys.intersection(common_cols)) < 2 and not any(h in header_keys for h in ("season","year")):
            continue
        for tr in table.select("tbody tr"):
            tds = tr.select("td")
            if not tds: continue
            cells = [norm(td.get_text(" ")) for td in tds]
            row = dict(zip(headers[:len(cells)], cells))
            row["_season"] = row.get("Season") or row.get("Year") or None
            if any(v for v in row.values() if v and v != row["_season"]):
                stats.append(row)
        if stats:
            break  # take the first plausible stats table

    # --- Accolades: lists near “Honors/Awards/Bio”, or sentences with award-y words
    accolades: List[str] = []
    for header_text in ["Honors", "Awards", "Bio", "Personal"]:
        hdr = soup.find(re.compile("^h[2-4]$"), string=re.compile(header_text, re.I))
        if hdr:
            ul = hdr.find_next("ul")
            if ul:
                for li in ul.select("li"):
                    t = norm(li.get_text(" "))
                    if t: accolades.append(t)
    if not accolades:
        for p in soup.select("p"):
            t = norm(p.get_text(" "))
            low = t.lower()
            if any(k in low for k in ["all-", "honor", "award", "miac", "player of the", "team of the week"]):
                accolades.append(t)

    return {
        "url": page_url,
        "provider": "sidearm",
        "name": name_final,
        "height_cm": height_cm,
        "position": position,
        "class_year": class_year,
        "hometown": hometown,
        "headshot_url": headshot,
        "stats_rows": stats,
        "accolades": accolades,
    }

# ---------------- Orchestrator ----------------

async def find_and_scrape(name: str, school: str, sport_path: str = SPORT_PATH) -> Dict[str, Any]:
    school_key = norm_key(school)
    domain = SCHOOL_TO_ATHLETICS.get(school_key)
    if not domain:
        base = slugify(re.sub(r"\b(college|university)\b", "", school_key)).replace("-", "")
        domain = f"athletics.{base}.edu"

    async with httpx.AsyncClient(timeout=TIMEOUT, headers=DEFAULT_HEADERS, http2=True) as client:
        url = await sidearm_find_profile(client, domain, name)
        if not url:
            url = await search_profile_by_web(name, domain)
        if not url:
            return {"found": False, "reason": "profile_not_found", "school_domain": domain, "input": {"name": name, "school": school}}

        r = await fetch(client, url)
        html_text = r.text
        provider = guess_provider_from_html(html_text) or "unknown"

        if provider == "sidearm":
            data = parse_sidearm_profile(html_text, str(r.url), name)
            
            # Try to get stats from team stats page if no stats found
            if not data.get("stats_rows"):
                print(f"DEBUG: No stats found in profile, trying team stats page...")
                team_stats = await fetch_player_stats_from_team_page(client, domain, name, sport_path)
                if team_stats:
                    print(f"DEBUG: Found {len(team_stats)} stats rows from team page")
                    data["stats_rows"] = team_stats
                    data["stats_source"] = "team_stats_page"
                else:
                    print(f"DEBUG: No stats found in team stats page either")
        else:
            soup = BeautifulSoup(html_text, "lxml")
            inferred_name = best_match(
                name,
                [
                    norm((soup.select_one("h1") or soup.title).get_text(" ")) if (soup.select_one("h1") or soup.title) else None,
                    (soup.select_one('meta[property="og:title"]') or {}).get("content", None),
                ],
            )
            data = {"url": str(r.url), "provider": provider, "name": inferred_name}

        data.update({
            "found": True,
            "input": {"name": name, "school": school, "sport_path": sport_path},
            "school_domain": domain,
        })
        return data

# ---------------- CLI ----------------

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python scrape_player.py 'Player Name' 'School Name'")
        raise SystemExit(1)
    name = sys.argv[1]
    school = sys.argv[2]
    result = asyncio.run(find_and_scrape(name, school))
    
    # Format output in the desired table format
    if result.get("found"):
        print(f"Name: {result.get('name', 'N/A')}")
        print(f"Height: {result.get('height_cm', 'N/A')} cm")
        print(f"Position: {result.get('position', 'N/A')}")
        print(f"Class Year: {result.get('class_year', 'N/A')}")
        print(f"Hometown: {result.get('hometown', 'N/A')}")
        print()
        print("Stats")
        print()
        
        stats_rows = result.get("stats_rows", [])
        if stats_rows:
            print("Career Statistics")
            print("Scoring Statistics")
            print("Scoring Statistics")
            
            # Print header row
            print("Season\tGP\tGS\tG\tA\tPTS\tSH\tSH%\tSOG\tSOG%\tGW\tPK-ATT\tMIN")
            
            # Deduplicate and organize by season
            season_data = {}
            for row in stats_rows:
                season = row.get("_season", "Unknown")
                gp = int(row.get("gp", "0"))
                gs = int(row.get("gs", "0"))
                g = int(row.get("g", "0"))
                a = int(row.get("a", "0"))
                pts = int(row.get("pts", "0"))
                sh = int(row.get("sh", "0"))
                sog = int(row.get("sog", "0"))
                gw = int(row.get("gw", "0"))
                min_played = int(row.get("min", "0"))
                
                # Keep the row with higher GP (overall stats vs conference stats)
                if season not in season_data or gp > season_data[season]["gp"]:
                    season_data[season] = {
                        "gp": gp, "gs": gs, "g": g, "a": a, "pts": pts,
                        "sh": sh, "sog": sog, "gw": gw, "min": min_played,
                        "sh_pct": row.get("sh%", "0.000"),
                        "sog_pct": row.get("sog%", "0.000"),
                        "pk_att": row.get("pg-pa", "0-0")
                    }
            
            # Calculate career totals
            total_gp = sum(data["gp"] for data in season_data.values())
            total_gs = sum(data["gs"] for data in season_data.values())
            total_g = sum(data["g"] for data in season_data.values())
            total_a = sum(data["a"] for data in season_data.values())
            total_pts = sum(data["pts"] for data in season_data.values())
            total_sh = sum(data["sh"] for data in season_data.values())
            total_sog = sum(data["sog"] for data in season_data.values())
            total_gw = sum(data["gw"] for data in season_data.values())
            total_min = sum(data["min"] for data in season_data.values())
            
            # Calculate percentages
            total_sh_pct = f"{total_g/total_sh:.3f}" if total_sh > 0 else "0.000"
            total_sog_pct = f"{total_sog/total_sh:.3f}" if total_sh > 0 else "0.000"
            
            # Print season data in order
            for season in sorted(season_data.keys()):
                data = season_data[season]
                print(f"{season}\t{data['gp']}\t{data['gs']}\t{data['g']}\t{data['a']}\t{data['pts']}\t{data['sh']}\t{data['sh_pct']}\t{data['sog']}\t{data['sog_pct']}\t{data['gw']}\t{data['pk_att']}\t{data['min']}")
            
            # Print career totals
            print(f"Total\t{total_gp}\t{total_gs}\t{total_g}\t{total_a}\t{total_pts}\t{total_sh}\t{total_sh_pct}\t{total_sog}\t{total_sog_pct}\t{total_gw}\t0-0\t{total_min}")
        else:
            print("No statistics available.")
    else:
        print(f"Player not found: {result.get('reason', 'Unknown error')}")
        print(json.dumps(result, indent=2, ensure_ascii=False))