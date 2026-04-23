"""
TikTok Research API - Streamlit GUI

A browser-based interface for collecting data from the TikTok Research API.
Supports video search, comment collection, user data queries, playlist info,
and TikTok Shop data. Built for academic researchers.

Run with:  streamlit run tiktok_research_app.py
       or: double-click run_tiktok_app.bat

Requirements: pip install streamlit requests
"""

import csv
import io
import json
import math
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests
import streamlit as st

# Constants

PRESETS_FILE = Path(__file__).parent / "tiktok_presets.json"
ENV_FILE = Path(__file__).parent / ".env"
REGION_OPTIONS = {
    "AD": "Andorra (AD)",
    "AE": "United Arab Emirates (AE)",
    "AF": "Afghanistan (AF)",
    "AG": "Antigua and Barbuda (AG)",
    "AI": "Anguilla (AI)",
    "AL": "Albania (AL)",
    "AM": "Armenia (AM)",
    "AO": "Angola (AO)",
    "AQ": "Antarctica (AQ)",
    "AR": "Argentina (AR)",
    "AS": "American Samoa (AS)",
    "AT": "Austria (AT)",
    "AU": "Australia (AU)",
    "AW": "Aruba (AW)",
    "AX": "Åland Islands (AX)",
    "AZ": "Azerbaijan (AZ)",
    "BA": "Bosnia and Herzegovina (BA)",
    "BB": "Barbados (BB)",
    "BD": "Bangladesh (BD)",
    "BE": "Belgium (BE)",
    "BF": "Burkina Faso (BF)",
    "BG": "Bulgaria (BG)",
    "BH": "Bahrain (BH)",
    "BI": "Burundi (BI)",
    "BJ": "Benin (BJ)",
    "BL": "Saint Barthélemy (BL)",
    "BM": "Bermuda (BM)",
    "BN": "Brunei (BN)",
    "BO": "Bolivia (BO)",
    "BQ": "Caribbean Netherlands (BQ)",
    "BR": "Brazil (BR)",
    "BS": "Bahamas (BS)",
    "BT": "Bhutan (BT)",
    "BV": "Bouvet Island (BV)",
    "BW": "Botswana (BW)",
    "BY": "Belarus (BY)",
    "BZ": "Belize (BZ)",
    "CA": "Canada (CA)",
    "CD": "DR Congo (CD)",
    "CF": "Central African Republic (CF)",
    "CG": "Republic of the Congo (CG)",
    "CH": "Switzerland (CH)",
    "CI": "Côte d'Ivoire (CI)",
    "CK": "Cook Islands (CK)",
    "CL": "Chile (CL)",
    "CM": "Cameroon (CM)",
    "CN": "China (CN)",
    "CO": "Colombia (CO)",
    "CR": "Costa Rica (CR)",
    "CU": "Cuba (CU)",
    "CV": "Cape Verde (CV)",
    "CW": "Curaçao (CW)",
    "CY": "Cyprus (CY)",
    "CZ": "Czech Republic (CZ)",
    "DE": "Germany (DE)",
    "DJ": "Djibouti (DJ)",
    "DK": "Denmark (DK)",
    "DM": "Dominica (DM)",
    "DO": "Dominican Republic (DO)",
    "DZ": "Algeria (DZ)",
    "EC": "Ecuador (EC)",
    "EE": "Estonia (EE)",
    "EG": "Egypt (EG)",
    "EH": "Western Sahara (EH)",
    "ER": "Eritrea (ER)",
    "ES": "Spain (ES)",
    "ET": "Ethiopia (ET)",
    "FI": "Finland (FI)",
    "FJ": "Fiji (FJ)",
    "FK": "Falkland Islands (FK)",
    "FM": "Micronesia (FM)",
    "FO": "Faroe Islands (FO)",
    "FR": "France (FR)",
    "GA": "Gabon (GA)",
    "GB": "United Kingdom (GB)",
    "GD": "Grenada (GD)",
    "GE": "Georgia (GE)",
    "GF": "French Guiana (GF)",
    "GG": "Guernsey (GG)",
    "GH": "Ghana (GH)",
    "GI": "Gibraltar (GI)",
    "GL": "Greenland (GL)",
    "GM": "Gambia (GM)",
    "GN": "Guinea (GN)",
    "GP": "Guadeloupe (GP)",
    "GQ": "Equatorial Guinea (GQ)",
    "GR": "Greece (GR)",
    "GU": "Guam (GU)",
    "GW": "Guinea-Bissau (GW)",
    "GY": "Guyana (GY)",
    "HK": "Hong Kong (HK)",
    "HN": "Honduras (HN)",
    "HR": "Croatia (HR)",
    "HT": "Haiti (HT)",
    "HU": "Hungary (HU)",
    "ID": "Indonesia (ID)",
    "IE": "Ireland (IE)",
    "IL": "Israel (IL)",
    "IM": "Isle of Man (IM)",
    "IN": "India (IN)",
    "IO": "British Indian Ocean Territory (IO)",
    "IQ": "Iraq (IQ)",
    "IR": "Iran (IR)",
    "IS": "Iceland (IS)",
    "IT": "Italy (IT)",
    "JE": "Jersey (JE)",
    "JM": "Jamaica (JM)",
    "JO": "Jordan (JO)",
    "JP": "Japan (JP)",
    "KE": "Kenya (KE)",
    "KG": "Kyrgyzstan (KG)",
    "KH": "Cambodia (KH)",
    "KI": "Kiribati (KI)",
    "KM": "Comoros (KM)",
    "KN": "Saint Kitts and Nevis (KN)",
    "KR": "South Korea (KR)",
    "KW": "Kuwait (KW)",
    "KY": "Cayman Islands (KY)",
    "KZ": "Kazakhstan (KZ)",
    "LA": "Laos (LA)",
    "LB": "Lebanon (LB)",
    "LC": "Saint Lucia (LC)",
    "LI": "Liechtenstein (LI)",
    "LK": "Sri Lanka (LK)",
    "LR": "Liberia (LR)",
    "LS": "Lesotho (LS)",
    "LT": "Lithuania (LT)",
    "LU": "Luxembourg (LU)",
    "LV": "Latvia (LV)",
    "LY": "Libya (LY)",
    "MA": "Morocco (MA)",
    "MC": "Monaco (MC)",
    "MD": "Moldova (MD)",
    "ME": "Montenegro (ME)",
    "MF": "Saint Martin (MF)",
    "MG": "Madagascar (MG)",
    "MH": "Marshall Islands (MH)",
    "MK": "North Macedonia (MK)",
    "ML": "Mali (ML)",
    "MM": "Myanmar (MM)",
    "MN": "Mongolia (MN)",
    "MO": "Macau (MO)",
    "MP": "Northern Mariana Islands (MP)",
    "MQ": "Martinique (MQ)",
    "MR": "Mauritania (MR)",
    "MS": "Montserrat (MS)",
    "MT": "Malta (MT)",
    "MU": "Mauritius (MU)",
    "MV": "Maldives (MV)",
    "MW": "Malawi (MW)",
    "MX": "Mexico (MX)",
    "MY": "Malaysia (MY)",
    "MZ": "Mozambique (MZ)",
    "NA": "Namibia (NA)",
    "NC": "New Caledonia (NC)",
    "NE": "Niger (NE)",
    "NF": "Norfolk Island (NF)",
    "NG": "Nigeria (NG)",
    "NI": "Nicaragua (NI)",
    "NL": "Netherlands (NL)",
    "NO": "Norway (NO)",
    "NP": "Nepal (NP)",
    "NR": "Nauru (NR)",
    "NU": "Niue (NU)",
    "NZ": "New Zealand (NZ)",
    "OM": "Oman (OM)",
    "PA": "Panama (PA)",
    "PE": "Peru (PE)",
    "PF": "French Polynesia (PF)",
    "PG": "Papua New Guinea (PG)",
    "PH": "Philippines (PH)",
    "PK": "Pakistan (PK)",
    "PL": "Poland (PL)",
    "PM": "Saint Pierre and Miquelon (PM)",
    "PN": "Pitcairn Islands (PN)",
    "PR": "Puerto Rico (PR)",
    "PS": "Palestine (PS)",
    "PT": "Portugal (PT)",
    "PW": "Palau (PW)",
    "PY": "Paraguay (PY)",
    "QA": "Qatar (QA)",
    "RE": "Réunion (RE)",
    "RO": "Romania (RO)",
    "RS": "Serbia (RS)",
    "RU": "Russia (RU)",
    "RW": "Rwanda (RW)",
    "SA": "Saudi Arabia (SA)",
    "SB": "Solomon Islands (SB)",
    "SC": "Seychelles (SC)",
    "SD": "Sudan (SD)",
    "SE": "Sweden (SE)",
    "SG": "Singapore (SG)",
    "SH": "Saint Helena (SH)",
    "SI": "Slovenia (SI)",
    "SK": "Slovakia (SK)",
    "SL": "Sierra Leone (SL)",
    "SM": "San Marino (SM)",
    "SN": "Senegal (SN)",
    "SO": "Somalia (SO)",
    "SR": "Suriname (SR)",
    "SS": "South Sudan (SS)",
    "ST": "São Tomé and Príncipe (ST)",
    "SV": "El Salvador (SV)",
    "SX": "Sint Maarten (SX)",
    "SY": "Syria (SY)",
    "SZ": "Eswatini (SZ)",
    "TC": "Turks and Caicos Islands (TC)",
    "TD": "Chad (TD)",
    "TF": "French Southern Territories (TF)",
    "TG": "Togo (TG)",
    "TH": "Thailand (TH)",
    "TJ": "Tajikistan (TJ)",
    "TK": "Tokelau (TK)",
    "TL": "Timor-Leste (TL)",
    "TM": "Turkmenistan (TM)",
    "TN": "Tunisia (TN)",
    "TO": "Tonga (TO)",
    "TP": "East Timor (legacy) (TP)",
    "TR": "Turkey (TR)",
    "TT": "Trinidad and Tobago (TT)",
    "TV": "Tuvalu (TV)",
    "TW": "Taiwan (TW)",
    "TZ": "Tanzania (TZ)",
    "UA": "Ukraine (UA)",
    "UG": "Uganda (UG)",
    "UM": "US Minor Outlying Islands (UM)",
    "US": "United States (US)",
    "UY": "Uruguay (UY)",
    "UZ": "Uzbekistan (UZ)",
    "VA": "Vatican City (VA)",
    "VC": "Saint Vincent and the Grenadines (VC)",
    "VE": "Venezuela (VE)",
    "VG": "British Virgin Islands (VG)",
    "VI": "US Virgin Islands (VI)",
    "VN": "Vietnam (VN)",
    "VU": "Vanuatu (VU)",
    "WF": "Wallis and Futuna (WF)",
    "WS": "Samoa (WS)",
    "XK": "Kosovo (XK)",
    "YE": "Yemen (YE)",
    "YT": "Mayotte (YT)",
    "ZA": "South Africa (ZA)",
    "ZM": "Zambia (ZM)",
    "ZW": "Zimbabwe (ZW)",
}
VIDEO_LENGTH_OPTIONS = ["(any)", "SHORT", "MID", "LONG", "EXTRA_LONG"]

VIDEO_CSV_FIELDS = [
    "video_link", "id", "create_time", "username", "region_code",
    "video_description", "voice_to_text", "music_id",
    "like_count", "comment_count", "share_count", "view_count",
    "effect_ids", "hashtag_names", "playlist_id",
    "video_label", "is_stem_verified", "video_tag",
]

COMMENT_CSV_FIELDS = [
    "username", "video_id", "id", "video_description", "voice_to_text",
    "parent_comment_id", "text", "like_count", "create_time",
]

DAILY_REQUEST_BUDGET = 1000

# Session state initialisation

if "groups" not in st.session_state:
    st.session_state.groups = [
        {"name": "Group 1", "field": "keyword", "terms": [], "internal_logic": "OR"}
    ]
if "connectors" not in st.session_state:
    st.session_state.connectors = []
if "last_video_csv" not in st.session_state:
    st.session_state.last_video_csv = None

# Credentials (.env)

def load_env_credentials() -> tuple:
    """Load client key and secret from .env file if it exists."""
    if not ENV_FILE.exists():
        return "", ""
    try:
        env = {}
        for line in ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
        return env.get("TIKTOK_CLIENT_KEY", ""), env.get("TIKTOK_CLIENT_SECRET", "")
    except Exception:
        return "", ""

def save_env_credentials(client_key: str, client_secret: str):
    """Save credentials to .env file."""
    ENV_FILE.write_text(
        "# TikTok Research API credentials\n"
        "# This file is stored locally - do not share it or commit it to version control.\n"
        f"TIKTOK_CLIENT_KEY={client_key}\n"
        f"TIKTOK_CLIENT_SECRET={client_secret}\n"
    )

def delete_env_credentials():
    if ENV_FILE.exists():
        ENV_FILE.unlink()

# Preset helpers

def load_presets() -> dict:
    if PRESETS_FILE.exists():
        try:
            return json.loads(PRESETS_FILE.read_text())
        except Exception:
            return {}
    return {}

def save_presets(presets: dict):
    PRESETS_FILE.write_text(json.dumps(presets, indent=2))

# Auth

def fetch_access_token(client_key: str, client_secret: str) -> str:
    resp = requests.post(
        "https://open.tiktokapis.com/v2/oauth/token/",
        data={
            "client_key": client_key,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        },
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("No access_token in response")
    return f"Bearer {token}"

# Date chunking

def split_into_month_chunks(start_str: str, end_str: str):
    start = datetime.strptime(start_str, "%Y%m%d")
    end = datetime.strptime(end_str, "%Y%m%d")
    chunks, current = [], start
    while current <= end:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        chunk_end = min(next_month - timedelta(days=1), end)
        chunks.append((current.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        current = chunk_end + timedelta(days=1)
    return chunks

# Query builder

def build_query(config: dict) -> dict:
    q = {"and": [], "or": [], "not": []}

    def add(bucket, field, op, values):
        if values:
            q[bucket.lower()].append({
                "operation": op,
                "field_name": field,
                "field_values": [str(v) for v in values],
            })

    regions = config.get("region_codes", [])
    if regions:
        add("and", "region_code", "IN", regions)

    users = [u.strip() for u in config.get("usernames", []) if u.strip()]
    if users:
        add("and", "username", "IN", users)

    vlen = config.get("video_length", "(any)")
    if vlen and vlen != "(any)":
        add("and", "video_length", "EQ", [vlen])

    for field, op, val_key in [
        ("view_count",    "GTE", "min_view_count"),
        ("view_count",    "LTE", "max_view_count"),
        ("comment_count", "GTE", "min_comment_count"),
        ("comment_count", "LTE", "max_comment_count"),
    ]:
        val = config.get(val_key)
        if val is not None:
            add("and", field, op, [val])

    groups = config.get("groups", [])
    connectors = config.get("connectors", [])

    for i, group in enumerate(groups):
        terms = [t.strip() for t in group.get("terms", []) if t.strip()]
        if not terms:
            continue
        field_name = "keyword" if group.get("field") == "keyword" else "hashtag_name"
        bucket = "AND" if i == 0 else connectors[i - 1] if i - 1 < len(connectors) else "AND"
        internal = group.get("internal_logic", "OR")
        if internal == "OR":
            add(bucket, field_name, "IN", terms)
        else:
            for term in terms:
                add(bucket, field_name, "IN", [term])

    # Remove empty buckets - TikTok API returns 500 if any bucket is present but empty
    return {k: v for k, v in q.items() if v}

def query_summary(config: dict) -> str:
    groups = config.get("groups", [])
    connectors = config.get("connectors", [])
    parts = []

    for i, group in enumerate(groups):
        terms = [t.strip() for t in group.get("terms", []) if t.strip()]
        if not terms:
            continue
        prefix = "#" if group.get("field") == "hashtag" else ""
        joined = f" {group.get('internal_logic', 'OR')} ".join(f"{prefix}{t}" for t in terms)
        parts.append((f"({joined})", group.get("name", f"Group {i+1}")))

    if not parts:
        return "(no conditions set)"

    lines = []
    for i, (expr, label) in enumerate(parts):
        if i == 0:
            lines.append(f"  {expr}  ← {label}")
        else:
            connector = connectors[i - 1] if i - 1 < len(connectors) else "AND"
            lines.append(connector)
            lines.append(f"  {expr}  ← {label}")

    extras = []
    if config.get("region_codes"):
        extras.append(f"region IN ({', '.join(config['region_codes'])})")
    users = [u.strip() for u in config.get("usernames", []) if u.strip()]
    if users:
        extras.append(f"username IN ({', '.join(users)})")
    vlen = config.get("video_length", "(any)")
    if vlen and vlen != "(any)":
        extras.append(f"length = {vlen}")
    if extras:
        lines.append("\nAND (static filters)")
        for e in extras:
            lines.append(f"  {e}")

    return "\n".join(lines)

# Video search

def video_query_api(query, start_date, end_date, cursor, search_id, authorization):
    url = (
        "https://open.tiktokapis.com/v2/research/video/query/"
        "?fields=id,create_time,username,region_code,video_description,"
        "voice_to_text,music_id,like_count,comment_count,share_count,"
        "view_count,effect_ids,video_label,hashtag_names,playlist_id,"
        "is_stem_verified,video_tag"
    )
    payload = {
        "query": query,
        "start_date": start_date,
        "end_date": end_date,
        "max_count": 100,
    }
    if cursor is not None:
        payload["cursor"] = cursor
    if search_id is not None:
        payload["search_id"] = search_id

    resp = requests.post(
        url,
        headers={"Authorization": authorization, "Content-Type": "application/json"},
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()

def append_videos_to_csv(data: dict, file_path: str) -> int:
    videos = data.get("data", {}).get("videos", [])
    if not videos:
        return 0
    file_exists = os.path.exists(file_path)
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=VIDEO_CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for v in videos:
            writer.writerow({
                "video_link": f"https://www.tiktok.com/@{v.get('username')}/video/{v.get('id')}",
                **{k: v.get(k) for k in VIDEO_CSV_FIELDS if k != "video_link"},
            })
    return len(videos)

USERNAME_BATCH_SIZE = 10  # TikTok API reliably handles up to ~10 usernames per query

def collect_username_batch(username_batch: list, config: dict, date_chunks: list,
                            max_iter: int, authorization: str, output_path: str,
                            log, failed_accounts: list) -> int:
    """
    Attempt to collect videos for a batch of usernames across all date chunks.
    If the batch fails consistently, split it in half and retry each half (binary search).
    Single-user batches that still fail are recorded in failed_accounts.
    Returns total videos saved.
    """
    total_saved = 0
    batch_config = {**config, "usernames": username_batch}
    query = build_query(batch_config)
    batch_failed = False

    for chunk_start, chunk_end in date_chunks:
        cursor, search_id = None, None
        iteration, has_more = 1, True
        consecutive_errors = 0
        chunk_failed = False

        while has_more and iteration <= max_iter:
            try:
                data = video_query_api(query, chunk_start, chunk_end, cursor, search_id, authorization)
                consecutive_errors = 0
                meta = data.get("data", {})
                has_more = meta.get("has_more", False)
                cursor = meta.get("cursor")
                search_id = meta.get("search_id")
                saved = append_videos_to_csv(data, output_path)
                total_saved += saved
                log(f"    ✅ Page {iteration} ({saved} videos, total: {total_saved})")
                iteration += 1
                time.sleep(2)

            except Exception as e:
                consecutive_errors += 1
                wait = 5 * (2 ** consecutive_errors)
                log(f"    ⚠️ Error (attempt {consecutive_errors}/3): {e}, retrying in {wait}s")
                if consecutive_errors >= 3:
                    chunk_failed = True
                    break
                time.sleep(wait)
                cursor, search_id = None, None

        if chunk_failed:
            batch_failed = True
            break

    if batch_failed:
        if len(username_batch) == 1:
            # Can't split further - this account is the problem
            failed_accounts.append(username_batch[0])
            log(f"    ❌ @{username_batch[0]} failed consistently, skipping")
        else:
            # Binary search fallback: split the batch and retry each half
            mid = len(username_batch) // 2
            left, right = username_batch[:mid], username_batch[mid:]
            log(f"    ↩️ Batch failed: splitting into {left} and {right} to isolate the problem account")
            total_saved += collect_username_batch(
                left, config, date_chunks, max_iter, authorization, output_path, log, failed_accounts
            )
            total_saved += collect_username_batch(
                right, config, date_chunks, max_iter, authorization, output_path, log, failed_accounts
            )

    return total_saved

def run_video_collection(config: dict, authorization: str, output_path: str, log):
    date_chunks = split_into_month_chunks(config["start_date"], config["end_date"])
    max_iter = config.get("max_iterations", 100)
    total_saved = 0
    failed_accounts = []

    usernames = [u.strip() for u in config.get("usernames", []) if u.strip()]
    has_terms = any(
        t.strip()
        for g in config.get("groups", [])
        for t in g.get("terms", [])
    )

    if usernames:
        username_batches = [
            usernames[i:i + USERNAME_BATCH_SIZE]
            for i in range(0, len(usernames), USERNAME_BATCH_SIZE)
        ]
        log(f"👤 {len(usernames)} usernames across {len(username_batches)} batch(es) (binary search fallback enabled)")
    else:
        username_batches = [[]]

    for batch_idx, username_batch in enumerate(username_batches):
        if username_batch:
            log(f"\n👥 Batch {batch_idx + 1}/{len(username_batches)}: {', '.join(f'@{u}' for u in username_batch)}")
        for chunk_start, chunk_end in date_chunks:
            log(f"  📅 {chunk_start} → {chunk_end}")

        total_saved += collect_username_batch(
            username_batch, config, date_chunks, max_iter,
            authorization, output_path, log, failed_accounts
        )

    log(f"\nDone! {total_saved} videos saved to {output_path}")
    if failed_accounts:
        log(f"⚠️ {len(failed_accounts)} account(s) failed consistently and were skipped:")
        for u in failed_accounts:
            log(f"   • @{u}")
    st.session_state.last_video_csv = output_path
    return total_saved

# Comments

def sample_videos(videos: list, method: str, n: int) -> list:
    """Return a sample of videos according to the chosen method."""
    if n >= len(videos):
        return videos
    if method == "Top N by view count":
        return sorted(videos, key=lambda v: int(v.get("view_count") or 0), reverse=True)[:n]
    elif method == "Top N by comment count":
        return sorted(videos, key=lambda v: int(v.get("comment_count") or 0), reverse=True)[:n]
    else:  # Random
        return random.sample(videos, n)

def fetch_comments_for_video(video_data: dict, output_path: str, authorization: str,
                              max_iterations: int, log_queue: list):
    video_id = video_data["id"]
    username = video_data.get("username", "")

    url = (
        "https://open.tiktokapis.com/v2/research/video/comment/list/"
        "?fields=id,like_count,create_time,text,video_id,parent_comment_id"
    )
    headers = {"Authorization": authorization, "Content-Type": "application/json"}

    cursor = None
    has_more = True
    iteration = 1
    all_comments = []
    backoff = 2

    while has_more and iteration <= max_iterations:
        payload = {"video_id": int(video_id), "max_count": 100}
        if cursor:
            payload["cursor"] = cursor

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=15)

            if resp.status_code == 200:
                data = resp.json()
                comments = data.get("data", {}).get("comments", [])
                all_comments.extend(comments)
                has_more = data.get("data", {}).get("has_more", False)
                cursor = data.get("data", {}).get("cursor")
                iteration += 1
                if not has_more:
                    break
                time.sleep(random.uniform(0.3, 0.6))

            elif resp.status_code == 429:
                log_queue.append(f"  ⚠️ Rate limit hit for video {video_id}, backing off {backoff}s")
                time.sleep(backoff)
                backoff *= 2

            else:
                log_queue.append(f"  ⚠️ HTTP {resp.status_code} for video {video_id}: {resp.text[:100]}")
                break

        except requests.RequestException as e:
            log_queue.append(f"  ⚠️ Network error for video {video_id}: {e}")
            time.sleep(backoff)
            backoff *= 2

    if all_comments:
        file_exists = os.path.exists(output_path)
        with open(output_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COMMENT_CSV_FIELDS)
            if not file_exists:
                writer.writeheader()
            for c in all_comments:
                writer.writerow({
                    "username": username,
                    "video_id": video_id,
                    "id": c.get("id"),
                    "video_description": video_data.get("video_description", ""),
                    "voice_to_text": video_data.get("voice_to_text", ""),
                    "parent_comment_id": c.get("parent_comment_id"),
                    "text": c.get("text"),
                    "like_count": c.get("like_count"),
                    "create_time": c.get("create_time"),
                })
        log_queue.append(f"  ✅ {username} ({video_id}) - {len(all_comments)} comments saved")
    else:
        log_queue.append(f"  ⚠️ No comments found for video {video_id}")

    return len(all_comments)

def read_video_csv(file_path: str) -> list:
    videos = []
    with open(file_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid_id = str(row.get("id", "")).strip()
            if vid_id.isdigit():
                videos.append({
                    "id": int(vid_id),
                    "username": row.get("username", ""),
                    "video_description": row.get("video_description", ""),
                    "voice_to_text": row.get("voice_to_text", ""),
                    "view_count": row.get("view_count", 0),
                    "comment_count": row.get("comment_count", 0),
                })
    return videos

def read_video_csv_from_upload(uploaded_file) -> list:
    content = uploaded_file.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    videos = []
    for row in reader:
        vid_id = str(row.get("id", "")).strip()
        if vid_id.isdigit():
            videos.append({
                "id": int(vid_id),
                "username": row.get("username", ""),
                "video_description": row.get("video_description", ""),
                "voice_to_text": row.get("voice_to_text", ""),
                "view_count": row.get("view_count", 0),
                "comment_count": row.get("comment_count", 0),
            })
    return videos

def run_comment_collection(videos: list, authorization: str, output_path: str,
                            max_iterations: int, max_workers: int, log):
    log(f"Collecting comments for {len(videos)} videos using {max_workers} parallel threads...\n")
    log_queue = []
    total = 0
    start = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                fetch_comments_for_video, v, output_path, authorization, max_iterations, log_queue
            ): v for v in videos
        }
        for future in as_completed(futures):
            try:
                total += future.result()
            except Exception as e:
                log_queue.append(f"  ❌ Unexpected error: {e}")
            for msg in log_queue:
                log(msg)
            log_queue.clear()

    log(f"\nDone! {total} comments collected in {time.time() - start:.1f}s, saved to {output_path}")
    return total

# User data endpoints

VIDEO_FIELDS = (
    "id,create_time,username,region_code,video_description,music_id,"
    "like_count,comment_count,share_count,view_count,hashtag_names,"
    "is_stem_verified,favorites_count,video_duration,video_label,video_tag"
)

USER_VIDEO_CSV_FIELDS = [
    "queried_username", "id", "create_time", "username", "region_code",
    "video_description", "music_id", "like_count", "comment_count",
    "share_count", "view_count", "hashtag_names", "is_stem_verified",
    "favorites_count", "video_duration", "video_label", "video_tag",
]

USER_RELATION_CSV_FIELDS = ["queried_username", "username", "display_name"]

def fetch_pinned_videos(username: str, authorization: str) -> list:
    url = f"https://open.tiktokapis.com/v2/research/user/pinned_videos/?fields={VIDEO_FIELDS}"
    resp = requests.post(
        url,
        headers={"Authorization": authorization, "Content-Type": "application/json"},
        json={"username": username},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("data", {}).get("pinned_videos_list", [])

def fetch_paginated_user_videos(username: str, authorization: str,
                                 endpoint: str, data_key: str,
                                 max_iterations: int, log) -> list:
    url = f"https://open.tiktokapis.com/v2/research/user/{endpoint}/?fields={VIDEO_FIELDS}"
    headers = {"Authorization": authorization, "Content-Type": "application/json"}
    cursor, has_more, iteration, all_videos = None, True, 1, []
    backoff = 2

    while has_more and iteration <= max_iterations:
        payload = {"username": username, "max_count": 100}
        if cursor:
            payload["cursor"] = cursor
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                videos = data.get(data_key, [])
                all_videos.extend(videos)
                has_more = data.get("has_more", False)
                cursor = data.get("cursor")
                log(f"  ✅ {username} - page {iteration} ({len(videos)} videos)")
                iteration += 1
                time.sleep(1)
            elif resp.status_code == 429:
                log(f"  ⚠️ Rate limit for {username}, waiting {backoff}s")
                time.sleep(backoff); backoff *= 2
            else:
                log(f"  ⚠️ HTTP {resp.status_code} for {username}: {resp.text[:100]}")
                break
        except requests.RequestException as e:
            log(f"  ⚠️ Network error for {username}: {e}")
            time.sleep(backoff); backoff *= 2

    return all_videos

def fetch_paginated_user_relations(username: str, authorization: str,
                                    endpoint: str, data_key: str,
                                    max_iterations: int, log) -> list:
    url = f"https://open.tiktokapis.com/v2/research/user/{endpoint}/"
    headers = {"Authorization": authorization, "Content-Type": "application/json"}
    cursor, has_more, iteration, all_users = None, True, 1, []
    backoff = 2

    while has_more and iteration <= max_iterations:
        payload = {"username": username, "max_count": 100}
        if cursor:
            payload["cursor"] = cursor
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                users = data.get(data_key, [])
                all_users.extend(users)
                has_more = data.get("has_more", False)
                cursor = data.get("cursor")
                log(f"  ✅ {username} - page {iteration} ({len(users)} users)")
                iteration += 1
                time.sleep(1)
            elif resp.status_code == 429:
                log(f"  ⚠️ Rate limit for {username}, waiting {backoff}s")
                time.sleep(backoff); backoff *= 2
            else:
                log(f"  ⚠️ HTTP {resp.status_code} for {username}: {resp.text[:100]}")
                break
        except requests.RequestException as e:
            log(f"  ⚠️ Network error for {username}: {e}")
            time.sleep(backoff); backoff *= 2

    return all_users

def save_user_videos_csv(videos: list, queried_username: str, output_path: str):
    file_exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=USER_VIDEO_CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for v in videos:
            writer.writerow({
                "queried_username": queried_username,
                **{k: v.get(k) for k in USER_VIDEO_CSV_FIELDS if k != "queried_username"},
            })

def save_user_relations_csv(users: list, queried_username: str, output_path: str):
    file_exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=USER_RELATION_CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for u in users:
            writer.writerow({
                "queried_username": queried_username,
                "username": u.get("username"),
                "display_name": u.get("display_name"),
            })

def run_user_data_collection(usernames: list, endpoint_key: str, authorization: str,
                              output_path: str, max_iterations: int, log) -> int:
    total = 0

    for username in usernames:
        username = username.strip()
        if not username:
            continue
        log(f"\n👤 Processing: {username}")

        try:
            if endpoint_key == "pinned_videos":
                videos = fetch_pinned_videos(username, authorization)
                save_user_videos_csv(videos, username, output_path)
                log(f"  ✅ {username} - {len(videos)} pinned videos saved")
                total += len(videos)

            elif endpoint_key == "liked_videos":
                videos = fetch_paginated_user_videos(
                    username, authorization, "liked_videos", "user_liked_videos", max_iterations, log
                )
                save_user_videos_csv(videos, username, output_path)
                total += len(videos)

            elif endpoint_key == "followers":
                users = fetch_paginated_user_relations(
                    username, authorization, "followers", "user_followers", max_iterations, log
                )
                save_user_relations_csv(users, username, output_path)
                total += len(users)

            elif endpoint_key == "following":
                users = fetch_paginated_user_relations(
                    username, authorization, "following", "user_following", max_iterations, log
                )
                save_user_relations_csv(users, username, output_path)
                total += len(users)

        except Exception as e:
            log(f"  ❌ Failed for {username}: {e}")

    log(f"\nDone! {total} records saved to {output_path}")
    return total

# Playlist API

PLAYLIST_CSV_FIELDS = [
    "playlist_id", "playlist_name", "playlist_item_total",
    "playlist_last_updated", "video_id",
]

def fetch_playlist(playlist_id: int, authorization: str, max_iterations: int, log) -> dict:
    """Returns playlist metadata + all video IDs across pages."""
    url = "https://open.tiktokapis.com/v2/research/playlist/info/?fields=playlist_id"
    headers = {"Authorization": authorization, "Content-Type": "application/json"}
    cursor, has_more, iteration = 0, True, 1
    meta, all_video_ids = {}, []

    while has_more and iteration <= max_iterations:
        payload = {"playlist_id": int(playlist_id), "max_count": 100, "cursor": cursor}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json().get("data", {})
                if not meta:
                    meta = {
                        "playlist_id": data.get("playlist_id"),
                        "playlist_name": data.get("playlist_name"),
                        "playlist_item_total": data.get("playlist_item_total"),
                        "playlist_last_updated": data.get("playlist_last_updated"),
                    }
                video_ids = data.get("playlist_video_ids", [])
                all_video_ids.extend(video_ids)
                has_more = data.get("has_more", False)
                cursor = data.get("cursor", 0)
                log(f"  ✅ Playlist {playlist_id} - page {iteration} ({len(video_ids)} video IDs)")
                iteration += 1
                time.sleep(1)
            elif resp.status_code == 429:
                log(f"  ⚠️ Rate limit, backing off")
                time.sleep(5)
            else:
                log(f"  ⚠️ HTTP {resp.status_code}: {resp.text[:100]}")
                break
        except requests.RequestException as e:
            log(f"  ⚠️ Error: {e}")
            break

    return {**meta, "video_ids": all_video_ids}

def save_playlist_csv(result: dict, output_path: str):
    file_exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PLAYLIST_CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        for vid_id in result.get("video_ids", []):
            writer.writerow({
                "playlist_id": result.get("playlist_id"),
                "playlist_name": result.get("playlist_name"),
                "playlist_item_total": result.get("playlist_item_total"),
                "playlist_last_updated": result.get("playlist_last_updated"),
                "video_id": vid_id,
            })

# TikTok Shop API

SHOP_INFO_FIELDS = "shop_name,shop_rating,shop_review_count,item_sold_count,shop_id"
SHOP_INFO_CSV = ["shop_id", "shop_name", "shop_rating", "shop_review_count", "item_sold_count"]

SHOP_PRODUCT_FIELDS = (
    "product_id,product_name,product_description,product_price,"
    "product_sold_count,product_review_count,"
    "product_rating_1_count,product_rating_2_count,product_rating_3_count,"
    "product_rating_4_count,product_rating_5_count"
)
SHOP_PRODUCT_CSV = [
    "shop_id", "product_id", "product_name", "product_description", "product_price",
    "product_sold_count", "product_review_count",
    "product_rating_1_count", "product_rating_2_count", "product_rating_3_count",
    "product_rating_4_count", "product_rating_5_count",
]

SHOP_REVIEW_FIELDS = "product_name,review_text,review_like_count,create_time,review_rating"
SHOP_REVIEW_CSV = [
    "product_id", "product_name", "review_text", "review_like_count",
    "create_time", "review_rating",
]

def fetch_shop_info(shop_name: str, authorization: str) -> list:
    url = f"https://open.tiktokapis.com/v2/research/tts/shop/?fields={SHOP_INFO_FIELDS}"
    resp = requests.post(
        url,
        headers={"Authorization": authorization, "Content-Type": "application/json"},
        json={"shop_name": shop_name, "limit": 10},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json().get("data", {}).get("shop_data", [])

def fetch_shop_products(shop_id: int, authorization: str, max_pages: int, log) -> list:
    url = f"https://open.tiktokapis.com/v2/research/tts/product/?fields={SHOP_PRODUCT_FIELDS}"
    headers = {"Authorization": authorization, "Content-Type": "application/json"}
    all_products, page = [], 1

    while page <= max_pages:
        payload = {"shop_id": int(shop_id), "page_start": page, "page_size": 10}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                products = resp.json().get("data", {}).get("product_data", [])
                if not products:
                    break
                all_products.extend(products)
                log(f"  ✅ Shop {shop_id} products - page {page} ({len(products)} products)")
                page += 1
                time.sleep(1)
            elif resp.status_code == 429:
                log(f"  ⚠️ Rate limit, backing off")
                time.sleep(5)
            else:
                log(f"  ⚠️ HTTP {resp.status_code}: {resp.text[:100]}")
                break
        except requests.RequestException as e:
            log(f"  ⚠️ Error: {e}")
            break

    return all_products

def fetch_product_reviews(product_id: int, authorization: str, max_pages: int, log) -> list:
    url = f"https://open.tiktokapis.com/v2/research/tts/review/?fields={SHOP_REVIEW_FIELDS}"
    headers = {"Authorization": authorization, "Content-Type": "application/json"}
    all_reviews, page = [], 1

    while page <= max_pages:
        payload = {"product_id": int(product_id), "page_start": page, "page_size": 10}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code == 200:
                reviews = resp.json().get("data", {}).get("review_data", [])
                if not reviews:
                    break
                all_reviews.extend(reviews)
                log(f"  ✅ Product {product_id} reviews - page {page} ({len(reviews)} reviews)")
                page += 1
                time.sleep(0.5)
            elif resp.status_code == 429:
                log(f"  ⚠️ Rate limit, backing off")
                time.sleep(5)
            else:
                log(f"  ⚠️ HTTP {resp.status_code}: {resp.text[:100]}")
                break
        except requests.RequestException as e:
            log(f"  ⚠️ Error: {e}")
            break

    return all_reviews

def save_shop_info_csv(shops: list, output_path: str):
    file_exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SHOP_INFO_CSV)
        if not file_exists:
            writer.writeheader()
        for s in shops:
            writer.writerow({k: s.get(k) for k in SHOP_INFO_CSV})

def save_shop_products_csv(products: list, shop_id: int, output_path: str):
    file_exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SHOP_PRODUCT_CSV)
        if not file_exists:
            writer.writeheader()
        for p in products:
            writer.writerow({"shop_id": shop_id, **{k: p.get(k) for k in SHOP_PRODUCT_CSV if k != "shop_id"}})

def save_shop_reviews_csv(reviews: list, product_id: int, output_path: str):
    file_exists = os.path.exists(output_path)
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SHOP_REVIEW_CSV)
        if not file_exists:
            writer.writeheader()
        for r in reviews:
            writer.writerow({"product_id": product_id, **{k: r.get(k) for k in SHOP_REVIEW_CSV if k != "product_id"}})

# Streamlit UI

st.set_page_config(page_title="TikTok Research API", page_icon="🎵", layout="wide")
st.title("TikTok Research API")

mode = st.radio(
    "Mode", ["🎬 Collect Video Data", "💬 Collect Comment Data", "👤 Collect User Data", "🎞️ Collect Playlist Data", "🛒 Collect TikTok Shop Data"],
    horizontal=True, label_visibility="collapsed",
)
st.divider()

presets = load_presets()

# Sidebar

with st.sidebar:
    st.header("🔑 API Credentials")

    saved_key, saved_secret = load_env_credentials()
    creds_saved = bool(saved_key and saved_secret)

    if creds_saved:
        st.caption("✅ Credentials loaded from saved `.env` file")

    client_key = st.text_input(
        "Client Key",
        value=saved_key,
        type="password",
        key="cred_key",
    )
    client_secret = st.text_input(
        "Client Secret",
        value=saved_secret,
        type="password",
        key="cred_secret",
    )

    col_save, col_clear = st.columns(2)
    if col_save.button("💾 Save", disabled=not (client_key and client_secret)):
        save_env_credentials(client_key, client_secret)
        st.success("Saved to `.env`")
        st.rerun()
    if col_clear.button("🗑️ Clear", disabled=not creds_saved):
        delete_env_credentials()
        st.info("Credentials cleared")
        st.rerun()

    st.caption("Credentials are saved locally in a `.env` file next to the script. Never share this file.")

    st.divider()
    st.header("💾 Presets")
    preset_names = list(presets.keys())
    selected_preset = st.selectbox("Load a preset", ["(none)"] + preset_names)

    if selected_preset != "(none)" and st.button("⬆️ Load preset"):
        p = presets[selected_preset]
        # Store the full preset so pget() can seed all widget default values
        st.session_state["loaded_preset"] = p
        # Push groups/connectors into session state so the query builder
        # re-renders with the correct number of rows and correct content
        if "groups" in p:
            st.session_state.groups = [dict(g) for g in p["groups"]]
        if "connectors" in p:
            st.session_state.connectors = list(p["connectors"])
        # Deleting widget keys doesn't work - Streamlit restores them from
        # the frontend value. Instead, directly overwrite each widget key
        # with the loaded data so the correct values are displayed.
        for i, g in enumerate(st.session_state.groups):
            fallback = chr(10).join(g.get("terms", []))
            st.session_state["gterms_" + str(i)] = fallback
            st.session_state["gname_" + str(i)] = g.get("name", "Group " + str(i + 1))
            st.session_state["gfield_" + str(i)] = g.get("field", "keyword")
            st.session_state["gint_" + str(i)] = g.get("internal_logic", "OR")
        for i, c in enumerate(st.session_state.connectors):
            st.session_state["conn_" + str(i + 1)] = c
        st.success(f"Loaded: {selected_preset}")
        st.rerun()

    new_preset_name = st.text_input("Save current config as", placeholder="My preset name")
    if st.button("💾 Save preset") and new_preset_name:
        cc = st.session_state.get("current_config", {})
        preset_to_save = {k: v for k, v in cc.items()
                         if k not in ("client_key", "client_secret")}
        # The sidebar renders before the video UI loop, so st.session_state.groups
        # may not yet have the current widget values written back into it.
        # Read terms directly from the gterms_* widget keys in session state,
        # which Streamlit updates immediately when the user edits a text area.
        groups_snapshot = []
        for i, g in enumerate(st.session_state.groups):
            g_copy = dict(g)
            fallback = chr(10).join(g.get("terms", []))
            raw = st.session_state.get("gterms_" + str(i), fallback)
            g_copy["terms"] = [t.strip() for t in raw.splitlines() if t.strip()]
            g_copy["name"] = st.session_state.get("gname_" + str(i), g.get("name", "Group " + str(i+1)))
            g_copy["field"] = st.session_state.get("gfield_" + str(i), g.get("field", "keyword"))
            g_copy["internal_logic"] = st.session_state.get("gint_" + str(i), g.get("internal_logic", "OR"))
            groups_snapshot.append(g_copy)
        preset_to_save["groups"] = groups_snapshot
        preset_to_save["connectors"] = [
            st.session_state.get(f"conn_{i+1}", st.session_state.connectors[i])
            for i in range(len(st.session_state.connectors))
        ]
        if not preset_to_save.get("start_date"):
            st.warning("Switch to **Collect Video Data** mode and configure your query before saving a preset.")
        else:
            presets[new_preset_name] = preset_to_save
            save_presets(presets)
            st.success(f"Saved: {new_preset_name}")

    if selected_preset != "(none)" and st.button("🗑️ Delete preset"):
        del presets[selected_preset]
        save_presets(presets)
        st.success(f"Deleted: {selected_preset}")
        st.rerun()

    st.divider()
    st.caption(f"Presets saved to:\n`{PRESETS_FILE}`")

def pget(key, default):
    """Pull a value from the currently loaded preset, falling back to default if not set."""
    return st.session_state.get("loaded_preset", {}).get(key, default)

# VIDEO SEARCH

if mode == "🎬 Collect Video Data":

    st.subheader("📅 Date Range")
    col1, col2 = st.columns(2)
    start_date = col1.date_input(
        "Start date",
        value=datetime.strptime(pget("start_date", (datetime.today() - timedelta(days=30)).strftime("%Y%m%d")), "%Y%m%d").date(),
    )
    end_date = col2.date_input(
        "End date",
        value=datetime.strptime(pget("end_date", datetime.today().strftime("%Y%m%d")), "%Y%m%d").date(),
    )
    if start_date > end_date:
        st.error("Start date must be before end date.")

    st.divider()

    with st.expander("🌍 Regions, Usernames & Video Length", expanded=True):
        col1, col2, col3 = st.columns(3)

        # Build default display labels from saved codes
        saved_codes = pget("region_codes", ["US"])
        saved_labels = [REGION_OPTIONS[c] for c in saved_codes if c in REGION_OPTIONS]

        selected_labels = col1.multiselect(
            "Region codes",
            options=sorted(REGION_OPTIONS.values()),
            default=saved_labels,
        )
        # Extract just the 2-letter codes to pass to the API
        region_codes = [label.split("(")[-1].rstrip(")") for label in selected_labels]
        usernames_raw = col2.text_area(
            "Usernames (one per line)", value="\n".join(pget("usernames", [])),
            height=120, placeholder="username1\nusername2",
        )
        usernames = [u.strip() for u in usernames_raw.splitlines() if u.strip()]
        vlen_default = pget("video_length", "(any)")
        video_length = col3.selectbox(
            "Video length", VIDEO_LENGTH_OPTIONS,
            index=VIDEO_LENGTH_OPTIONS.index(vlen_default) if vlen_default in VIDEO_LENGTH_OPTIONS else 0,
        )

    with st.expander("📊 Engagement filters (optional)"):
        col1, col2 = st.columns(2)
        use_view = col1.checkbox("Filter by view count", value=pget("use_view", False))
        min_view = max_view = None
        if use_view:
            min_view = col1.number_input("Min views", min_value=0, value=pget("min_view_count", 0), step=1000)
            max_view = col1.number_input("Max views", min_value=0, value=pget("max_view_count", 1_000_000), step=1000)
        use_comment = col2.checkbox("Filter by comment count", value=pget("use_comment", False))
        min_comment = max_comment = None
        if use_comment:
            min_comment = col2.number_input("Min comments", min_value=0, value=pget("min_comment_count", 0), step=100)
            max_comment = col2.number_input("Max comments", min_value=0, value=pget("max_comment_count", 10_000), step=100)

    st.divider()
    st.subheader("🔍 Keyword / Hashtag Query Builder")
    st.caption(
        "Each group is a set of terms joined internally by **AND** or **OR**. "
        "Between groups, choose how they connect: **AND**, **OR**, or **NOT**.  \n"
        "**Example:** `(climate OR environment)` **AND** `(policy)` **NOT** `(satire)`"
    )

    groups = st.session_state.groups
    connectors = st.session_state.connectors

    while len(connectors) < len(groups) - 1:
        connectors.append("AND")
    while len(connectors) > len(groups) - 1:
        connectors.pop()

    for i, group in enumerate(groups):
        if i > 0:
            st.markdown("")
            col_conn, _ = st.columns([1, 5])
            connectors[i - 1] = col_conn.selectbox(
                "Connector", ["AND", "OR", "NOT"],
                index=["AND", "OR", "NOT"].index(connectors[i - 1]),
                key=f"conn_{i}", label_visibility="collapsed",
            )
            st.markdown("")

        with st.container(border=True):
            col_name, col_field, col_int, col_del = st.columns([2.5, 1.5, 1.5, 0.7])
            group["name"] = col_name.text_input(
                "Group name", value=group.get("name", f"Group {i+1}"),
                key=f"gname_{i}", label_visibility="collapsed",
                placeholder=f"Group {i+1} name",
            )
            group["field"] = col_field.selectbox(
                "Field", ["keyword", "hashtag"],
                index=0 if group.get("field", "keyword") == "keyword" else 1,
                key=f"gfield_{i}",
            )
            group["internal_logic"] = col_int.selectbox(
                "Terms joined by", ["OR", "AND"],
                index=0 if group.get("internal_logic", "OR") == "OR" else 1,
                key=f"gint_{i}",
            )
            if col_del.button("✕", key=f"gdel_{i}", help="Remove this group") and len(groups) > 1:
                st.session_state.groups.pop(i)
                if i > 0 and i - 1 < len(st.session_state.connectors):
                    st.session_state.connectors.pop(i - 1)
                elif i == 0 and st.session_state.connectors:
                    st.session_state.connectors.pop(0)
                st.rerun()

            terms_raw = st.text_area(
                "Terms (one per line)", value="\n".join(group.get("terms", [])),
                height=100, key=f"gterms_{i}",
                placeholder="climate\nenvironment\nnet zero",
            )
            group["terms"] = [t.strip() for t in terms_raw.splitlines() if t.strip()]

    if st.button("➕ Add group"):
        st.session_state.groups.append({
            "name": f"Group {len(groups) + 1}",
            "field": "keyword", "terms": [], "internal_logic": "OR",
        })
        st.session_state.connectors.append("AND")
        st.rerun()

    st.divider()

    with st.expander("⚙️ Run settings"):
        max_iterations = st.number_input(
            "Max pages per date chunk (100 videos/page)",
            min_value=1, max_value=500, value=pget("max_iterations", 100),
        )
        output_path = st.text_input(
            "Output CSV file path",
            value=pget("output_path", "tiktok_results.csv"),
            placeholder="C:/Users/you/Desktop/results.csv",
        )

    # current_config is used both for running a collection and as the source for
    # preset saving. Credentials are kept separate so they are never written to
    # the presets file.
    current_config = {
        "start_date": start_date.strftime("%Y%m%d"),
        "end_date": end_date.strftime("%Y%m%d"),
        "region_codes": region_codes,
        "usernames": usernames,
        "video_length": video_length,
        "groups": [dict(g) for g in st.session_state.groups],
        "connectors": list(st.session_state.connectors),
        "use_view": use_view,
        "min_view_count": int(min_view) if use_view and min_view is not None else None,
        "max_view_count": int(max_view) if use_view and max_view is not None else None,
        "use_comment": use_comment,
        "min_comment_count": int(min_comment) if use_comment and min_comment is not None else None,
        "max_comment_count": int(max_comment) if use_comment and max_comment is not None else None,
        "max_iterations": int(max_iterations),
        "output_path": output_path,
    }
    st.session_state["current_config"] = current_config
    # Credentials are kept separate and only used at run time
    _run_credentials = {"client_key": client_key, "client_secret": client_secret}

    # Contextual query guidance
    has_usernames = bool([u.strip() for u in usernames if u.strip()])
    has_terms = any(
        t.strip()
        for g in st.session_state.groups
        for t in g.get("terms", [])
    )

    if has_usernames and not has_terms:
        st.info(
            "**Username-only mode** - your query will return all videos posted by the specified "
            "user(s) within the selected date range, with no keyword or hashtag filtering applied. "
            "This is the right approach if you want everything from a set of accounts."
        )
    elif not has_usernames and not has_terms and not region_codes:
        st.warning(
            "⚠️ Your query has no conditions set - add at least one username, keyword, hashtag, "
            "or region before running."
        )

    with st.expander("👁️ Query preview"):
        st.markdown("**Plain English:**")
        st.code(query_summary(current_config), language=None)
        st.markdown("**JSON sent to API:**")
        st.json(build_query(current_config))

    st.divider()
    ready = client_key and client_secret and output_path and start_date <= end_date
    if not ready:
        missing = []
        if not client_key or not client_secret:
            missing.append("API credentials (sidebar)")
        if not output_path:
            missing.append("output file path")
        if start_date > end_date:
            missing.append("valid date range")
        st.warning(f"Please fill in: {', '.join(missing)}")

    if st.button("▶️ Run video search", type="primary", disabled=not ready):
        log_area = st.empty()
        log_lines = []

        def log(msg):
            log_lines.append(msg)
            log_area.text("\n".join(log_lines))

        log("Authenticating...")
        try:
            auth = fetch_access_token(client_key, client_secret)
            log("Authenticated.\n")
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            st.stop()

        with st.spinner("Collecting videos..."):
            try:
                total = run_video_collection(current_config, auth, output_path, log)
                st.success(f"✅ {total} videos saved to `{output_path}`")
                st.info("💬 Switch to **Collect Comments** mode to pull comments for these videos.")
            except Exception as e:
                st.error(f"Collection failed: {e}")

# COLLECT COMMENTS

elif mode == "💬 Collect Comment Data":

    st.subheader("💬 Collect Comment Data")
    st.caption("Pull comments for a list of videos. You can sample down the list before running to manage your API request budget.")

    # Video source
    source_options = ["Upload a CSV file"]
    if st.session_state.last_video_csv and os.path.exists(st.session_state.last_video_csv):
        source_options.insert(0, f"Use current session results ({Path(st.session_state.last_video_csv).name})")

    video_source = st.radio("Video source", source_options, horizontal=True)

    videos = []
    if "Use current session results" in video_source:
        videos = read_video_csv(st.session_state.last_video_csv)
        st.success(f"✅ Loaded {len(videos)} videos from `{st.session_state.last_video_csv}`")
    else:
        uploaded = st.file_uploader("Upload video CSV", type=["csv"])
        if uploaded:
            videos = read_video_csv_from_upload(uploaded)
            st.success(f"✅ Loaded {len(videos)} videos from uploaded file")
        else:
            st.info("Upload a CSV from the Video Search mode (`id`, `username`, `video_description`, `voice_to_text` columns required).")

    st.divider()

    # Sampling
    st.subheader("Video sampling (optional)")
    st.caption("Reduce the number of videos before fetching comments to stay within your daily API budget.")

    use_sampling = st.checkbox("Sample videos before fetching comments", value=False)
    sampled_videos = videos

    if use_sampling and videos:
        col1, col2 = st.columns(2)
        sample_method = col1.selectbox(
            "Selection method",
            ["Top N by view count", "Top N by comment count", "Random sample"],
        )
        sample_n = col2.number_input(
            "Number of videos to sample",
            min_value=1, max_value=len(videos), value=min(100, len(videos)),
        )

        method_explanations = {
            "Top N by view count": (
                "Selects the most-viewed videos in your dataset. "
                "Useful if you want comments on content that had the most reach."
            ),
            "Top N by comment count": (
                "Selects the videos that generated the most discussion. "
                "Useful if you are specifically interested in audience engagement and response."
            ),
            "Random sample": (
                "Selects videos at random from your dataset. "
                "Useful if you want a representative cross-section rather than focusing on high-performing content."
            ),
        }
        st.caption(method_explanations[sample_method])

        sampled_videos = sample_videos(videos, sample_method, int(sample_n))
        st.info(f"**{len(sampled_videos)}** videos selected out of {len(videos)} ({sample_method})")

    # Request budget estimate
    if sampled_videos:
        st.divider()
        st.subheader("📊 API request estimate")
        st.caption(
            "The TikTok Research API allows **1,000 requests per day** (resets at midnight UTC), "
            "shared across all API calls (video search, comments, user info, etc.). "
            "Each request returns up to 100 records."
        )

        _max_pages_preview = st.session_state.get("_max_comment_pages", 10)

        # Use actual comment counts from the CSV to calculate exact page requirements
        total_comments = sum(int(v.get("comment_count") or 0) for v in sampled_videos)
        exact_pages = sum(
            min(math.ceil(int(v.get("comment_count") or 0) / 100), _max_pages_preview)
            for v in sampled_videos
        )
        capped_videos = sum(
            1 for v in sampled_videos
            if math.ceil(int(v.get("comment_count") or 0) / 100) > _max_pages_preview
        )

        col1, col2, col3 = st.columns(3)
        col1.metric("Videos to process", len(sampled_videos))
        col2.metric("Total comments (from search data)", f"{total_comments:,}")
        col3.metric("Estimated API requests needed", exact_pages)

        budget_pct = exact_pages / DAILY_REQUEST_BUDGET * 100

        if capped_videos > 0:
            st.info(
                f"ℹ️ {capped_videos} video(s) have more comments than your page cap of {_max_pages_preview} "
                f"pages × 100 = {_max_pages_preview * 100:,} comments will retrieve. "
                f"Increase the page cap in Run Settings if you want all comments."
            )

        if exact_pages > DAILY_REQUEST_BUDGET:
            st.warning(
                f"⚠️ This run needs **{exact_pages} requests** ({budget_pct:.0f}% of your 1,000 daily limit). "
                f"Remember this budget is shared with your video search requests. "
                f"Consider sampling fewer videos or lowering the page cap."
            )
        elif budget_pct > 50:
            st.warning(
                f"⚠️ This run will use ~{exact_pages} requests ({budget_pct:.0f}% of your daily budget). "
                f"Remember this is shared with your video search requests."
            )
        else:
            st.success(
                f"✅ Estimated usage: {exact_pages} requests ({budget_pct:.0f}% of your 1,000 daily limit)."
            )

    st.divider()

    # Run settings
    with st.expander("⚙️ Run settings", expanded=True):
        col1, col2 = st.columns(2)
        max_comment_pages = col1.number_input(
            "Max pages per video (100 comments/page)",
            min_value=1, max_value=500, value=10, key="max_comment_pages_input",
        )
        # Keep in sync for the budget preview above
        st.session_state["_max_comment_pages"] = int(max_comment_pages)

        max_workers = col2.number_input(
            "Parallel threads", min_value=1, max_value=10, value=5,
            help="How many videos to fetch simultaneously. Higher = faster but more likely to hit rate limits.",
        )
        output_path_comments = st.text_input(
            "Output CSV file path",
            value="tiktok_comments.csv",
            placeholder="C:/Users/you/Desktop/comments.csv",
        )

    st.divider()
    ready = client_key and client_secret and len(sampled_videos) > 0 and output_path_comments
    if not ready:
        missing = []
        if not client_key or not client_secret:
            missing.append("API credentials (sidebar)")
        if not sampled_videos:
            missing.append("video list")
        if not output_path_comments:
            missing.append("output file path")
        st.warning(f"Please fill in: {', '.join(missing)}")

    if st.button("▶️ Run comment collection", type="primary", disabled=not ready):
        log_area = st.empty()
        log_lines = []

        def log(msg):
            log_lines.append(msg)
            log_area.text("\n".join(log_lines))

        log("Authenticating...")
        try:
            auth = fetch_access_token(client_key, client_secret)
            log("Authenticated.\n")
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            st.stop()

        with st.spinner("Collecting comments..."):
            try:
                total = run_comment_collection(
                    sampled_videos, auth, output_path_comments,
                    int(max_comment_pages), int(max_workers), log,
                )
                st.success(f"✅ {total} comments saved to `{output_path_comments}`")
            except Exception as e:
                st.error(f"Collection failed: {e}")

# USER DATA

elif mode == "👤 Collect User Data":

    st.subheader("👤 Collect User Data")
    st.caption(
        "Query data about specific users - their pinned videos, liked videos, followers, or following list. "
        "Enter one username per line."
    )

    ENDPOINT_OPTIONS = {
        "📌 Pinned Videos": "pinned_videos",
        "❤️ Liked Videos": "liked_videos",
        "👥 Followers": "followers",
        "➡️ Following": "following",
    }

    endpoint_label = st.radio(
        "What do you want to collect?",
        list(ENDPOINT_OPTIONS.keys()),
        horizontal=True,
    )
    endpoint_key = ENDPOINT_OPTIONS[endpoint_label]

    is_paginated = endpoint_key != "pinned_videos"
    is_video_output = endpoint_key in ("pinned_videos", "liked_videos")

    if endpoint_key == "pinned_videos":
        st.info("ℹ️ Pinned videos are a single request per user - no pagination needed.")
    elif endpoint_key in ("followers", "following"):
        st.info(
            "ℹ️ Followers/Following return only `username` and `display_name`. "
            "This endpoint has a separate daily limit of 20,000 requests rather than the usual 1,000."
        )

    st.divider()

    usernames_input = st.text_area(
        "Usernames to query (one per line)",
        height=150,
        placeholder="username1\nusername2\nusername3",
    )
    usernames_list = [u.strip() for u in usernames_input.splitlines() if u.strip()]

    if usernames_list:
        st.caption(f"{len(usernames_list)} username(s) entered")

    st.divider()

    with st.expander("⚙️ Run settings", expanded=True):
        max_pages_user = 10
        if is_paginated:
            max_pages_user = st.number_input(
                "Max pages per user (100 records/page)",
                min_value=1, max_value=500, value=10,
                help="Only applies to liked videos, followers, and following - pinned videos are always a single request.",
            )

        default_filename = f"tiktok_{endpoint_key}.csv"
        output_path_user = st.text_input(
            "Output CSV file path",
            value=default_filename,
            placeholder=f"C:/Users/you/Desktop/{default_filename}",
        )

    if usernames_list and is_paginated:
        st.divider()
        st.subheader("📊 API request estimate")
        min_req = len(usernames_list)
        max_req = len(usernames_list) * max_pages_user
        budget = 20000 if endpoint_key in ("followers", "following") else DAILY_REQUEST_BUDGET
        st.caption(
            f"Daily request limit for this endpoint: **{budget:,}/day** (resets at midnight UTC)."
        )
        col1, col2, col3 = st.columns(3)
        col1.metric("Users to query", len(usernames_list))
        col2.metric("Min requests (1 page each)", min_req)
        col3.metric("Max requests (all pages)", max_req)
        pct_max = max_req / budget * 100
        if max_req > budget:
            st.warning(f"⚠️ Worst case {max_req} requests ({pct_max:.0f}% of daily limit). Consider reducing max pages.")
        elif pct_max > 50:
            st.warning(f"⚠️ Up to {max_req} requests ({pct_max:.0f}% of daily limit).")
        else:
            st.success(f"✅ Estimated {min_req}–{max_req} requests ({max_req / budget * 100:.0f}% of daily limit at most).")

    st.divider()
    ready = client_key and client_secret and len(usernames_list) > 0 and output_path_user
    if not ready:
        missing = []
        if not client_key or not client_secret:
            missing.append("API credentials (sidebar)")
        if not usernames_list:
            missing.append("at least one username")
        if not output_path_user:
            missing.append("output file path")
        st.warning(f"Please fill in: {', '.join(missing)}")

    if st.button("▶️ Run", type="primary", disabled=not ready):
        log_area = st.empty()
        log_lines = []

        def log(msg):
            log_lines.append(msg)
            log_area.text("\n".join(log_lines))

        log("Authenticating...")
        try:
            auth = fetch_access_token(client_key, client_secret)
            log("Authenticated.\n")
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            st.stop()

        with st.spinner(f"Collecting {endpoint_label}..."):
            try:
                total = run_user_data_collection(
                    usernames_list, endpoint_key, auth,
                    output_path_user, int(max_pages_user), log,
                )
                st.success(f"✅ {total} records saved to `{output_path_user}`")
            except Exception as e:
                st.error(f"Collection failed: {e}")

# PLAYLIST INFO

elif mode == "🎞️ Collect Playlist Data":

    st.subheader("🎞️ Collect Playlist Data")
    st.caption(
        "Retrieve metadata and the full list of video IDs for one or more playlists. "
        "Enter one playlist ID per line - these are the numeric IDs found in TikTok playlist URLs."
    )

    playlist_ids_raw = st.text_area(
        "Playlist IDs (one per line)",
        height=150,
        placeholder="1234569763387255595\n9876543210987654321",
    )
    playlist_ids = [p.strip() for p in playlist_ids_raw.splitlines() if p.strip().isdigit()]

    if playlist_ids_raw.strip() and not playlist_ids:
        st.warning("Playlist IDs must be numeric. Check your input.")
    elif playlist_ids:
        st.caption(f"{len(playlist_ids)} playlist ID(s) entered")

    st.divider()

    with st.expander("⚙️ Run settings", expanded=True):
        max_playlist_pages = st.number_input(
            "Max pages per playlist (100 video IDs/page)",
            min_value=1, max_value=200, value=10,
        )
        output_path_playlist = st.text_input(
            "Output CSV file path",
            value="tiktok_playlists.csv",
            placeholder="C:/Users/you/Desktop/tiktok_playlists.csv",
        )

    st.divider()
    ready = client_key and client_secret and len(playlist_ids) > 0 and output_path_playlist
    if not ready:
        missing = []
        if not client_key or not client_secret:
            missing.append("API credentials (sidebar)")
        if not playlist_ids:
            missing.append("at least one valid playlist ID")
        if not output_path_playlist:
            missing.append("output file path")
        st.warning(f"Please fill in: {', '.join(missing)}")

    if st.button("▶️ Run", type="primary", disabled=not ready, key="run_playlist"):
        log_area = st.empty()
        log_lines = []

        def log(msg):
            log_lines.append(msg)
            log_area.text("\n".join(log_lines))

        log("Authenticating...")
        try:
            auth = fetch_access_token(client_key, client_secret)
            log("Authenticated.\n")
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            st.stop()

        total_videos = 0
        with st.spinner("Fetching playlist data..."):
            for pid in playlist_ids:
                log(f"\n📋 Fetching playlist: {pid}")
                try:
                    result = fetch_playlist(int(pid), auth, int(max_playlist_pages), log)
                    save_playlist_csv(result, output_path_playlist)
                    n = len(result.get("video_ids", []))
                    total_videos += n
                    log(f"  💾 {n} video IDs saved for playlist '{result.get('playlist_name', pid)}'")
                except Exception as e:
                    log(f"  ❌ Failed for playlist {pid}: {e}")

        st.success(f"✅ Done - {total_videos} video IDs saved across {len(playlist_ids)} playlist(s) to `{output_path_playlist}`")

# TIKTOK SHOP

elif mode == "🛒 Collect TikTok Shop Data":

    st.subheader("Collect TikTok Shop Data")
    st.caption(
        "Query TikTok Shop data for shops operating in the EU. "
        "Search by shop name to get shop details, then drill down to products and reviews. "
        "Run each step in order, or run all three together."
    )
    st.info("ℹ️ TikTok Shop data is only available for shops operating in the EU.")

    shop_step = st.radio(
        "What do you want to collect?",
        ["🏪 Shop Info (search by name)", "📦 Products (from shop ID)", "⭐ Reviews (from product ID)", "🔄 All three in sequence"],
        horizontal=False,
    )

    st.divider()

    # Step 1: Shop Info
    if shop_step in ("🏪 Shop Info (search by name)", "🔄 All three in sequence"):
        st.markdown("**Step 1 - Shop Info**")
        st.caption("Search by shop name. Returns shop ID, rating, review count, and items sold.")
        shop_names_raw = st.text_area(
            "Shop names to search (one per line)",
            height=120,
            placeholder="My Shop Name\nAnother Shop",
            key="shop_names",
        )
        shop_names = [s.strip() for s in shop_names_raw.splitlines() if s.strip()]
        output_shop_info = st.text_input("Shop info output CSV", value="tiktok_shop_info.csv", key="out_shop_info")

    # Step 2: Products
    if shop_step in ("📦 Products (from shop ID)", "🔄 All three in sequence"):
        st.markdown("**Step 2 - Products**")
        if shop_step == "📦 Products (from shop ID)":
            st.caption("Enter shop IDs to retrieve all products. Get shop IDs from the Shop Info step.")
            shop_ids_raw = st.text_area(
                "Shop IDs (one per line)",
                height=100,
                placeholder="123456789\n987654321",
                key="shop_ids_manual",
            )
            manual_shop_ids = [s.strip() for s in shop_ids_raw.splitlines() if s.strip().isdigit()]
        else:
            st.caption("Shop IDs will be taken from the Shop Info results above.")
            manual_shop_ids = []

        max_product_pages = st.number_input(
            "Max pages per shop (10 products/page)", min_value=1, max_value=100, value=10, key="max_prod_pages"
        )
        output_products = st.text_input("Products output CSV", value="tiktok_shop_products.csv", key="out_products")

    # Step 3: Reviews
    if shop_step in ("⭐ Reviews (from product ID)", "🔄 All three in sequence"):
        st.markdown("**Step 3 - Reviews**")
        if shop_step == "⭐ Reviews (from product ID)":
            st.caption("Enter product IDs to retrieve all reviews. Get product IDs from the Products step.")
            product_ids_raw = st.text_area(
                "Product IDs (one per line)",
                height=100,
                placeholder="111222333\n444555666",
                key="product_ids_manual",
            )
            manual_product_ids = [p.strip() for p in product_ids_raw.splitlines() if p.strip().isdigit()]
        else:
            st.caption("Product IDs will be taken from the Products results above.")
            manual_product_ids = []

        max_review_pages = st.number_input(
            "Max pages per product (10 reviews/page)", min_value=1, max_value=100, value=10, key="max_rev_pages"
        )
        output_reviews = st.text_input("Reviews output CSV", value="tiktok_shop_reviews.csv", key="out_reviews")

    st.divider()

    # Validation
    ready = client_key and client_secret
    if not ready:
        st.warning("Please fill in API credentials (sidebar)")

    if st.button("▶️ Run", type="primary", disabled=not ready, key="run_shop"):
        log_area = st.empty()
        log_lines = []

        def log(msg):
            log_lines.append(msg)
            log_area.text("\n".join(log_lines))

        log("Authenticating...")
        try:
            auth = fetch_access_token(client_key, client_secret)
            log("Authenticated.\n")
        except Exception as e:
            st.error(f"Authentication failed: {e}")
            st.stop()

        collected_shop_ids = []
        collected_product_ids = []

        with st.spinner("Collecting shop data..."):

            # Step 1: Shop info
            if shop_step in ("🏪 Shop Info (search by name)", "🔄 All three in sequence"):
                log("Fetching shop info...")
                for name in shop_names:
                    try:
                        shops = fetch_shop_info(name, auth)
                        save_shop_info_csv(shops, output_shop_info)
                        for s in shops:
                            sid = s.get("shop_id")
                            if sid:
                                collected_shop_ids.append(sid)
                        log(f"  ✅ '{name}': {len(shops)} shop(s) found")
                    except Exception as e:
                        log(f"  ❌ Failed for '{name}': {e}")
                if shop_step == "🏪 Shop Info (search by name)":
                    st.success(f"✅ Shop info saved to `{output_shop_info}`")

            # Step 2: Products
            if shop_step in ("📦 Products (from shop ID)", "🔄 All three in sequence"):
                shop_ids_to_use = collected_shop_ids if shop_step == "🔄 All three in sequence" else manual_shop_ids
                log(f"\nFetching products for {len(shop_ids_to_use)} shop(s)...")
                for sid in shop_ids_to_use:
                    try:
                        products = fetch_shop_products(int(sid), auth, int(max_product_pages), log)
                        save_shop_products_csv(products, int(sid), output_products)
                        for p in products:
                            pid = p.get("product_id")
                            if pid:
                                collected_product_ids.append(pid)
                        log(f"  ✅ Shop {sid} - {len(products)} products saved")
                    except Exception as e:
                        log(f"  ❌ Failed for shop {sid}: {e}")
                if shop_step == "📦 Products (from shop ID)":
                    st.success(f"✅ Products saved to `{output_products}`")

            # Step 3: Reviews
            if shop_step in ("⭐ Reviews (from product ID)", "🔄 All three in sequence"):
                product_ids_to_use = collected_product_ids if shop_step == "🔄 All three in sequence" else manual_product_ids
                log(f"\nFetching reviews for {len(product_ids_to_use)} product(s)...")
                for pid in product_ids_to_use:
                    try:
                        reviews = fetch_product_reviews(int(pid), auth, int(max_review_pages), log)
                        save_shop_reviews_csv(reviews, int(pid), output_reviews)
                        log(f"  ✅ Product {pid} - {len(reviews)} reviews saved")
                    except Exception as e:
                        log(f"  ❌ Failed for product {pid}: {e}")
                if shop_step == "⭐ Reviews (from product ID)":
                    st.success(f"✅ Reviews saved to `{output_reviews}`")

            if shop_step == "🔄 All three in sequence":
                st.success(
                    f"✅ All done!\n"
                    f"- Shop info → `{output_shop_info}`\n"
                    f"- Products → `{output_products}`\n"
                    f"- Reviews → `{output_reviews}`"
                )
