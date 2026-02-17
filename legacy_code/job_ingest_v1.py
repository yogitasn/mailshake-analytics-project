# ============================================================================
# IMPORTS
# ============================================================================
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import boto3
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd

# ============================================================================
# GLOBALS
# ============================================================================
BASE = "https://api.mailshake.com/2017-04-01"
HEADERS = {"Content-Type": "application/json"}
BUCKET = "mailshake-analytics"
RAW_PREFIX = "raw"
WATERMARK_PREFIX = "metadata/watermark"
TEAMS_KEY = "config/teams_test3_sample.json"

s3 = boto3.client("s3")
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

API_CALL_DELAY_FIRST_RUN = 3.0
API_CALL_DELAY_INCREMENTAL = 1.0

MAX_PAGES_FIRST_RUN = 100
MAX_PAGES_INCREMENTAL = 3

is_first_run_mode = False
api_call_count = 0
start_time = None

# ============================================================================
# TEAM CONFIG
# ============================================================================
def load_teams() -> Dict[str, Dict[str, str]]:
    obj = s3.get_object(Bucket=BUCKET, Key=TEAMS_KEY)
    return json.loads(obj["Body"].read().decode("utf-8")).get("teams", {})

# ============================================================================
# SAFE POST
# ============================================================================
def safe_post(url: str, payload: Dict[str, Any], auth: HTTPBasicAuth) -> requests.Response:
    global api_call_count, start_time

    if start_time is None:
        start_time = time.time()

    delay = API_CALL_DELAY_FIRST_RUN if is_first_run_mode else API_CALL_DELAY_INCREMENTAL
    time.sleep(delay)

    api_call_count += 1
    elapsed = time.time() - start_time
    rate = api_call_count / (elapsed / 60) if elapsed > 0 else 0

    resp = requests.post(url, json=payload, headers=HEADERS, auth=auth, timeout=30)

    if resp.status_code == 429:
        endpoint = url.split("/")[-1]
        mode = "FIRST RUN" if is_first_run_mode else "INCREMENTAL"
        print("\n🚫 RATE LIMIT HIT")
        print(f"Endpoint: {endpoint}")
        print(f"Mode: {mode}")
        print(f"Rate: {rate:.1f} calls/min")
        raise SystemExit(1)

    resp.raise_for_status()
    return resp

# ============================================================================
# WATERMARK HELPERS
# ============================================================================
def read_watermarks(team_id: str) -> dict:
    key = f"{WATERMARK_PREFIX}/watermark_{team_id}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}

def update_watermarks(team_id: str, new_data: dict):
    key = f"{WATERMARK_PREFIX}/watermark_{team_id}.json"
    current = read_watermarks(team_id)
    current.update(new_data)
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(current, indent=2).encode("utf-8"))

def get_watermark(team_id: str, entity: str) -> str:
    return read_watermarks(team_id).get(entity, "1970-01-01T00:00:00Z")

def normalize_iso(ts: str) -> str:
    if not ts:
        return ts
    if ts.endswith("Z"):
        return ts.replace("Z", "+00:00")
    if len(ts) >= 5:
        tz = ts[-5:]
        if tz[0] in "+-" and tz[1:].isdigit() and ":" not in tz:
            return ts[:-5] + tz[:3] + ":" + tz[3:]
    return ts

# ============================================================================
# SAVE PARQUET
# ============================================================================
def save_snapshot_or_incremental(
    data: list, team_id: str, entity: str, ts_col: str, first_run: bool
):
    if not data:
        return None

    df = pd.DataFrame(data)

    if ts_col in df.columns:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
        df = df.dropna(subset=[ts_col])
        if df.empty:
            return None
        df[ts_col] = df[ts_col].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    batch_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")

    # FULL
    if entity in ["campaign", "created_leads"]:
        file = f"/tmp/{team_id}_{entity}_full.parquet"
        key = f"{RAW_PREFIX}/team_id={team_id}/entity={entity}/snapshot/{entity}.parquet"
        df.to_parquet(file, index=False)
        s3.upload_file(file, BUCKET, key)
        print(f"✓ {entity}: {len(df)} records (FULL)")
        return df[ts_col].max() if ts_col in df.columns else batch_ts

    # FIRST RUN SNAPSHOT
    if first_run and entity.startswith("activity_"):
        file = f"/tmp/{team_id}_{entity}_snapshot.parquet"
        key = f"{RAW_PREFIX}/team_id={team_id}/entity={entity}/snapshot/{entity}.parquet"
        df.to_parquet(file, index=False)
        s3.upload_file(file, BUCKET, key)
        print(f"✓ {entity}: {len(df)} records (SNAPSHOT 20d)")
        return df[ts_col].max()

    # INCREMENTAL
    current_wm = get_watermark(team_id, entity)
    if ts_col in df.columns:
        df = df[df[ts_col] > current_wm]
    if df.empty:
        return None

    df["event_date"] = pd.to_datetime(df[ts_col], utc=True).dt.strftime("%Y-%m-%d")
    new_wm = df[ts_col].max()

    total = 0
    for d in df["event_date"].unique():
        part = df[df["event_date"] == d].drop(columns=["event_date"])
        suffix = new_wm.replace(":", "").replace("-", "")
        file = f"/tmp/{team_id}_{entity}_{d}_{suffix}.parquet"
        key = f"{RAW_PREFIX}/team_id={team_id}/entity={entity}/event_date={d}/{entity}_{suffix}.parquet"
        part.to_parquet(file, index=False)
        s3.upload_file(file, BUCKET, key)
        total += len(part)

    print(f"✓ {entity}: {total} records (INCREMENTAL)")
    return new_wm

# ============================================================================
# FETCH FUNCTIONS
# ============================================================================
def fetch_campaigns(team_id: str, auth: HTTPBasicAuth) -> List[Dict[str, Any]]:
    resp = safe_post(f"{BASE}/campaigns/list", {"teamID": team_id}, auth)
    return resp.json().get("results", []) or []

def fetch_activity_with_since(
    team_id: str, api_name: str, since_ts: str, auth: HTTPBasicAuth, first_run: bool
) -> List[Dict[str, Any]]:
    payload = {"teamID": team_id, "perPage": 100}

    if first_run:
        cutoff = datetime.now(timezone.utc) - timedelta(days=20)
        payload["since"] = cutoff.strftime("%Y-%m-%d %H:%M:%S")
        max_pages = MAX_PAGES_FIRST_RUN
    elif since_ts and since_ts != "1970-01-01T00:00:00Z":
        since_dt = datetime.fromisoformat(normalize_iso(since_ts))
        payload["since"] = since_dt.strftime("%Y-%m-%d %H:%M:%S")
        max_pages = MAX_PAGES_INCREMENTAL
    else:
        max_pages = MAX_PAGES_FIRST_RUN

    results_all = []
    page = 0

    while True:
        page += 1
        if page > max_pages:
            break

        resp = safe_post(f"{BASE}/activity/{api_name}", payload, auth)
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        results_all.extend(results)
        token = data.get("nextToken")
        if not token:
            break

        payload = {"teamID": team_id, "nextToken": token, "perPage": 100}
        if "since" in payload:
            payload["since"] = payload.get("since")

    return results_all

def fetch_sent_activity(
    team_id: str, watermark: str, first_run: bool, auth: HTTPBasicAuth
) -> List[Dict[str, Any]]:
    payload = {"teamID": team_id, "perPage": 100}
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=20)
        if first_run
        else datetime.fromisoformat(normalize_iso(watermark))
    )

    results_all = []
    page = 0

    while True:
        page += 1
        if page > (MAX_PAGES_FIRST_RUN if first_run else MAX_PAGES_INCREMENTAL):
            break

        resp = safe_post(f"{BASE}/activity/sent", payload, auth)
        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        stop = False
        for r in results:
            ts = r.get("actionDate")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(normalize_iso(ts))
            except:
                continue
            if dt >= cutoff:
                results_all.append(r)
            else:
                stop = True

        if stop and not first_run:
            break

        token = data.get("nextToken")
        if not token:
            break

        payload = {"teamID": team_id, "nextToken": token, "perPage": 100}

    return results_all

# ============================================================================
# RUN TEAM
# ============================================================================
def run_team(team: Dict[str, str]):
    global is_first_run_mode

    team_id, auth = team["team_id"], HTTPBasicAuth(team["api_token"], "")
    print(f"\n🚀 Team {team_id}")

    watermarks = read_watermarks(team_id)
    is_first_run_mode = len(watermarks) == 0
    print("🆕 FIRST RUN" if is_first_run_mode else "🔄 INCREMENTAL RUN")

    campaigns = fetch_campaigns(team_id, auth)
    if campaigns:
        wm = save_snapshot_or_incremental(campaigns, team_id, "campaign", "created", True)
        if wm:
            update_watermarks(team_id, {"campaign": wm})

    activity_config = {
        "activity_sent": ("sent", "actionDate", False),
        "activity_open": ("opens", "actionDate", True),
        "activity_reply": ("replies", "actionDate", True),
        "activity_clicks": ("clicks", "actionDate", True),
        "created_leads": ("created-leads", "created", True),
    }

    watermarks = read_watermarks(team_id)

    for entity, (api, ts_col, supports_since) in activity_config.items():
        first_run = entity not in watermarks
        watermark = watermarks.get(entity, "1970-01-01T00:00:00Z")

        data = (
            fetch_activity_with_since(team_id, api, watermark, auth, first_run)
            if supports_since
            else fetch_sent_activity(team_id, watermark, first_run, auth)
        )

        if not data:
            continue

        new_wm = save_snapshot_or_incremental(data, team_id, entity, ts_col, first_run)
        if new_wm:
            update_watermarks(team_id, {entity: new_wm})

    print(f"✅ Team {team_id} completed")

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    teams = load_teams()

    print(f"Teams to process: {len(teams)}")

    for _, team_cfg in teams.items():
        try:
            run_team(team_cfg)
        except SystemExit:
            print("🚫 RATE LIMIT — STOPPING")
            break
        except Exception as e:
            print(f"❌ Team failed: {e}")
            import traceback
            traceback.print_exc()

    if start_time:
        elapsed = time.time() - start_time
        print(f"\n🏁 JOB FINISHED")
        print(f"Total API calls: {api_call_count}")
        print(f"Total time: {elapsed/60:.1f} min")
        print(f"Avg rate: {api_call_count/(elapsed/60):.1f} calls/min")
