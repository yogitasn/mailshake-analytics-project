# ============================================================================
# IMPORTS
# ============================================================================
import json
from datetime import datetime
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
CLIENTS_KEY = "config/clients_test.json"
s3 = boto3.client("s3")
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")

# ============================================================================
# CLIENT CONFIG
# ============================================================================
def load_clients() -> Dict[str, Dict[str, str]]:
    obj = s3.get_object(Bucket=BUCKET, Key=CLIENTS_KEY)
    return json.loads(obj["Body"].read().decode("utf-8")).get("clients", {})

# ============================================================================
# SAFE POST
# ============================================================================
def safe_post(url: str, payload: Dict[str, Any], auth: HTTPBasicAuth) -> requests.Response:
    resp = requests.post(url, json=payload, headers=HEADERS, auth=auth, timeout=30)
    if resp.status_code == 429:
        print("🚫 API RATE LIMIT HIT — STOPPING JOB")
        raise SystemExit(1)
    resp.raise_for_status()
    return resp

# ============================================================================
# WATERMARK HELPERS
# ============================================================================
def read_watermarks(client_id: str) -> dict:
    key = f"{WATERMARK_PREFIX}/watermark_{client_id}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return {}

def update_watermarks(client_id: str, new_data: dict):
    key = f"{WATERMARK_PREFIX}/watermark_{client_id}.json"
    current = read_watermarks(client_id)
    current.update(new_data)
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(current, indent=2).encode("utf-8"))

def get_watermark(client_id: str, entity: str) -> str:
    watermarks = read_watermarks(client_id)
    return watermarks.get(entity, "1970-01-01T00:00:00Z")

# ============================================================================
# SAVE PARQUET (SNAPSHOT OR INCREMENTAL)
# ============================================================================
# ============================================================================
# SAVE PARQUET (SNAPSHOT OR INCREMENTAL) — FULL SPARK SAFE WITH WATERMARK
# ============================================================================
def save_snapshot_or_incremental(
    data: list, client_id: str, entity: str, ts_col: str, first_run: bool
):
    if not data:
        print(f"⚠️ No data for {entity}")
        return None

    df = pd.DataFrame(data)

    # Convert timestamp column if exists
    if ts_col in df.columns:
        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
        df = df.dropna(subset=[ts_col])

        if df.empty:
            print(f"⚠️ No valid timestamps for {entity}")
            return None

        # Spark-safe ISO format
        df[ts_col] = df[ts_col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    else:
        print(f"⚠️ {ts_col} missing — saving anyway")

    batch_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    # -------------------------
    # FIRST RUN → SNAPSHOT
    # -------------------------
    if first_run:
        file = f"/tmp/{client_id}_{entity}_snapshot.parquet"
        key = f"{RAW_PREFIX}/client_id={client_id}/entity={entity}/snapshot/{entity}.parquet"
        df.to_parquet(file, index=False)
        s3.upload_file(file, BUCKET, key)
        print(f"🟦 SNAPSHOT → {entity}")
        return df[ts_col].max() if ts_col in df.columns else None

    # -------------------------
    # FILTER BY SAVED WATERMARK
    # -------------------------
    current_wm = get_watermark(client_id, entity)
    print(current_wm)
    if ts_col in df.columns and current_wm:
        current_wm_dt = pd.to_datetime(current_wm, utc=True)
        df = df[pd.to_datetime(df[ts_col], utc=True) > current_wm_dt]
    if df.empty:
        print(f"⚠️ No new records for {entity} after watermark {current_wm}")
        return None

    # -------------------------
    # CREATED_LEADS → run_date partition
    # -------------------------
    if entity == "created_leads":
        new_wm = df[ts_col].max() if ts_col in df.columns else batch_ts
        wm_suffix = new_wm.replace(":", "").replace("-", "")
        file = f"/tmp/{client_id}_{entity}_{wm_suffix}.parquet"
        key = f"{RAW_PREFIX}/client_id={client_id}/entity={entity}/run_date={RUN_DATE}/{entity}_{wm_suffix}.parquet"
        df.to_parquet(file, index=False)
        s3.upload_file(file, BUCKET, key)
        print(f"🟢 INCREMENTAL → {entity} ({len(df)})")
        return new_wm

    # -------------------------
    # ACTIVITIES → event_date partition
    # -------------------------
    if ts_col in df.columns:
        df["event_date"] = pd.to_datetime(df[ts_col], utc=True).dt.strftime("%Y-%m-%d")
        new_wm = df[ts_col].max()
    else:
        df["event_date"] = RUN_DATE
        new_wm = batch_ts

    for d in df["event_date"].unique():
        part = df[df["event_date"] == d].copy()
        part.drop(columns=["event_date"], inplace=True)
        wm_suffix = new_wm.replace(":", "").replace("-", "")
        file = f"/tmp/{client_id}_{entity}_{d}_{wm_suffix}.parquet"
        key = f"{RAW_PREFIX}/client_id={client_id}/entity={entity}/event_date={d}/{entity}_{wm_suffix}.parquet"
        part.to_parquet(file, index=False)
        s3.upload_file(file, BUCKET, key)
        print(f"🟢 INCREMENTAL → {entity} ({len(part)})")

    return new_wm

# ============================================================================
# FETCH FUNCTIONS
# ============================================================================
def fetch_campaigns(team_id: str, auth: HTTPBasicAuth) -> List[Dict[str, Any]]:
    resp = safe_post(f"{BASE}/campaigns/list", {"teamID": team_id}, auth)
    return resp.json().get("results", []) or []

def fetch_activity(team_id: str, api_name: str, since_ts: str, auth: HTTPBasicAuth) -> List[Dict[str, Any]]:
    resp = safe_post(f"{BASE}/activity/{api_name}", {"teamID": team_id, "since_ts": since_ts}, auth)
    return resp.json().get("results", []) or []

# ============================================================================
# RUN SINGLE CLIENT
# ============================================================================
def run_client(client: Dict[str, str]):
    client_id, team_id, auth = client["client_id"], client["team_id"], HTTPBasicAuth(client["api_token"], "")
    print(f"\n🚀 STARTING CLIENT {client_id}")

    # -------- CAMPAIGNS (FULL LOAD) --------
    campaigns = fetch_campaigns(team_id, auth)
    if campaigns:
        wm = save_snapshot_or_incremental(campaigns, client_id, "campaign", "created", first_run=True)
        if wm:
            update_watermarks(client_id, {"campaign": wm})

    # -------- ACTIVITIES / CREATED_LEADS (INCREMENTAL) --------
    activity_map = {
        "activity_open": ("opens", "actionDate"),
        "activity_reply": ("replies", "actionDate"),
        "activity_sent": ("sent", "actionDate"),
        "created_leads": ("created-leads", "created")
    }

    watermarks = read_watermarks(client_id)

    for entity, (api, ts_col) in activity_map.items():
        watermark = watermarks.get(entity, "1970-01-01T00:00:00Z")
        first_run = entity not in watermarks
        print(f"➡ {entity} | first_run={first_run}")

        data = fetch_activity(team_id, api, watermark, auth)
        if not data:
            print(f"⚠️ No new records for {entity}")
            continue

        new_wm = save_snapshot_or_incremental(data, client_id, entity, ts_col, first_run)

        if new_wm:
            old_wm = watermarks.get(entity)
            if old_wm is None or new_wm > old_wm:
                update_watermarks(client_id, {entity: new_wm})
                watermarks[entity] = new_wm
                print(f"🔁 Watermark updated → {new_wm}")

    print(f"✅ CLIENT {client_id} COMPLETED")

# ============================================================================
# MAIN LOOP
# ============================================================================
if __name__ == "__main__":
    clients = load_clients()
    for client_id, client_cfg in clients.items():
        try:
            run_client({
                "client_id": client_id,
                "team_id": client_cfg["team_id"],
                "api_token": client_cfg["api_token"]
            })
        except SystemExit:
            print("🚫 API LIMIT HIT — STOPPING ALL CLIENTS")
            break
        except Exception as e:
            print(f"❌ Client {client_id} failed: {e}")

    print("\n🏁 JOB FINISHED")
