from pyspark.sql.types import *

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, BooleanType
)

CAMPAIGN_SCHEMA = StructType([

    # --------------------
    # Core campaign
    # --------------------
    StructField("object", StringType(), True),
    StructField("id", LongType(), True),
    StructField("title", StringType(), True),
    StructField("created", StringType(), True),
    StructField("isarchived", BooleanType(), True),
    StructField("ispaused", BooleanType(), True),
    StructField("wizardstatus", IntegerType(), True),
    StructField("url", StringType(), True),

    # --------------------
    # Sender
    # --------------------
    StructField("sender_object", StringType(), True),
    StructField("sender_id", LongType(), True),
    StructField("sender_emailaddress", StringType(), True),
    StructField("sender_fromname", StringType(), True),
    StructField("sender_created", StringType(), True),

    # --------------------
    # Message
    # --------------------
    StructField("messages_id", LongType(), True),
    StructField("messages_ispaused", BooleanType(), True),
    StructField("messages_object", StringType(), True),
    StructField("messages_replytoid", LongType(), True),
    StructField("messages_subject", StringType(), True),
    StructField("messages_type", StringType(), True),
])
ACTIVITY_OPEN_SCHEMA = StructType([
    StructField("object", StringType(), True),
    StructField("id", LongType(), True),
    StructField("actiondate", StringType(), True),
    StructField("isduplicate", BooleanType(), True),

    StructField("recipient_object", StringType(), True),
    StructField("recipient_id", LongType(), True),
    StructField("recipient_emailaddress", StringType(), True),
    StructField("recipient_fullname", StringType(), True),
    StructField("recipient_created", StringType(), True),
    StructField("recipient_ispaused", BooleanType(), True),
    StructField("recipient_contactid", LongType(), True),
    StructField("recipient_first", StringType(), True),
    StructField("recipient_last", StringType(), True),

    StructField("recipient_fields_link", StringType(), True),
    StructField("recipient_fields_status", StringType(), True),
    StructField("recipient_fields_first", StringType(), True),
    StructField("recipient_fields_position", StringType(), True),
    StructField("recipient_fields_date_applied", StringType(), True),
    StructField("recipient_fields_account", StringType(), True),
    StructField("recipient_fields_phonenumber", StringType(), True),
    StructField("recipient_fields_facebookurl", StringType(), True),
    StructField("recipient_fields_instagramid", StringType(), True),
    StructField("recipient_fields_linkedinurl", StringType(), True),
    StructField("recipient_fields_twitterid", StringType(), True),

    StructField("campaign_object", StringType(), True),
    StructField("campaign_id", LongType(), True),
    StructField("campaign_title", StringType(), True),
    StructField("campaign_wizardstatus", IntegerType(), True),

    StructField("parent_object", StringType(), True),
    StructField("parent_id", LongType(), True),
    StructField("parent_type", StringType(), True),
    StructField("parent_message_object", StringType(), True),
    StructField("parent_message_id", LongType(), True),
    StructField("parent_message_type", StringType(), True),
    StructField("parent_message_subject", StringType(), True),
    StructField("parent_message_replytoid", LongType(), True),
])

ACTIVITY_REPLY_SCHEMA = StructType([

    # --------------------
    # Core fields
    # --------------------
    StructField("object", StringType(), True),
    StructField("id", LongType(), True),
    StructField("actiondate", StringType(), True),
    StructField("type", StringType(), True),
    StructField("subject", StringType(), True),

    StructField("externalid", StringType(), True),
    StructField("externalrawmessageid", StringType(), True),
    StructField("externalconversationid", StringType(), True),

    StructField("rawbody", StringType(), True),
    StructField("body", StringType(), True),
    StructField("plaintextbody", StringType(), True),

    # --------------------
    # Recipient
    # --------------------
    StructField("recipient_object", StringType(), True),
    StructField("recipient_id", LongType(), True),
    StructField("recipient_emailaddress", StringType(), True),
    StructField("recipient_fullname", StringType(), True),
    StructField("recipient_created", StringType(), True),
    StructField("recipient_ispaused", BooleanType(), True),
    StructField("recipient_contactid", LongType(), True),
    StructField("recipient_first", StringType(), True),
    StructField("recipient_last", StringType(), True),

    # --------------------
    # Recipient fields
    # --------------------
    StructField("recipient_fields_link", StringType(), True),
    StructField("recipient_fields_status", StringType(), True),
    StructField("recipient_fields_first", StringType(), True),
    StructField("recipient_fields_position", StringType(), True),
    StructField("recipient_fields_date_applied", StringType(), True),
    StructField("recipient_fields_account", StringType(), True),
    StructField("recipient_fields_phonenumber", StringType(), True),
    StructField("recipient_fields_facebookurl", StringType(), True),
    StructField("recipient_fields_instagramid", StringType(), True),
    StructField("recipient_fields_linkedinurl", StringType(), True),
    StructField("recipient_fields_twitterid", StringType(), True),

    # --------------------
    # Campaign
    # --------------------
    StructField("campaign_object", StringType(), True),
    StructField("campaign_id", LongType(), True),
    StructField("campaign_title", StringType(), True),
    StructField("campaign_wizardstatus", IntegerType(), True),

    # --------------------
    # Parent / Message
    # --------------------
    StructField("parent_object", StringType(), True),
    StructField("parent_id", LongType(), True),
    StructField("parent_type", StringType(), True),

    StructField("parent_message_object", StringType(), True),
    StructField("parent_message_id", LongType(), True),
    StructField("parent_message_type", StringType(), True),
    StructField("parent_message_subject", StringType(), True),
    StructField("parent_message_replytoid", LongType(), True),

    # --------------------
    # From
    # --------------------
    StructField("from_object", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("from_fullname", StringType(), True),
    StructField("from_first", StringType(), True),
    StructField("from_last", StringType(), True),
])


ACTIVITY_SENT_SCHEMA = StructType([

    # --------------------
    # Core
    # --------------------
    StructField("object", StringType(), True),
    StructField("id", LongType(), True),
    StructField("actiondate", StringType(), True),
    StructField("type", StringType(), True),
    StructField("excludebody", BooleanType(), True),

    # --------------------
    # To (exploded from array)
    # --------------------
    StructField("to_address", StringType(), True),
    StructField("to_first", StringType(), True),
    StructField("to_fullname", StringType(), True),
    StructField("to_last", StringType(), True),
    StructField("to_object", StringType(), True),

    # --------------------
    # Message content
    # --------------------
    StructField("subject", StringType(), True),
    StructField("externalid", StringType(), True),
    StructField("externalrawmessageid", StringType(), True),
    StructField("externalconversationid", StringType(), True),

    StructField("rawbody", StringType(), True),
    StructField("body", StringType(), True),
    StructField("plaintextbody", StringType(), True),

    # --------------------
    # Recipient
    # --------------------
    StructField("recipient_object", StringType(), True),
    StructField("recipient_id", LongType(), True),
    StructField("recipient_emailaddress", StringType(), True),
    StructField("recipient_fullname", StringType(), True),
    StructField("recipient_created", StringType(), True),
    StructField("recipient_ispaused", BooleanType(), True),
    StructField("recipient_first", StringType(), True),
    StructField("recipient_last", StringType(), True),

    # --------------------
    # Recipient fields
    # --------------------
    StructField("recipient_fields_account", StringType(), True),
    StructField("recipient_fields_phonenumber", StringType(), True),
    StructField("recipient_fields_facebookurl", StringType(), True),
    StructField("recipient_fields_instagramid", StringType(), True),
    StructField("recipient_fields_linkedinurl", StringType(), True),
    StructField("recipient_fields_twitterid", StringType(), True),
    StructField("recipient_fields_link", StringType(), True),
    StructField("recipient_fields_position", StringType(), True),
    StructField("recipient_fields_date_applied", StringType(), True),
    StructField("recipient_fields_status", StringType(), True),

    # --------------------
    # Campaign
    # --------------------
    StructField("campaign_object", StringType(), True),
    StructField("campaign_id", LongType(), True),
    StructField("campaign_title", StringType(), True),
    StructField("campaign_wizardstatus", IntegerType(), True),

    # --------------------
    # Message (parent)
    # --------------------
    StructField("message_object", StringType(), True),
    StructField("message_id", LongType(), True),
    StructField("message_type", StringType(), True),
    StructField("message_subject", StringType(), True),
    StructField("message_replytoid", LongType(), True),

    # --------------------
    # From
    # --------------------
    StructField("from_object", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("from_fullname", StringType(), True),
    StructField("from_first", StringType(), True),
    StructField("from_last", StringType(), True),
])

ACTIVITY_CLICKS_SCHEMA = StructType([
    StructField("object", StringType(), True),
    StructField("id", LongType(), True),
    StructField("link", StringType(), True),
    StructField("actionDate", StringType(), True),
    StructField("isDuplicate", BooleanType(), True),

    StructField("recipient_contactID", LongType(), True),
    StructField("recipient_created", StringType(), True),
    StructField("recipient_emailAddress", StringType(), True),
    StructField("recipient_fields_account", StringType(), True),
    StructField("recipient_fields_facebookUrl", StringType(), True),
    StructField("recipient_fields_first", StringType(), True),
    StructField("recipient_fields_instagramID", StringType(), True),
    StructField("recipient_fields_last", StringType(), True),
    StructField("recipient_fields_linkedInUrl", StringType(), True),
    StructField("recipient_fields_phoneNumber", StringType(), True),
    StructField("recipient_fields_twitterID", StringType(), True),
    StructField("recipient_first", StringType(), True),
    StructField("recipient_fullName", StringType(), True),
    StructField("recipient_id", LongType(), True),
    StructField("recipient_isPaused", BooleanType(), True),
    StructField("recipient_last", StringType(), True),
    StructField("recipient_object", StringType(), True),

    StructField("campaign_id", LongType(), True),
    StructField("campaign_object", StringType(), True),
    StructField("campaign_title", StringType(), True),
    StructField("campaign_wizardStatus", IntegerType(), True),

    StructField("parent_id", LongType(), True),
    StructField("parent_message_id", LongType(), True),
    StructField("parent_message_object", StringType(), True),
    StructField("parent_message_replyToID", IntegerType(), True),
    StructField("parent_message_subject", StringType(), True),
    StructField("parent_message_type", StringType(), True),
    StructField("parent_object", StringType(), True),
    StructField("parent_type", StringType(), True),
])

CREATED_LEADS_SCHEMA = StructType([

    # --------------------
    # Core
    # --------------------
    StructField("object", StringType(), True),
    StructField("id", LongType(), True),
    StructField("created", StringType(), True),
    StructField("openeddate", StringType(), True),
    StructField("laststatuschangedate", StringType(), True),
    StructField("annotation", StringType(), True),
    StructField("status", StringType(), True),

    # --------------------
    # Recipient
    # --------------------
    StructField("recipient_object", StringType(), True),
    StructField("recipient_id", LongType(), True),
    StructField("recipient_emailaddress", StringType(), True),
    StructField("recipient_fullname", StringType(), True),
    StructField("recipient_created", StringType(), True),
    StructField("recipient_ispaused", BooleanType(), True),
    StructField("recipient_contactid", LongType(), True),
    StructField("recipient_first", StringType(), True),
    StructField("recipient_last", StringType(), True),

    # --------------------
    # Recipient fields
    # --------------------
    StructField("recipient_fields_link", StringType(), True),
    StructField("recipient_fields_first", StringType(), True),
    StructField("recipient_fields_status", StringType(), True),
    StructField("recipient_fields_position", StringType(), True),
    StructField("recipient_fields_date_applied", StringType(), True),
    StructField("recipient_fields_account", StringType(), True),
    StructField("recipient_fields_phonenumber", StringType(), True),
    StructField("recipient_fields_facebookurl", StringType(), True),
    StructField("recipient_fields_instagramid", StringType(), True),
    StructField("recipient_fields_linkedinurl", StringType(), True),
    StructField("recipient_fields_twitterid", StringType(), True),

    # --------------------
    # Campaign
    # --------------------
    StructField("campaign_object", StringType(), True),
    StructField("campaign_id", LongType(), True),
    StructField("campaign_title", StringType(), True),
    StructField("campaign_wizardstatus", IntegerType(), True),

    # --------------------
    # Assigned To
    # --------------------
    StructField("assignedto_object", StringType(), True),
    StructField("assignedto_id", LongType(), True),
    StructField("assignedto_emailaddress", StringType(), True),
    StructField("assignedto_fullname", StringType(), True),
    StructField("assignedto_first", StringType(), True),
    StructField("assignedto_last", StringType(), True),
])

#Implement schema enforcement to avoid schema drift 
# ============================================================================
# IMPORTS
# ============================================================================
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, max, lit, current_timestamp, current_date, explode_outer
)
from pyspark.sql.types import NullType, StringType, DoubleType, StructType, ArrayType,LongType
from datetime import datetime, timedelta
import os, re, json, boto3
from typing import Dict
from datetime import datetime, date

# ============================================================================
# CONFIG
# ============================================================================
RAW_PATH = "s3a://mailshake-analytics/raw"
CLEAN_PATH = "s3a://mailshake-analytics/clean"
clean_base_path = "s3a://mailshake-analytics/clean"
raw_base_path = "s3a://mailshake-analytics/raw"
BUCKET = "mailshake-analytics"
TEAMS_KEY = "config/teams_test1_sample.json"
RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")
SINGLE_DATE = None        # None for incremental activities
# ============================================================================
# SPARK SESSION
# ============================================================================
spark = (
    SparkSession.builder
    .appName("MailshakeCampaignCurations")
    .config(
        "spark.driver.extraClassPath",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    )
    .config(
        "spark.executor.extraClassPath",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    )
    .getOrCreate()
)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ============================================================================
# S3 CONFIG
# ============================================================================
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
hadoop_conf.set("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# ============================================================================
# team LOADING
# ============================================================================
s3 = boto3.client("s3")


def load_teams() -> Dict[str, Dict[str, str]]:
    obj = s3.get_object(Bucket=BUCKET, Key=TEAMS_KEY)
    return json.loads(obj["Body"].read().decode("utf-8")).get("teams", {})

# Load raw teams first
teams_dict = load_teams()

# Transform keys to use semantic team_id
new_teams = {}
for k, v in teams_dict.items():
    new_key = v['team_id']
    new_teams[new_key] = v

teams_dict = new_teams

TEAM_IDS = list(teams_dict.keys())
print("Loaded teams:", TEAM_IDS)


# ============================================================================
# HELPERS
# ============================================================================
def sanitize_column_names(df):
    for col_name in df.columns:
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
        clean = re.sub(r'_+', '_', clean).lower()
        if clean != col_name:
            df = df.withColumnRenamed(col_name, clean)
    return df

def fix_void_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, NullType):
            df = df.withColumn(field.name, col(field.name).cast(StringType()))
    return df


def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    # Normalize df columns
    df = df.select([col(c).alias(c.strip().lower()) for c in df.columns])

    expected_cols = [f.name.strip().lower() for f in schema.fields]
    expected_types = {f.name.strip().lower(): f.dataType for f in schema.fields}

    # Add missing columns
    for c, t in expected_types.items():
        if c not in df.columns:
            print(f"⚠️ Adding missing column: {c} ({t})")
            df = df.withColumn(c, lit(None).cast(t))

    # Cast existing columns
    for c, t in expected_types.items():
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(t))

    # Drop extra columns
    extra_cols = set(df.columns) - set(expected_cols)
    if extra_cols:
        print(f"⚠️ Dropping extra columns: {extra_cols}")
        df = df.drop(*extra_cols)

    # Reorder
    return df.select(expected_cols)


def flatten_struct_columns(df):
    while True:
        struct_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StructType)]
        if not struct_cols: break
        for col_name in struct_cols:
            for nested in df.schema[col_name].dataType.fields:
                df = df.withColumn(f"{col_name}_{nested.name}", col(f"{col_name}.{nested.name}"))
            df = df.drop(col_name)

    array_struct_cols = [
        f.name for f in df.schema.fields
        if isinstance(f.dataType, ArrayType) and isinstance(f.dataType.elementType, StructType)
    ]
    for col_name in array_struct_cols:
        df = df.withColumn(col_name, explode_outer(col(col_name)))
        for nested in df.schema[col_name].dataType.fields:
            df = df.withColumn(f"{col_name}_{nested.name}", col(f"{col_name}.{nested.name}"))
        df = df.drop(col_name)
    return df


def get_dates_to_process(clean_path, raw_base_path, dataset_name, team_ids, single_date=None):
    """
    Returns dict:
    - team_id -> list of incremental event_dates that ACTUALLY exist in raw
    - Empty list => snapshot
    """
    s3_team = boto3.client("s3")
    bucket = "mailshake-analytics"

    # Manual override
    if single_date:
        return {c: [single_date] for c in team_ids}

    # Campaigns never use incremental
    if dataset_name.startswith("campaign"):
        return {c: [] for c in team_ids}

    # --- Read clean to get last processed date ---
    try:
        existing = spark.read.parquet(clean_path)
        last_dates = (
            existing.groupBy("team_id")
            .agg(max("source_date").alias("last_date"))
            .collect()
        )
        last_map = {str(r["team_id"]): r["last_date"] for r in last_dates}
        print("Loaded last_dates from clean:")
        for k, v in last_map.items():
            print(f"  {k}: {v}")
    except Exception:
        last_map = {}

    dates = {}

    for team in team_ids:
        # No clean → snapshot
        if team not in last_map:
            dates[team] = []
            continue

        last_date = datetime.strptime(str(last_map[team]), "%Y-%m-%d").date()

        # List S3 folders for this team & dataset
        prefix = f"raw/team_id={team}/entity={dataset_name}/"
        
        incremental_dates = []

        try:
            paginator = s3_team.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')

            for page in pages:
                for cp in page.get("CommonPrefixes", []):
                    folder_name = cp.get("Prefix").rstrip('/').split('/')[-1]  # e.g., event_date=2026-01-03
                    if folder_name.startswith("event_date="):
                        d_str = folder_name.split("=")[1]
                        d_dt = datetime.strptime(d_str, "%Y-%m-%d").date()
                        print(f"Found S3 folder: {d_dt}")
                        if d_dt > last_date:
                            incremental_dates.append(d_str)

        except Exception as e:
            print(f"⚠️ Could not list raw path {prefix}: {e}")
            incremental_dates = []

        dates[team] = sorted(incremental_dates)
        print(f"{team} last_date={last_date}, incremental_dates={dates[team]}")

    return dates
def process_dataset(
    raw_base_path: str,
    clean_base_path: str,
    team_ids: list,
    dataset_name: str,
    unique_keys: list,
    schema: StructType,
    explode_col: str = None,
    dates_per_team: dict = None
):
    """
    Generic dataset processor:
    - Handles snapshots and incremental loads
    - Flattens nested structs and arrays
    - Sanitizes column names
    - Enforces schema: adds missing columns, casts types, drops extras, reorders
    - Fixes NullType columns
    - Deduplicates based on unique keys
    """

    entity_path = f"{clean_base_path}/entity={dataset_name}"

    for team_id in team_ids:

        # ------------------------------------------------------------------
        # Decide snapshot vs incremental
        # ------------------------------------------------------------------
        clean_team_path = f"{clean_base_path}/entity={dataset_name}/team_id={team_id}"

        if dataset_name.startswith("campaign"):
            snapshot_mode = True
        else:
            try:
                spark.read.parquet(clean_team_path)
                snapshot_mode = False   # clean exists → incremental
            except Exception:
                snapshot_mode = True    # first run → snapshot

        # ------------------------------------------------------------------
        # Determine paths to process
        # ------------------------------------------------------------------
        paths_to_process = []

        if snapshot_mode:
            paths_to_process.append("snapshot")
        else:
            incremental_dates = (dates_per_team or {}).get(team_id, [])
            if not incremental_dates:
                print(f"⚠️ No incremental dates for {dataset_name} | {team_id}, skipping.")
                continue

            for d in incremental_dates:
                paths_to_process.append(f"event_date={d}")

        # ------------------------------------------------------------------
        # Process each path
        # ------------------------------------------------------------------
        for p in paths_to_process:
            input_path = f"{raw_base_path}/team_id={team_id}/entity={dataset_name}/{p}/"

            try:
                print(f"📂 Processing {dataset_name} | {team_id} | {p}")
                df = spark.read.parquet(input_path)
              
               # 1️⃣ Flatten structs & explode arrays FIRST
                df = flatten_struct_columns(df)
                
        
                # 2️⃣ Sanitize column names ONCE (after flattening)
                df = sanitize_column_names(df)
                
                # 3️⃣ Fix NullType columns
                df = fix_void_columns(df)
                
                # 4️⃣ Enforce schema (last, always)
                df = enforce_schema(df, schema)

                # -------------------- Source date logic --------------------
                if p.startswith("event_date="):
                    source_date_val = p.split("=")[1]   # incremental
                else:
                    # snapshot → derive from data
                    if "actiondate" in df.columns:
                        source_date_val = df.selectExpr("date(actiondate) as d").agg({"d": "max"}).collect()[0][0]
                    elif "created" in df.columns:
                        source_date_val = df.selectExpr("date(created) as d").agg({"d": "max"}).collect()[0][0]
                    else:
                        source_date_val = RUN_DATE
                source_date_val = str(source_date_val)

                # -------------------- Metadata --------------------
                df = (
                    df.withColumn("team_id", lit(team_id))
                      .withColumn("source_date", lit(source_date_val))
                      .withColumn("team_id_col", lit(team_id))
                      .withColumn("source_date_col", lit(source_date_val))
                      .withColumn("processing_timestamp", current_timestamp())
                      .withColumn("processing_date", current_date())
                      .withColumn("load_type", lit("snapshot" if snapshot_mode else "incremental"))
                )

                # -------------------- Deduplication --------------------
                safe_keys = [k.replace(".", "_") for k in unique_keys]
                df = df.dropDuplicates(safe_keys + ["team_id", "source_date"])

                # -------------------- Write --------------------
                write_mode = "overwrite" if snapshot_mode else "append"
                df.write.mode(write_mode).partitionBy("team_id","source_date").parquet(entity_path)

                print(f"✅ Written {df.count()} records for {dataset_name} | {team_id} | {p}")

            except Exception as e:
                print(f"⚠️ Skipped {dataset_name} | {team_id} | {p}: {e}")


# ============================================================================
# RUN
# ============================================================================

# # -------------------- campaign --------------------
process_dataset(
    RAW_PATH,
    CLEAN_PATH,
    TEAM_IDS,
    "campaign",
    unique_keys=["id", "messages_id"],
    schema=CAMPAIGN_SCHEMA,
    dates_per_team=None
)

# -------------------- activity_open --------------------
dates_per_team = get_dates_to_process(
    clean_path=f"{CLEAN_PATH}/entity=activity_open",
    raw_base_path=RAW_PATH,
    dataset_name="activity_open",
    team_ids=TEAM_IDS,
    single_date=None
)


process_dataset(
    RAW_PATH,
    CLEAN_PATH,
    TEAM_IDS,
    "activity_open",
    unique_keys=["id", "recipient.id", "campaign.id"],
    schema=ACTIVITY_OPEN_SCHEMA,
    dates_per_team=dates_per_team
)

# -------------------- activity_reply --------------------
dates_per_team = get_dates_to_process(
    clean_path=f"{CLEAN_PATH}/entity=activity_reply",
    raw_base_path=RAW_PATH,
    dataset_name="activity_reply",
    team_ids=TEAM_IDS,
    single_date=None
)


process_dataset(
    RAW_PATH,
    CLEAN_PATH,
    TEAM_IDS,
    "activity_reply",
    unique_keys=["id", "recipient.id", "campaign.id"],
    schema=ACTIVITY_REPLY_SCHEMA,
    dates_per_team=dates_per_team
)

# -------------------- activity_sent --------------------
dates_per_team = get_dates_to_process(
    clean_path=f"{CLEAN_PATH}/entity=activity_sent",
    raw_base_path=RAW_PATH,
    dataset_name="activity_sent",
    team_ids=TEAM_IDS,
    single_date=None
)


process_dataset(
    RAW_PATH,
    CLEAN_PATH,
    TEAM_IDS,
    "activity_sent",
    unique_keys=["id", "recipient.id", "campaign.id"],
    schema=ACTIVITY_SENT_SCHEMA,
    dates_per_team=dates_per_team
)

# -------------------- activity_clicks --------------------
dates_per_team = get_dates_to_process(
    clean_path=f"{CLEAN_PATH}/entity=activity_clicks",
    raw_base_path=RAW_PATH,
    dataset_name="activity_clicks",
    team_ids=TEAM_IDS,
    single_date=None
)


process_dataset(
    RAW_PATH,
    CLEAN_PATH,
    TEAM_IDS,
    "activity_clicks",
    unique_keys=["id", "recipient.id", "campaign.id"],
    schema=ACTIVITY_CLICKS_SCHEMA,
    dates_per_team=dates_per_team
)
# # -------------------- created_leads --------------------
dates_per_team = get_dates_to_process(
    clean_path=f"{CLEAN_PATH}/entity=created_leads",
    raw_base_path=RAW_PATH,
    dataset_name="created_leads",
    team_ids=TEAM_IDS,
    single_date=None
)


process_dataset(
    RAW_PATH,
    CLEAN_PATH,
    TEAM_IDS,
    "created_leads",
    unique_keys=["id", "recipient.id", "campaign.id"],
    schema=CREATED_LEADS_SCHEMA,
    dates_per_team=dates_per_team
)
spark.stop()
print("🎉 All datasets processed successfully!")