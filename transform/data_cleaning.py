"""
data_cleaning.py — Final Production Version
-------------------------------------------
1. Cleans and standardizes ABR + Common Crawl data.
2. Deduplicates exactly as per SQL DISTINCT logic:
   - ABR: DISTINCT abn, entity_name, state, postcode
   - CC:  DISTINCT abn, company_name, postcode
3. Inserts cleaned data into pre_dwh schema in batches.
"""

import os
import re
import json
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from fuzzywuzzy import process

# ------------------- Load DB config ------------------- #
load_dotenv(dotenv_path="C:/Users/Admin/Desktop/firmable-etl-pipeline/.venv/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# ------------------- State Mapping ------------------- #
STATE_MAPPING = {
    "NSW": "NSW", "NEW SOUTH WALES": "NSW",
    "VIC": "VIC", "VICTORIA": "VIC",
    "QLD": "QLD", "QUEENSLAND": "QLD",
    "SA": "SA", "SOUTH AUSTRALIA": "SA",
    "WA": "WA", "WESTERN AUSTRALIA": "WA",
    "TAS": "TAS", "TASMANIA": "TAS",
    "ACT": "ACT", "AUSTRALIAN CAPITAL TERRITORY": "ACT",
    "NT": "NT", "NORTHERN TERRITORY": "NT"
}

# ------------------- Cleaning Functions ------------------- #
def standardize_state(state: str) -> str:
    """Standardize state names using mapping and fuzzy matching."""
    if not isinstance(state, str):
        return None
    state_clean = re.sub(r'[\.\s]+', ' ', state.strip().upper())
    if state_clean in STATE_MAPPING:
        return STATE_MAPPING[state_clean]
    best_match, score = process.extractOne(state_clean, STATE_MAPPING.keys())
    if score > 85:
        return STATE_MAPPING[best_match]
    return None


def clean_company_name(name: str) -> str:
    if not name:
        return None
    name = re.sub(r'[^A-Za-z0-9 &]', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name.title()


def clean_postcode(postcode: str) -> str:
    if not postcode:
        return None
    postcode_clean = re.sub(r'\D', '', str(postcode))
    return postcode_clean if postcode_clean else None


def clean_abn(abn: str) -> str:
    if not abn:
        return None
    abn_clean = re.sub(r'[\s\-]', '', str(abn))
    return abn_clean if len(abn_clean) == 11 else None


def safe_jsonify(x):
    """Safely convert arrays/dicts to JSON strings for SQL insertion."""
    try:
        if isinstance(x, (dict, list)):
            return json.dumps(x)
        elif isinstance(x, (str, int, float)) or x is None:
            return x
        elif isinstance(x, (pd.Series, pd.DataFrame)):
            return json.dumps(x.to_dict())
        return str(x)
    except Exception:
        return None

# ------------------- Database Utility Functions ------------------- #
def fetch_raw_data(query: str) -> pd.DataFrame:
    """Fetch raw data from PostgreSQL."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print("Error fetching data:", e)
        return pd.DataFrame()


def save_cleaned_data(df: pd.DataFrame, table_name: str, batch_size: int = 500_000):
    """Save cleaned data to PostgreSQL in batches, handling large datasets efficiently."""
    if df.empty:
        print(f"No data to save for {table_name}")
        return

    # Convert JSON / array fields safely
    for col in df.columns:
        df[col] = df[col].apply(safe_jsonify)

    cols = df.columns.tolist()
    col_names = ', '.join(cols)
    total_rows = len(df)
    batches = range(0, total_rows, batch_size)

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # ---- Create table if not exists ---- #
                col_defs = ', '.join([f"{c} TEXT" for c in cols if c.lower() != "created_at"])
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        {col_defs}
                        {',' if col_defs else ''} created_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                conn.commit()

                # ---- Truncate before reloading ---- #
                cur.execute(f"TRUNCATE TABLE {table_name};")
                conn.commit()

                from psycopg2.extras import execute_values

                # ---- Batch Insert ---- #
                for i in batches:
                    batch_df = df.iloc[i:i + batch_size]
                    batch_values = batch_df.values.tolist()

                    execute_values(
                        cur,
                        f"INSERT INTO {table_name} ({col_names}) VALUES %s",
                        batch_values
                    )
                    conn.commit()
                    print(f"✅ Inserted batch {i // batch_size + 1} ({len(batch_df):,} rows)")

                print(f"🎯 Successfully inserted {total_rows:,} records into {table_name}")

    except Exception as e:
        print("❌ Error saving cleaned data:", e)


# ------------------- Main Cleaning Pipeline ------------------- #
if __name__ == "__main__":
    abr_query = "SELECT * FROM prd_firmable.stg.abr_raw_companies;"
    cc_query = "SELECT * FROM prd_firmable.stg.common_crawl_raw_companies;"

    df_abr = fetch_raw_data(abr_query)
    df_cc = fetch_raw_data(cc_query)

    print(f"Raw ABR records: {len(df_abr):,}, Raw CC records: {len(df_cc):,}")

    # --- Clean ABR --- #
    df_abr["entity_name"] = df_abr["entity_name"].apply(clean_company_name)
    df_abr["abn"] = df_abr["abn"].apply(clean_abn)
    df_abr["postcode"] = df_abr["postcode"].apply(clean_postcode)
    df_abr["state"] = df_abr["state"].apply(standardize_state)

    # --- Clean CC --- #
    df_cc["company_name"] = df_cc["company_name"].apply(clean_company_name)
    df_cc["abn"] = df_cc["abn"].apply(clean_abn)
    df_cc["postcode"] = df_cc["postcode"].apply(clean_postcode)

    # --- Deduplication (Exact SQL DISTINCT logic) --- #
    df_abr = df_abr.drop_duplicates(subset=["abn", "entity_name", "state", "postcode"])
    df_cc = df_cc.drop_duplicates(subset=["abn", "company_name", "postcode"])

    print(f"Deduplicated ABR records: {len(df_abr):,}, Deduplicated CC records: {len(df_cc):,}")

    # --- Save Cleaned Outputs --- #
    save_cleaned_data(df_abr, "prd_firmable.pre_dwh.cleaned_abr_companies")
    save_cleaned_data(df_cc, "prd_firmable.pre_dwh.cleaned_commoncrawl_companies")

