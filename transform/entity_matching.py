import os
import psycopg2
import pandas as pd
from rapidfuzz import process, fuzz
from openai import OpenAI
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# ---------------- Load environment ---------------- #
load_dotenv(dotenv_path="C:/Users/Admin/Desktop/firmable-etl-pipeline/.venv/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# ---------------- DB Helpers ---------------- #
def store_matches_to_db(matches_df: pd.DataFrame):
    if matches_df.empty:
        print("No matches to store.")
        return

    matches_df = matches_df.dropna(subset=["crawl_company_name", "abr_company_name"], how="all")
    if matches_df.empty:
        print("No valid matches to store (all null).")
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Drop and recreate table
    cursor.execute("DROP TABLE IF EXISTS prd_firmable.dwh.dim_entity_match_company_data;")
    cursor.execute("""
        CREATE TABLE prd_firmable.dwh.dim_entity_match_company_data (
            crawl_domain TEXT,
            crawl_company_name TEXT,
            crawl_abn CHAR(20),
            abr_abn VARCHAR(20),
            abr_company_name TEXT,
            abr_entity_type TEXT,
            abr_state TEXT,
            abr_postcode VARCHAR(20),
            match_method TEXT,
            match_score NUMERIC,
            match_confidence TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            creation_dt TIMESTAMP DEFAULT NOW()
        );
    """)

    expected_columns = [
        "crawl_domain", "crawl_company_name", "crawl_abn",
        "abr_abn", "abr_company_name", "abr_entity_type",
        "abr_state", "abr_postcode", "match_method",
        "match_score", "match_confidence"
    ]

    for col in expected_columns:
        if col not in matches_df.columns:
            matches_df[col] = None

    matches_df["creation_dt"] = pd.Timestamp.now()

    execute_values(
        cursor,
        f"""
        INSERT INTO prd_firmable.dwh.dim_entity_match_company_data (
            {', '.join(expected_columns)}, creation_dt
        ) VALUES %s
        """,
        matches_df[expected_columns + ["creation_dt"]].values.tolist()
    )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ {len(matches_df)} matched records saved to DB.")

# ---------------- Fetch Helpers ---------------- #
def fetch_crawl_data():
    conn = psycopg2.connect(**DB_CONFIG)
    df = pd.read_sql("""
        SELECT domain, company_name, abn, postcode 
        FROM prd_firmable.pre_dwh.cleaned_commoncrawl_companies;
    """, conn)
    conn.close()
    return df

def fetch_abr_chunk(offset=0, limit=50000):
    conn = psycopg2.connect(**DB_CONFIG)
    query = f"""
        SELECT abn, entity_name, entity_type, state, postcode
        FROM prd_firmable.pre_dwh.cleaned_abr_companies
        WHERE postcode IN (
              SELECT DISTINCT postcode
              FROM prd_firmable.pre_dwh.cleaned_commoncrawl_companies
          )
        ORDER BY abn
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ---------------- Matching Functions ---------------- #
def rule_based_match_sql():
    """Fetch rule-based matches directly in SQL."""
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT DISTINCT
            cc.domain AS crawl_domain,
            cc.company_name AS crawl_company_name,
            cc.abn AS crawl_abn,
            abr.abn AS abr_abn,
            abr.entity_name AS abr_company_name,
            abr.entity_type AS abr_entity_type,
            abr.state AS abr_state,
            abr.postcode AS abr_postcode,
            'rule_based_abn' AS match_method,
            100.0 AS match_score,
            'high' AS match_confidence
        FROM prd_firmable.pre_dwh.cleaned_commoncrawl_companies cc
        INNER JOIN prd_firmable.pre_dwh.cleaned_abr_companies abr
        ON TRIM(cc.abn) = TRIM(abr.abn)
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def fuzzy_match(crawl_df, abr_df, threshold=80):
    results = []
    if crawl_df.empty:
        return pd.DataFrame([]), crawl_df

    abr_df["entity_name_lower"] = abr_df["entity_name"].str.lower()
    abr_grouped = abr_df.groupby("postcode")

    for _, crawl_row in crawl_df.iterrows():
        postcode = crawl_row["postcode"]
        if postcode not in abr_grouped.groups:
            continue

        abr_subset = abr_grouped.get_group(postcode)
        match_name, score, abr_idx = process.extractOne(
            crawl_row["company_name"], abr_subset["entity_name"].tolist(), scorer=fuzz.token_sort_ratio
        )

        if score >= threshold:
            abr_row = abr_subset.iloc[abr_idx]
            results.append({
                "crawl_domain": crawl_row["domain"],
                "crawl_company_name": crawl_row["company_name"],
                "crawl_abn": crawl_row["abn"],
                "abr_abn": abr_row["abn"],
                "abr_company_name": abr_row["entity_name"],
                "abr_entity_type": abr_row["entity_type"],
                "abr_state": abr_row["state"],
                "abr_postcode": abr_row["postcode"],
                "match_method": "fuzzy",
                "match_score": score,
                "match_confidence": "high" if score >= 92 else "medium"
            })

    fuzzy_df = pd.DataFrame(results)
    matched_domains = fuzzy_df["crawl_domain"].tolist() if not fuzzy_df.empty else []
    remaining_crawl = crawl_df[~crawl_df["domain"].isin(matched_domains)].copy()
    return fuzzy_df, remaining_crawl

# ---------------- OpenAI LLM Matching ---------------- #
def llm_match(crawl_df, abr_df):
    """LLM-assisted matching using OpenAI GPT."""
    if client is None or crawl_df.empty:
        return pd.DataFrame([]), crawl_df

    results = []

    for _, crawl_row in crawl_df.iterrows():
        # Build prompt for GPT
        prompt = f"""
        You are an expert in Australian business entity resolution. Your task is to determine if web data and official business register data refer to the same company.

        CONTEXT:
        - Common Crawl data is extracted from company websites (may have informal names, abbreviations)
        - ABR data is from Australian Business Register (official legal names, may be formal)
        - Australian companies often trade under different names than their legal registration
        - Consider common variations: "Pty Ltd" vs "Proprietary Limited", abbreviations, "The" prefix

        MATCHING GUIDELINES:
        1. Strong match indicators:
        - ABN found on website matches ABR record exactly (if available)
        - Domain name clearly derives from entity name
        - Address match (same suburb/postcode is strong signal)
        - Trading name listed in ABR matches website name

        2. Weak match indicators:
        - Similar industry only
        - Similar name but different legal structure
        - Geographic proximity only

        3. Non-match indicators:
        - Completely different business activities
        - Different states with no connection
        - Name similarity is coincidental (e.g., "Smith Consulting" is common)

        EXAMPLES:

        Example 1 - MATCH:
        Website: "acmewidgets.com.au", Name: "Acme Widgets", Location: "Sydney NSW"
        ABR: ABN 12-345-678-901, Name: "ACME WIDGETS PTY LTD", Location: "Sydney NSW 2000"
        Reasoning: Domain matches entity name closely, same city. The website uses informal trading name while ABR has formal legal name.
        Decision: MATCH, Confidence: HIGH
        Here are the details..
        
        Company: {crawl_row['company_name']} 
        Postcode: {crawl_row['postcode']}
        ABR options: {abr_df[['entity_name','abn','postcode']].to_dict(orient='records')}
        Return only the best matching ABR record ABN if confident, otherwise return None.
        """

        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            gpt_result = response.choices[0].message.content.strip()
            if gpt_result.lower() != "none":
                # Example output parsing: assume ABN returned
                matched_abn = gpt_result
                abr_row = abr_df[abr_df['abn'] == matched_abn].iloc[0]
                results.append({
                    "crawl_domain": crawl_row["domain"],
                    "crawl_company_name": crawl_row["company_name"],
                    "crawl_abn": crawl_row["abn"],
                    "abr_abn": abr_row["abn"],
                    "abr_company_name": abr_row["entity_name"],
                    "abr_entity_type": abr_row["entity_type"],
                    "abr_state": abr_row["state"],
                    "abr_postcode": abr_row["postcode"],
                    "match_method": "LLM",
                    "match_score": 95.0,
                    "match_confidence": "medium"
                })
        except Exception as e:
            print(f"⚠️ LLM match failed for {crawl_row['company_name']}: {e}")
            continue

    llm_df = pd.DataFrame(results)
    matched_domains = llm_df["crawl_domain"].tolist() if not llm_df.empty else []
    remaining_crawl = crawl_df[~crawl_df["domain"].isin(matched_domains)].copy()
    return llm_df, remaining_crawl

# ---------------- Main Pipeline ---------------- #
def run_entity_matching_chunked(batch_size=50000, enable_llm=False):
    crawl_df = fetch_crawl_data()
    offset = 0
    final_matches = []

    # --- Step 1: Rule-based SQL matches ---
    print("Performing rule-based SQL match...")
    rule_matches = rule_based_match_sql()
    print(f"Rule-based matches found: {len(rule_matches)}")
    if not rule_matches.empty:
        final_matches.append(rule_matches)
        matched_domains = rule_matches["crawl_domain"].tolist()
        crawl_df = crawl_df[~crawl_df["domain"].isin(matched_domains)].copy()

    # --- Step 2: Process in chunks for fuzzy / LLM ---
    while not crawl_df.empty:
        print(f"Fetching ABR chunk offset={offset}")
        abr_chunk = fetch_abr_chunk(offset=offset, limit=batch_size)
        if abr_chunk.empty:
            break

        # Fuzzy match only remaining rows
        print("  Performing fuzzy match...")
        fuzzy_matches, crawl_df = fuzzy_match(crawl_df, abr_chunk)
        if not fuzzy_matches.empty:
            final_matches.append(fuzzy_matches)

        # Optional LLM match
        if enable_llm and not crawl_df.empty:
            print("  Performing LLM match...")
            llm_matches, crawl_df = llm_match(crawl_df, abr_chunk)
            if not llm_matches.empty:
                final_matches.append(llm_matches)

        offset += batch_size

    final_df = pd.concat(final_matches, ignore_index=True) if final_matches else pd.DataFrame([])
    print(f"\n Total Matches: {len(final_df)}")
    store_matches_to_db(final_df)

# ---------------- Entrypoint ---------------- #
if __name__ == "__main__":
    run_entity_matching_chunked(batch_size=50000, enable_llm=False)
