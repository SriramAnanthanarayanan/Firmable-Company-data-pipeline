import pandas as pd
import random
import re
from fuzzywuzzy import fuzz

# Sample ABR data
abr_data = pd.DataFrame([
    {"abn": "11000000948", "entity_name": "QBE INSURANCE (INTERNATIONAL) LTD", "entity_type": "Australian Public Company", "state": "NSW", "postcode": "2000"},
    {"abn": "11000002568", "entity_name": "TOOHEYS PTY LIMITED", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2141"},
    {"abn": "11000003314", "entity_name": "NEWCASTLE GOLF CLUB LTD", "entity_type": "Australian Public Company", "state": "NSW", "postcode": "2295"},
    {"abn": "11000003378", "entity_name": "BBC HARDWARE LIMITED", "entity_type": "Australian Public Company", "state": "VIC", "postcode": "3121"},
    {"abn": "11000007876", "entity_name": "AMPOL PETROLEUM PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2015"},
    {"abn": "11000009496", "entity_name": "PIONEER STEEL PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2000"},
    {"abn": "11000013098", "entity_name": "SYDNEY NIGHT PATROL & INQUIRY CO PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2113"},
    {"abn": "11000015976", "entity_name": "MIRROR NEWSPAPERS PTY LIMITED", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2010"},
    {"abn": "11000016722", "entity_name": "INSURANCE AUSTRALIA LIMITED", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2000"},
    {"abn": "11000017596", "entity_name": "BJELKE-PETERSEN BROS PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2114"},
    {"abn": "11000018342", "entity_name": "A.C.N. 000 018 342 PTY LIMITED", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2322"},
    {"abn": "11000025696", "entity_name": "H.F. LAMPE INVESTMENTS PTY.", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2800"},
    {"abn": "11000032922", "entity_name": "HD BUILDING (NSW) PTY LIMITED", "entity_type": "Australian Public Company", "state": "NSW", "postcode": "2000"},
    {"abn": "11000037409", "entity_name": "FOREST COACH LINES PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2084"},
    {"abn": "11000042642", "entity_name": "DABEE PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2843"},
    {"abn": "11000044262", "entity_name": "PLATYPUS LEATHER INDUSTRIES PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2019"},
    {"abn": "11000045509", "entity_name": "J & M MFG PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2000"},
    {"abn": "11000047129", "entity_name": "DAVID DONALDSON PTY LTD", "entity_type": "Australian Private Company", "state": "NSW", "postcode": "2000"},
    {"abn": "11000047950", "entity_name": "SYDNEY MISSIONARY & BIBLE COLLEGE", "entity_type": "Australian Public Company", "state": "NSW", "postcode": "2132"}
])

# Function to generate variant names
def generate_variant(name, i):
    # Fuzzy-like variant
    fuzzy_name = name.replace("&", "and")
    fuzzy_name = re.sub(r"[^\w\s]", "", fuzzy_name)
    fuzzy_name = fuzzy_name.title() if random.random() < 0.5 else fuzzy_name.upper()
    
    # LLM-like variant (simplified abbreviation)
    words = name.split()
    if len(words) > 1:
        llm_name = f"{words[0]} {words[-1]}"
    else:
        llm_name = name
    return fuzzy_name, llm_name

# Build synthetic DataFrame
rows = []
for i, row in abr_data.iterrows():
    # Exact copy → rule-based
    rows.append({
        "abn": row["abn"],
        "company_name": row["entity_name"],
        "state": row["state"],
        "postcode": row["postcode"],
        "domain": f"test{i}.com",
        "variant_company_name": row["entity_name"],
        "match_with_abr_type": "rule_based",
        "score": 100,
        "confidence_type": "high"
    })
    
    fuzzy_name, llm_name = generate_variant(row["entity_name"], i)
    
    # Fuzzy variant
    rows.append({
        "abn": row["abn"],
        "company_name": row["entity_name"],
        "state": row["state"],
        "postcode": row["postcode"],
        "domain": f"26dh7{i}.com",
        "variant_company_name": fuzzy_name,
        "match_with_abr_type": "fuzzy",
        "score": fuzz.ratio(row["entity_name"], fuzzy_name),
        "confidence_type": "high"
    })
    
    # LLM variant
    rows.append({
        "abn": row["abn"],
        "company_name": row["entity_name"],
        "state": row["state"],
        "postcode": row["postcode"],
        "domain": f"piper233{i}.com",
        "variant_company_name": llm_name,
        "match_with_abr_type": "llm",
        "score": fuzz.ratio(row["entity_name"], llm_name),
        "confidence_type": "medium"
    })

synthetic_df = pd.DataFrame(rows)

# Display all columns fully
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
print(synthetic_df)
