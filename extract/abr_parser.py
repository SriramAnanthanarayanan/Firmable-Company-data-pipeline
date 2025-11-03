import os
from lxml import etree
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ------------------- Load DB Config ------------------- #
load_dotenv(dotenv_path="C:/Users/Admin/Desktop/firmable-etl-pipeline/.venv/.env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

TABLE_NAME = "prd_firmable.stg.abr_raw_companies"
FOLDER_PATH = "../data"
BATCH_SIZE = 50000  

# ------------------- PostgreSQL Connection ------------------- #
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()

# Recreate table
create_table_query = f"""
DROP TABLE IF EXISTS {TABLE_NAME};
CREATE TABLE {TABLE_NAME} (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(20),
    entity_name TEXT,
    entity_type TEXT,
    entity_status VARCHAR(50),
    address TEXT,
    postcode VARCHAR(20),
    state VARCHAR(20),
    start_date DATE,
    created_at TIMESTAMP DEFAULT NOW()
);
"""
cursor.execute(create_table_query)
conn.commit()

insert_query = f"""
INSERT INTO {TABLE_NAME} (abn, entity_name, entity_type, entity_status, address, postcode, state, start_date)
VALUES %s
"""

# ------------------- Helper Function ------------------- #
def extract_abr_data(abr):
    abn_elem = abr.find("ABN")
    abn = abn_elem.text if abn_elem is not None else None
    entity_status = abn_elem.get("status") if abn_elem is not None else None
    start_date = abn_elem.get("ABNStatusFromDate") if abn_elem is not None else None

    entity_type_elem = abr.find("EntityType/EntityTypeText")
    entity_type = entity_type_elem.text if entity_type_elem is not None else None

    main_name_elem = abr.find("MainEntity/NonIndividualName/NonIndividualNameText")
    entity_name = main_name_elem.text if main_name_elem is not None else None

    address_elem = abr.find("MainEntity/BusinessAddress/AddressDetails")
    state = address_elem.find("State").text if address_elem is not None and address_elem.find("State") is not None else None
    postcode = address_elem.find("Postcode").text if address_elem is not None and address_elem.find("Postcode") is not None else None
    address = f"{state} {postcode}" if state and postcode else None

    return (abn, entity_name, entity_type, entity_status, address, postcode, state, start_date)

# ------------------- Parse XML Files in Batches ------------------- #
xml_files = [os.path.join(FOLDER_PATH, f) for f in os.listdir(FOLDER_PATH) if f.endswith('.xml')]
batch = []
total_inserted = 0

for file in xml_files:
    print(f"Processing file: {file}")
    for _, abr in etree.iterparse(file, tag="ABR"):
        row = extract_abr_data(abr)
        batch.append(row)
        abr.clear()  # free memory

        if len(batch) >= BATCH_SIZE:
            execute_values(cursor, insert_query, batch)
            conn.commit()
            total_inserted += len(batch)
            print(f"Inserted batch of {len(batch)} rows. Total inserted: {total_inserted}")
            batch = []

# Insert remaining rows
if batch:
    execute_values(cursor, insert_query, batch)
    conn.commit()
    total_inserted += len(batch)
    print(f"Inserted final batch of {len(batch)} rows. Total inserted: {total_inserted}")

# ------------------- Close Connection ------------------- #
cursor.close()
conn.close()
print("ETL completed successfully!")
