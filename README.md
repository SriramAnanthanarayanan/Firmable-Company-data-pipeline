# **Firmable ETL Pipeline**

## **1\. Overview**

The Firmable ETL Pipeline extracts, transforms, and loads Australian company data from two sources—Common Crawl and the Australian Business Register (ABR)—into a PostgreSQL database. The pipeline performs entity matching to create a unified view of companies in Australia.

## **2\. Project Structure**

firmable-etl-pipeline/  
├── extract/  
│   ├── commoncrawl\_scraper.py   \# Extract company info from Common Crawl WARC files  
│   └── abr\_parser.py            \# Parse ABR XML files  
├── transform/  
│   ├── data\_cleaning.py         \# Standardize and clean extracted data  
│   └── entity\_matcher.py        \# Match companies using rule based, fuzzy matching and  LLM approach  
├── db/  
│   └── create\_schema.sql        \# PostgreSQL schema creation script  
├── run\_pipeline.py              \# Orchestrates the full ETL pipeline  
├── requirements.txt             \# Python dependencies  
├── pyproject.toml               \# UV package configuration  
└── README.md                    \# Project documentation

## **3\. Pipeline Architecture and Design**

### **Simplified Prototype**

Common Crawl Index → WARC Files → HTML Extraction → Company Name , Domain, phone , abn, postcode , email , title  
                         ↓  
ABR XML → XML Parsing → ABN, Entity Info  
                         ↓  
Data Cleaning & Normalization  
                         ↓  
Entity Matching (Rule based/ Fuzzy / LLM)  
                         ↓  
PostgreSQL Unified Table

The code in this repository implements a local prototype that runs as a series of sequential Python scripts:

1. **Extract (ABR):** abr\_parser.py parses local ABR XML files, processes them in batches, and loads the raw data into the stg.abr\_raw\_companies table in PostgreSQL.  
2. **Extract (Common Crawl):** commoncrawl\_scraper.py queries the Common Crawl index, fetches corresponding WARC files, scrapes HTML for company details, and loads the raw data into stg.common\_crawl\_raw\_companies.  
3. **Transform (Cleaning):** data\_cleaning.py reads from the stg tables, cleans/standardizes data (e.g., state names, postcodes, ABNs), and saves the cleaned, deduplicated results into the pre\_dwh schema.  
4. **Transform (Entity Matching):** entity\_matching.py executes a multi-stage matching process (rule-based, fuzzy, and optional LLM) to link records from the Common Crawl and ABR sources.  
5. **Load:** The final matched and unified data is loaded into the dwh.dim\_entity\_match\_company\_data table for consumption.

### **Production-Grade Architecture**

For a production-grade application, I prefer the below approach, tech stack, or system to build a scalable ETL pipeline to extract company data and serve it downstream to empower analytics.

The target architecture, illustrated in the diagram below, is designed for scalability, observability, and robustness using a modern cloud stack (AWS).

**Description:**

* **Orchestration:** Airflow (MWAA) triggers and manages the entire pipeline.  
* **Ingestion:** An ECS/Fargate task (Ingestion) fetches raw data (ABR XML, Common Crawl paths) and stores it in an S3 "Raw Zone" bucket.  
* **Processing (Cleaning & Matching):** A second ECS/Fargate task (Cleaning & Matching) is triggered. It reads raw data from S3, performs the cleaning and entity matching logic, and writes the curated data (e.g., in Parquet format) to an S3 "Curated Zone".  
* **Processing (dbt):** A third ECS/Fargate task runs dbt models. It reads the curated data from S3, applies final transformations, runs data quality tests, and loads the data into the Snowflake Data Warehouse.  
* **Data Warehouse:** Snowflake serves as the scalable cloud data warehouse for the final star-schema models.  
* **Consumption:** BI Tools (like Tableau or Power BI) and other data consumers query Snowflake.  
* **Observability:** CloudWatch and Datadog collect logs (Airflow, ECS, Snowflake) and metrics (query performance, container stats) for monitoring and alerting.

## **4\. Database Schema (PostgreSQL DDL)**

The database schema is structured into three layers for data lineage and quality: stg (Staging for raw data), pre\_dwh (Pre-Data Warehouse for cleaned data), and dwh (Data Warehouse for the final unified view).

Refer to db/ddl\_Scripts.sql for more information about the schema.

## **5\. AI Model Used & Rationale**

* **Model:** OpenAI's GPT-4  
* **Rationale:** The LLM's strength is semantic understanding and contextual reasoning, which surpasses traditional algorithms for difficult entity resolution. For instance, it can determine that "ACME Pty Ltd" at "123 Main St" is the same entity as "ACME Corporation" at "123 Main St, Sydney" even if string similarity is low, by reasoning across the entire set of address, name, and domain data points.

**Prompt Used:**

"""  
Determine if the two company names refer to the same Australian business entity.  
 1\. Crawl: "{row.company\_name}"  
 2\. ABR: "{cand\_name}"  
 Answer only 'Yes' or 'No'.  
"""

## **6\. ETL Pipeline Implementation**

The pipeline logic is implemented as a set of Python scripts:

* **abr\_parser.py:** Extracts ABR data. Uses lxml.etree.iterparse to stream-process large XML files and psycopg2.extras.execute\_values to bulk-load data into stg.abr\_raw\_companies.  
* **commoncrawl\_scraper.py:** Extracts Common Crawl data. Queries the CC index, fetches WARC files, parses HTML with BeautifulSoup, and loads data into stg.common\_crawl\_raw\_companies.  
* **data\_cleaning.py:** Transforms the data. Reads from stg tables into pandas, performs standardization (states, postcodes), cleaning (company names), and deduplication, then loads the results into pre\_dwh tables.  
* **entity\_matching.py:** Transforms and Loads the final model. It reads from the pre\_dwh tables and performs a multi-stage entity matching process to link Common Crawl records to ABR records. The final matched dataset is loaded into dwh.dim\_entity\_match\_company\_data.

## **7\. Transformations & Data Quality**

### **dbt Models & Tests**

This prototype does not currently use dbt. Transformations are handled directly in Python scripts (data\_cleaning.py, entity\_matching.py).

In a production environment, this logic would be refactored into dbt models:

* **Staging Models:** stg\_abr\_companies and stg\_common\_crawl\_companies would be created as views on the raw tables, performing light cleaning and renaming.  
* **Intermediate Models:** int\_companies\_cleaned would handle the logic from data\_cleaning.py.  
* **Mart Models:** dim\_companies would be the final model, containing the entity matching logic from entity\_matching.py.

Data quality checks are currently implicit (e.g., ABN validation in commoncrawl\_scraper.py). dbt would formalize these using dbt tests (e.g., unique, not\_null on ABNs, accepted\_values for states).

### **Entity Matching Approach**

The entity matching design in entity\_matching.py is a multi-stage cascade:

1. **Rule-Based Match:** First, it performs a high-confidence match directly in SQL, joining records where TRIM(cc.abn) \= TRIM(abr.abn). This is the fastest and most reliable match.  
2. **Fuzzy Match:** For remaining Common Crawl records, it attempts a fuzzy string match. To optimize, it blocks records by postcode (only comparing records within the same postcode) and then uses rapidfuzz.fuzz.token\_sort\_ratio to compare company names.  
3. **LLM Match (Optional):** For records that still don't match, an optional step can be enabled to send the candidate data to OpenAI's GPT-4. The LLM is asked to act as an expert and determine if the two entities are the same, handling complex cases that fuzzy logic would miss.