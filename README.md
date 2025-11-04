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

## **3\. Setup and Running (using uv)**

This guide provides setup and execution steps using the uv package manager.

### **Step 1: Prerequisites**

* **Install uv:** If you don't have uv, install it using pip, brew, or curl:  
  \# On macOS / Linux  
  curl \-LsSf \[https://astral.sh/uv/install.sh\](https://astral.sh/uv/install.sh) | sh

  \# On Windows (PowerShell)  
  irm \[https://astral.sh/uv/install-powershell.ps1\](https://astral.sh/uv/install-powershell.ps1) | iex

* **PostgreSQL:** A running PostgreSQL instance that you can connect to.  
* **ABR Data:** Your ABR XML data files downloaded to a local directory.

### **Step 2: Code & Environment Setup**

1. **Clone the Repository:**  
   git clone \<your-repository-url\>  
   cd firmable-etl-pipeline

2. Create .env File:  
   This project uses a .env file for database and API credentials. Create a file named .env in the project's root directory:  
   DB\_HOST=localhost  
   DB\_PORT=5432  
   DB\_NAME=prd\_firmable  
   DB\_USER=your\_postgres\_user  
   DB\_PASSWORD=your\_postgres\_password  
   OPENAI\_API\_KEY=sk-your-openai-key-here

3. Initialize Virtual Environment:  
   uv will create a .venv directory and manage dependencies from pyproject.toml or requirements.txt.  
   \# This creates a virtual environment named .venv  
   uv venv

4. **Activate Environment:**  
   \# On macOS / Linux  
   source .venv/bin/activate

   \# On Windows (PowerShell)  
   .venv\\Scripts\\Activate.ps1

5. Install Dependencies:  
   Use uv to install all required packages from requirements.txt.  
   uv pip install \-r requirements.txt

### **Step 3: Database Setup**

1. Connect to PostgreSQL:  
   Use psql or a tool like pgadmin4 to connect to your instance.  
   psql \-U your\_postgres\_user \-d postgres

2. **Create Database & User (if not done):**  
   CREATE DATABASE prd\_firmable;  
   CREATE USER etl\_user WITH PASSWORD 'your\_postgres\_password';  
   GRANT ALL PRIVILEGES ON DATABASE prd\_firmable TO etl\_user;

   *(Note: Ensure the DB\_USER in your .env matches this user.)*  
3. Run the DDL Script:  
   Execute the DDL script to create all schemas and tables.  
   \# Make sure your .env user (etl\_user) has privileges  
   psql \-U etl\_user \-d prd\_firmable \-f db/ddl\_scripts.sql 

### **Step 4: Run the ETL Pipeline (In Order)**

You must run the scripts sequentially as they depend on each other.

1. **Run abr\_parser.py (Extract ABR):**  
   * **Action:** Parses local ABR XML files and loads them into stg.abr\_raw\_companies.  
   * **Before running:** Update the FOLDER\_PATH \= "../data" variable in abr\_parser.py to point to the directory containing your XML files.  
   * **Run:**  
     uv run python extract/abr\_parser.py

2. **Run commoncrawl\_scraper.py (Extract Common Crawl):**  
   * **Action:** Scrapes the Common Crawl index and loads data into stg.common\_crawl\_raw\_companies. This may take a long time.  
   * **Run:**  
     uv run python extract/commoncrawl\_scraper.py

3. **Run data\_cleaning.py (Transform \- Clean):**  
   * **Action:** Reads from stg tables, cleans/standardizes data, and saves it to the pre\_dwh schema.  
   * **Run:**  
     uv run python transform/data\_cleaning.py

4. **Run entity\_matching.py (Transform \- Match):**  
   * **Action:** Reads from pre\_dwh, performs the matching logic, and loads the final unified dataset into dwh.dim\_entity\_match\_company\_data.  
   * **Note:** You can set enable\_llm=False in the script's if \_\_name\_\_ \== "\_\_main\_\_": block to avoid running LLM calls for testing. If you have an API key, you can enable it.  
   * **Run:**  
     uv run python transform/entity\_matching.py

After these steps, your dwh.dim\_entity\_match\_company\_data table will be populated and ready for analysis.

## **4\. Pipeline Architecture and Design**

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

<img width="1883" height="847" alt="image" src="https://github.com/user-attachments/assets/e921a479-05e9-4184-b01f-d3f19ef54ac4" />


**Description:**

* **Orchestration:** Airflow (MWAA) triggers and manages the entire pipeline.  
* **Ingestion:** An ECS/Fargate task (Ingestion) fetches raw data (ABR XML, Common Crawl paths) and stores it in an S3 "Raw Zone" bucket.  
* **Processing (Cleaning & Matching):** A second ECS/Fargate task (Cleaning & Matching) is triggered. It reads raw data from S3, performs the cleaning and entity matching logic, and writes the curated data (e.g., in Parquet format) to an S3 "Curated Zone".  
* **Processing (dbt):** A third ECS/Fargate task runs dbt models. It reads the curated data from S3, applies final transformations, runs data quality tests, and loads the data into the Snowflake Data Warehouse.  
* **Data Warehouse:** Snowflake serves as the scalable cloud data warehouse for the final star-schema models.  
* **Consumption:** BI Tools (like Tableau or Power BI) and other data consumers query Snowflake.  
* **Observability:** CloudWatch and Datadog collect logs (Airflow, ECS, Snowflake) and metrics (query performance, container stats) for monitoring and alerting.

### **Component Justification**

| Component | Technology | Rationale (Why this choice?) |
| :---- | :---- | :---- |
| **Orchestration** | **Airflow (AWS MWAA)** | **Why:** Manages complex dependencies as code (DAGs). Provides robust retry/backfill logic, task-level failure alerts, and a UI for visualizing runs. This is non-trivial to build and essential for a resilient pipeline. **Why MWAA?** It's the managed service. It eliminates the high operational overhead of self-hosting, patching, and scaling an Airflow cluster. |
| **Data Lake (Storage)** | **AWS S3** | **Why:** It's the core principle of modern data stacks: **decouple storage from compute**. S3 is durable (11 nines), infinitely scalable, and cost-effective. **Role:** It acts as the single source of truth. Storing raw files in the Raw Zone (immutable) and processed Parquet files in the Curated Zone allows multiple compute engines (dbt, Snowflake, Spark) to access the same data without costly duplication. |
| **Ingestion / Processing** | **ECS/Fargate** | **Why:** Our Python scripts are too long-running and resource-intensive for AWS Lambda (which has time/memory/package size limits). Running them on dedicated EC2 instances creates a 24/7 cost and scaling/patching burden. **Why Fargate?** It's the "serverless container" sweet spot. It allows us to run our heavy-duty, containerized Python/dbt tasks with isolated resources and pay *only* for the execution time, with zero operational overhead. |
| **Data Transformation** | **dbt (Data Build Tool)** | **Why:** It moves our core business logic (from data\_cleaning.py, entity\_matching.py) out of a Python "black box" and into version-controlled, auditable SQL models. **Key Wins:** 1\. **Testing:** Data quality (unique, not null, etc.) becomes explicit dbt test steps, not implicit Python checks. 2\. **Lineage:** Auto-generates documentation and lineage graphs, making the pipeline understandable. 3\. **Modularity:** Replaces monolithic scripts with composable, reusable SQL models. |
| **Data Warehouse** | **Snowflake** | **Why:** Its **separation of storage and compute** is the key. In our prototype (PostgreSQL), a massive dbt run would lock tables and block analyst queries. **With Snowflake:** We can run a large XL-Warehouse for the 2-hour dbt build (ETL) and a separate S-Warehouse for BI Tools (Analytics). *They do not block each other*. This workload isolation is critical for production. It also natively queries Parquet on S3. |
| **Observability** | **CloudWatch & Datadog** | **Why Both?** They serve two different needs. **CloudWatch:** Monitors *AWS infrastructure*. (e.g., "Is the Fargate task CPU at 100%?", "Did the MWAA service run?"). **Datadog:** Monitors the *data application*. (e.g., "Alert me if the dbt pipeline latency exceeds 30 mins," "Dashboard the number of records matched per day," "Alert if dbt test failures increase.") |


## **5\. Database Schema (PostgreSQL DDL)**

The database schema is structured into three layers for data lineage and quality: stg (Staging for raw data), pre\_dwh (Pre-Data Warehouse for cleaned data), and dwh (Data Warehouse for the final unified view).

Refer to db/ddl\_scripts.sql for more information about the schema.

## **6\. AI Model Used & Rationale**

* **Model:** OpenAI's GPT-4  
* **Rationale:** The LLM's strength is semantic understanding and contextual reasoning, which surpasses traditional algorithms for difficult entity resolution. For instance, it can determine that "ACME Pty Ltd" at "123 Main St" is the same entity as "ACME Corporation" at "123 Main St, Sydney" even if string similarity is low, by reasoning across the entire set of address, name, and domain data points.

**Prompt Used:**

"""  
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
"""

## **7\. ETL Pipeline Implementation**

The pipeline logic is implemented as a set of Python scripts:

* **abr\_parser.py:** Extracts ABR data. Uses lxml.etree.iterparse to stream-process large XML files and psycopg2.extras.execute\_values to bulk-load data into stg.abr\_raw\_companies.  
* **commoncrawl\_scraper.py:** Extracts Common Crawl data. Queries the CC index, fetches WARC files, parses HTML with BeautifulSoup, and loads data into stg.common\_crawl\_raw\_companies.  
* **data\_cleaning.py:** Transforms the data. Reads from stg tables into pandas, performs standardization (states, postcodes), cleaning (company names), and deduplication, then loads the results into pre\_dwh tables.  
* **entity\_matching.py:** Transforms and Loads the final model. It reads from the pre\_dwh tables and performs a multi-stage entity matching process to link Common Crawl records to ABR records. The final matched dataset is loaded into dwh.dim\_entity\_match\_company\_data.

## **8\. Transformations & Data Quality**

### **dbt Models & Tests**

This prototype does not currently use dbt. Transformations are handled directly in Python scripts (data\_cleaning.py, entity\_matching.py).

In a production environment, this logic would be refactored into dbt models:

* **Staging Models:** stg\_abr\_companies and stg\_common\_crawl\_companies would be created as views on the raw tables, performing light cleaning and renaming.  
* **Intermediate Models:** int\_companies\_cleaned would handle the logic from data\_cleaning.py.  
* **Mart Models:** dim\_companies would be the final model, containing the entity matching logic from entity\_matching.py.

Data quality checks are currently implicit (e.g., ABN validation in commoncrawl\_scraper.py). dbt would formalize these using dbt tests (e.g., unique, not\_null on ABNs, accepted\_values for states).

### **Entity Matching Approach**

The entity matching design in entity\_matching.py is a multi-stage cascade:

# Stage 1: Rule-Based Match (SQL)

Logic: First, it performs a high-confidence match directly in SQL, joining records where the cleaned ABR (Australian Business Number) from Common Crawl matches a record in the ABR dataset (TRIM(cc.abn) = TRIM(abr.abn)).

Purpose: This is the fastest, cheapest, and most reliable match. It captures all the "easy wins" where a company clearly lists its valid ABN on its website.

# Stage 2: Fuzzy Match (Python + rapidfuzz)

Logic: For all Common Crawl records that did not match in Stage 1, this stage attempts a fuzzy string match on the company name.

Key Optimization (Blocking): To avoid comparing every crawl record to all 3 million+ ABR records (an N*M problem), it uses a technique called blocking. The records are "blocked" by postcode. This means it only compares the names of companies that are located in the same postcode, drastically reducing the search space.

Scoring: It uses rapidfuzz.fuzz.token_sort_ratio, which is effective at handling minor spelling differences, "Pty Ltd" vs. "Pty Limited", or reordered words.

Purpose: Catches entities that are clearly the same but are missing an ABN on their website (e.g., "Acme Inc" in 2000 vs. "Acme Incorporated" in 2000).

# Stage 3: LLM Match (Optional AI Match)

Logic: For records that still don't match, this optional step sends the candidate data (Crawl company name, ABR company name) to OpenAI's GPT-4.

Purpose: This is the "expert" step to handle high ambiguity. An LLM can use semantic reasoning to identify matches that rules and fuzzy logic would miss. For example, it can determine that Crawl: "ACME" at 123 Main St is the same as ABR: "ACME CORPORATION PTY LTD" at 123 Main St, Sydney, even if the string similarity score is low.


### Special Note

In an ideal scenario, a rule-based system will have significantly lower coverage compared to other methods. Therefore, it will primarily rely on fallback mechanisms such as fuzzy logic or an LLM-based approach. This presents an excellent opportunity to leverage the capabilities of LLMs to accomplish the task effectively.

Additionally, we can consider exposing our data warehouses to the LLM through an MCP server. This would allow the model to gain a complete understanding of the data and perform tasks more seamlessly.

**IDE Used**  - VS Code for development