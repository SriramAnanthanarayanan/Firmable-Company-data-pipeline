import re
import json
import os
import requests
from io import BytesIO
from urllib.parse import urlparse
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ------------------- Load DB Config ------------------- #
load_dotenv(dotenv_path="C:/Users/Admin/Desktop/firmable-etl-pipeline/.venv/.env")  # full path

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# ------------------- Utility Functions ------------------- #
def clean_text(text: str) -> str:
    return ' '.join(text.split())

def extract_domain(url: str) -> str:
    domain = urlparse(url).netloc
    return domain[4:] if domain.startswith("www.") else domain

def extract_company_name(domain: str) -> str:
    name = domain
    for suffix in ['.com.au', '.net.au', '.org.au', '.au', '.com', '.net', '.org']:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    name = name.replace('-', ' ').replace('_', ' ').title()
    return name

def validate_abn(abn: str) -> bool:
    if not abn:
        return False
    abn_clean = re.sub(r'[\s\-]', '', abn)
    if not re.match(r'^\d{11}$', abn_clean):
        return False
    weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    total = sum((int(d) - 1 if i == 0 else int(d)) * w for i, (d, w) in enumerate(zip(abn_clean, weights)))
    return total % 89 == 0

def extract_abn(text: str):
    for pattern in [r'ABN[:\s]*([0-9 ]{11,20})', r'\b(\d{2}\s?\d{3}\s?\d{3}\s?\d{3})\b']:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            abn = re.sub(r'\s+', '', match.group(1))
            if validate_abn(abn):
                return abn
    return None

def extract_emails(text: str):
    return re.findall(r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}', text)

def extract_phone(text: str):
    return re.findall(r'(\+61\s?\d{1,2}\s?\d{3}\s?\d{3}|\(0\d\)\s?\d{4}\s?\d{4}|\d{4}\s?\d{3}\s?\d{3})', text)

def extract_postcode(text: str):
    match = re.search(r'\b(0[289][0-9]{2}|[1-9][0-9]{3})\b', text)
    return match.group(1) if match else None

# ------------------- PostgreSQL Storage ------------------- #
def store_to_postgres(records, table_name="prd_firmable.stg.common_crawl_raw_companies"):
    if not records:
        print("No records to store.")
        return
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Drop & recreate table (full load)
    cursor.execute(f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            url TEXT NOT NULL,
            domain TEXT NOT NULL,
            company_name TEXT,
            abn CHAR(20),
            title TEXT,
            emails TEXT[],
            phones TEXT[],
            postcode CHAR(20),
            structured_data JSONB,
            snippet TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()

    insert_query = f"""
        INSERT INTO {table_name}
        (url, domain, company_name, abn, title, emails, phones, postcode, structured_data, snippet)
        VALUES %s
        ON CONFLICT DO NOTHING;
    """

    values = [
        (
            r["url"],
            r["domain"],
            r.get("company_name"),
            r.get("abn"),
            r.get("title"),
            r.get("emails"),
            r.get("phones"),
            r.get("postcode"),
            json.dumps(r.get("structured_data")),
            r.get("snippet")
        )
        for r in records
    ]

    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted {len(values)} records into PostgreSQL.")

# ------------------- Common Crawl Scraper ------------------- #
class CommonCrawlScraper:
    def __init__(self, index_url: str):
        self.index_url = index_url

    def count_total_urls(self):
        """Estimate total matching URLs."""
        count = 0
        try:
            with requests.get(self.index_url, stream=True, timeout=30) as r:
                r.raise_for_status()
                for line in r.iter_lines():
                    if line:
                        count += 1
        except Exception as e:
            print(f"Error counting URLs: {e}")
        return count

    def fetch_metadata(self, batch_size=1000):
        """Yield metadata in batches."""
        batch = []
        try:
            with requests.get(self.index_url, stream=True, timeout=30) as r:
                r.raise_for_status()
                for line in r.iter_lines():
                    if not line:
                        continue
                    try:
                        record = json.loads(line.decode("utf-8"))
                        batch.append(record)
                        if len(batch) >= batch_size:
                            yield batch
                            batch = []
                    except json.JSONDecodeError:
                        continue
                if batch:
                    yield batch
        except Exception as e:
            print(f"Error fetching metadata: {e}")

    def fetch_html(self, record):
        filename, offset, length = record.get("filename"), record.get("offset"), record.get("length")
        if not all([filename, offset, length]):
            return ""
        try:
            warc_url = f"https://data.commoncrawl.org/{filename}"
            headers = {"Range": f"bytes={offset}-{int(offset)+int(length)-1}"}
            response = requests.get(warc_url, headers=headers, timeout=30)
            response.raise_for_status()
            for rec in ArchiveIterator(BytesIO(response.content)):
                if rec.rec_type == "response":
                    return rec.content_stream().read().decode("utf-8", errors="ignore")
        except Exception as e:
            print(f"Error reading WARC: {e}")
        return ""

    def parse_html(self, html: str, url: str):
        soup = BeautifulSoup(html, "html.parser")
        text = clean_text(soup.get_text())

        structured_data = []
        for script in soup.find_all("script", {"type": "application/ld+json"}):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    structured_data.extend(data)
                else:
                    structured_data.append(data)
            except Exception:
                continue

        return {
            "url": url,
            "domain": extract_domain(url),
            "company_name": extract_company_name(extract_domain(url)),
            "title": soup.title.get_text(strip=True) if soup.title else None,
            "abn": extract_abn(text),
            "emails": extract_emails(text),
            "phones": extract_phone(text),
            "postcode": extract_postcode(text),
            "structured_data": structured_data,
            "snippet": text[:500]
        }

    def run(self, batch_size=1000):
        total_count = self.count_total_urls()
        print(f"Total matching URLs in index: {total_count}\n")

        all_results = []
        for batch_num, batch_metadata in enumerate(self.fetch_metadata(batch_size=batch_size), start=1):
            print(f"Processing batch {batch_num}, size={len(batch_metadata)}")
            for rec in batch_metadata:
                html = self.fetch_html(rec)
                if html:
                    all_results.append(self.parse_html(html, rec["url"]))
        return all_results

# ------------------- Main Execution ------------------- #
if __name__ == "__main__":
    query = "*.com.au"
    index_url = f"https://index.commoncrawl.org/CC-MAIN-2025-13-index?url={query}&output=json"

    scraper = CommonCrawlScraper(index_url)
    scraped_data = scraper.run(batch_size=1000)

    # Store scraped data in PostgreSQL
    store_to_postgres(scraped_data)

    print("Scraping and storage complete.")
