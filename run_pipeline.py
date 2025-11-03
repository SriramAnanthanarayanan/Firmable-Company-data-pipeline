# run_pipeline.py

from extract import abr_parser, commoncrawl_scraper
from transform import data_cleaning, entity_matching


def run_pipeline(commoncrawl_limit=5, abr_limit=5):
    print("===== Step 1: Extract Common Crawl Data =====")
    cc_data = commoncrawl_scraper.run_commoncrawl_extraction(limit=commoncrawl_limit)

    print("\n===== Step 2: Extract ABR Data =====")
    abr_file = "abr_sample.xml.gz"  # Update with actual path
    abr_data = abr_parser.parse_abr_xml(abr_file, limit=abr_limit)

    print("\n===== Step 3: Transform Data =====")
    cc_clean = data_cleaning.clean_commoncrawl_data(cc_data)
    abr_clean = data_cleaning.clean_abr_data(abr_data)

    print("\n===== Step 4: Entity Matching =====")
    unified_data = entity_matching.match_entities(cc_clean, abr_clean)

    print("\nPipeline execution completed!")

if __name__ == "__main__":
    run_pipeline()
