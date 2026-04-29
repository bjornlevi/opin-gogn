#!/usr/bin/env python3
"""
Enrich Birgi (sellers) from both Rikið and Reykjavík with VAT numbers.

Process:
1. Extract unique seller names from both Rikið and Reykjavík data
2. Search fyrirtækjaskrá for each seller to find national ID
3. Use the API to look up company details and VAT number
4. Create lookup table with seller -> VAT number mappings

Usage:
    python3 enrich_rikid_vat_from_api.py \
      --rikid-input <rikid_parquet> \
      --reykjavik-input <reykjavik_parquet> \
      --output <output_parquet> \
      [--api-key <key>]
"""
import argparse
import duckdb
import requests
import re
import json
import time
from pathlib import Path
import sys

# Search page for finding national IDs
SEARCH_URL = "https://www.skatturinn.is/fyrirtaekjaskra/leit/"
# API for getting company details
API_BASE = "https://api.skattur.cloud/legalentities/v2.1"

# Rate limiting
SEARCH_DELAY = 0.5  # seconds between searches
API_DELAY = 0.2     # seconds between API calls

class CompanyLookup:
    def __init__(self, api_key=None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; Birgi-VAT-Enrichment/1.0)'
        })
        self.api_key = api_key
        if api_key:
            self.session.headers.update({
                'Ocp-Apim-Subscription-Key': api_key
            })
        self.last_request = 0

    def search_company(self, name):
        """Search for company by name and return national ID."""
        try:
            # Rate limiting
            elapsed = time.time() - self.last_request
            if elapsed < SEARCH_DELAY:
                time.sleep(SEARCH_DELAY - elapsed)
            self.last_request = time.time()

            response = self.session.get(SEARCH_URL, params={"q": name}, timeout=10)
            if response.status_code != 200:
                return None

            # Extract 10-digit national IDs from the response
            ids = re.findall(r'\b\d{10}\b', response.text)
            if ids:
                # Return the first (most likely match)
                return ids[0]
            return None

        except Exception as e:
            print(f"  Error searching for '{name}': {e}", file=sys.stderr)
            return None

    def get_vat_number(self, national_id):
        """Get VAT number from company API using national ID."""
        try:
            # Rate limiting
            elapsed = time.time() - self.last_request
            if elapsed < API_DELAY:
                time.sleep(API_DELAY - elapsed)
            self.last_request = time.time()

            url = f"{API_BASE}/{national_id}"
            response = self.session.get(url, params={"language": "is"}, timeout=10)

            if response.status_code != 200:
                return None

            data = response.json()

            # Extract VAT number from the vat array
            if "vat" in data and isinstance(data["vat"], list):
                # Get the first active VAT number (not deregistered)
                for vat_entry in data["vat"]:
                    if "vatNumber" in vat_entry:
                        vat_num = vat_entry["vatNumber"]
                        # Check if still registered (no deRegistered date)
                        if not vat_entry.get("deRegistered"):
                            return vat_num
                # If all are deregistered, return the latest
                if data["vat"]:
                    return data["vat"][0].get("vatNumber")

            return None

        except Exception as e:
            print(f"  Error getting VAT for ID '{national_id}': {e}", file=sys.stderr)
            return None

def main():
    parser = argparse.ArgumentParser(
        description="Enrich sellers (Birgi) from Rikið and Reykjavík with VAT numbers from fyrirtækjaskrá API"
    )
    parser.add_argument("--rikid-input", required=True, help="Rikið parquet file")
    parser.add_argument("--reykjavik-input", required=True, help="Reykjavík parquet file")
    parser.add_argument("--output", required=True, help="Output parquet file with VAT lookup")
    parser.add_argument("--api-key", help="API subscription key for fyrirtækjaskrá")
    args = parser.parse_args()

    rikid_path = Path(args.rikid_input)
    reykjavik_path = Path(args.reykjavik_input)
    output_path = Path(args.output)

    if not rikid_path.exists():
        print(f"Rikið file not found: {rikid_path}", file=sys.stderr)
        sys.exit(1)
    if not reykjavik_path.exists():
        print(f"Reykjavík file not found: {reykjavik_path}", file=sys.stderr)
        sys.exit(1)

    print("Reading data sources...")
    con = duckdb.connect(":memory:")

    # Read Rikið data
    con.execute(f"CREATE TABLE rikid AS SELECT * FROM read_parquet('{rikid_path}')")

    # Read Reykjavík data
    con.execute(f"CREATE TABLE reykjavik AS SELECT * FROM read_parquet('{reykjavik_path}')")

    # Get unique sellers from Rikið
    rikid_sellers = con.execute(
        'SELECT DISTINCT COALESCE(NULLIF(TRIM("Birgi"), \'\'), \'(óskráð)\') as seller FROM rikid WHERE "Birgi" IS NOT NULL ORDER BY seller'
    ).fetchall()

    # Get unique sellers from Reykjavík (using the same logic as RKV_SUPPLIER_EXPR)
    reykjavik_sellers = con.execute("""
        SELECT DISTINCT COALESCE(
            NULLIF(TRIM(xfyrirtaeki), ''),
            NULLIF(TRIM(fyrirtaeki), ''),
            NULLIF(TRIM(CAST(vm_numer AS VARCHAR)), '')
        ) AS seller
        FROM reykjavik
        ORDER BY seller
    """).fetchall()

    # Combine and deduplicate
    all_sellers = set()
    all_sellers.update([s[0] for s in rikid_sellers])
    all_sellers.update([s[0] for s in reykjavik_sellers])
    seller_names = sorted(list(all_sellers))

    print(f"Found {len(seller_names)} unique sellers from both sources")
    print(f"  - Rikið: {len(rikid_sellers)} sellers")
    print(f"  - Reykjavík: {len(reykjavik_sellers)} sellers")

    # Initialize lookup client
    lookup = CompanyLookup(api_key=args.api_key)

    # Create lookup table with enriched data
    con.execute("""
        CREATE TABLE vat_lookup (
            seller_name VARCHAR,
            vm_numer VARCHAR,
            vsk_link VARCHAR
        )
    """)

    print("Searching for VAT numbers...")
    found_count = 0

    for i, seller_name in enumerate(seller_names, 1):
        if i % 50 == 0:
            print(f"  Processed {i}/{len(seller_names)}")

        # Search for national ID
        national_id = lookup.search_company(seller_name)
        vat_number = None

        if national_id:
            # Get VAT number from API
            vat_number = lookup.get_vat_number(national_id)
            if vat_number:
                found_count += 1

        # Create vsk_link if we have VAT number
        vsk_link = None
        if vat_number:
            vsk_link = f"https://www.skatturinn.is/fyrirtaekjaskra/leit/vsk-numer/{vat_number}"

        # Insert into lookup table
        con.execute(
            "INSERT INTO vat_lookup VALUES (?, ?, ?)",
            [seller_name, vat_number, vsk_link]
        )

    print(f"Found {found_count}/{len(seller_names)} sellers with VAT numbers")

    # Write the VAT lookup table
    con.execute(f"""
        COPY vat_lookup TO '{output_path}' (FORMAT PARQUET)
    """)

    print(f"Wrote VAT lookup table to {output_path}")

if __name__ == "__main__":
    main()
