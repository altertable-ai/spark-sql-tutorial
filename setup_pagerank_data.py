#!/usr/bin/env python3
"""
Script to create and populate a page links table in Altertable for PageRank demo.
Requires environment variables:
- ALTERTABLE_USERNAME: Username for authentication
- ALTERTABLE_PASSWORD: Password for authentication
- ALTERTABLE_CATALOG: Catalog name to use
"""

import os
import random
import pyarrow as pa
from altertable_flightsql import Client


def generate_page_links(num_pages=1000, min_links=5, max_links=20):
    """
    Generate realistic page link data for PageRank calculation.

    Args:
        num_pages: Number of pages in the graph
        min_links: Minimum number of outbound links per page
        max_links: Maximum number of outbound links per page

    Returns:
        List of tuples (from_page, to_page)
    """
    links = []

    # Generate links for each page
    for page_id in range(num_pages):
        # Random number of outbound links for this page
        num_links = random.randint(min_links, max_links)

        # Select random destination pages (no self-links)
        possible_destinations = [p for p in range(num_pages) if p != page_id]

        # Use power law distribution to make some pages more popular
        # Pages with lower IDs are more likely to be linked to
        weights = [1.0 / (dest + 1) ** 0.5 for dest in possible_destinations]

        # Sample destinations without replacement
        destinations = random.choices(
            possible_destinations,
            weights=weights,
            k=min(num_links, len(possible_destinations))
        )

        # Remove duplicates
        destinations = list(set(destinations))

        # Add links
        for dest in destinations:
            links.append((page_id, dest))

    return links


def main():
    # Get credentials from environment variables
    username = os.getenv('ALTERTABLE_USERNAME')
    password = os.getenv('ALTERTABLE_PASSWORD')
    catalog = os.getenv('ALTERTABLE_CATALOG')

    if not username or not password:
        raise ValueError("ALTERTABLE_USERNAME and ALTERTABLE_PASSWORD environment variables must be set")

    if not catalog:
        raise ValueError("ALTERTABLE_CATALOG environment variable must be set")

    print(f"Connecting to Altertable...")
    print(f"  Catalog: {catalog}")
    print(f"  Username: {username}")

    # Connect to Altertable
    with Client(
        username=username,
        password=password,
        catalog=catalog
    ) as client:
        print("\nConnected successfully!")

        # Generate link data
        print("\nGenerating page link data...")
        num_pages = 1000
        links = generate_page_links(num_pages)
        print(f"  Generated {len(links)} links for {num_pages} pages")

        # Prepare data for bulk insert using Arrow Flight
        print("\nInserting data using Arrow Flight bulk insert...")
        from_pages = [link[0] for link in links]
        to_pages = [link[1] for link in links]

        # Create Arrow schema
        schema = pa.schema([
            ("from_page", pa.int32()),
            ("to_page", pa.int32())
        ])

        # Create Arrow record batch
        record_batch = pa.record_batch(
            [from_pages, to_pages],
            schema=schema
        )

        # Use ingest with REPLACE mode to drop and recreate table
        from altertable_flightsql.client import IngestTableMode

        try:
            with client.ingest(
                table_name="page_links",
                schema=schema,
                mode=IngestTableMode.REPLACE
            ) as writer:
                writer.write(record_batch)
            print(f"✓ Successfully inserted {len(links)} links using Arrow Flight bulk insert")
        except Exception as e:
            print(f"✗ Error during ingest: {e}")
            raise

        # Verify data
        print("\nVerifying data...")
        reader = client.query("SELECT COUNT(*) as count FROM page_links")
        for batch in reader:
            df = batch.data.to_pandas()
            count = df['count'].iloc[0]
            print(f"  Total rows in table: {count}")

        # Show sample data
        print("\nSample data (first 10 rows):")
        reader = client.query("SELECT * FROM page_links LIMIT 10")
        for batch in reader:
            df = batch.data.to_pandas()
            print(df.to_string(index=False))

        print("\n✓ Setup complete! Table 'page_links' is ready for PageRank calculation.")


if __name__ == "__main__":
    main()
