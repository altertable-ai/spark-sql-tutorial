#!/usr/bin/env python3
"""
Script to compute PageRank using PySpark on data from Altertable.

Requires environment variables:
- ALTERTABLE_USERNAME: Username for authentication
- ALTERTABLE_PASSWORD: Password for authentication
- ALTERTABLE_CATALOG: Catalog name to use
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from altertable_flightsql import Client
import pyarrow as pa


def read_page_links_from_altertable(client):
    """
    Read page_links table from Altertable and convert to Spark DataFrame.

    Args:
        client: Altertable client connection

    Returns:
        PySpark DataFrame with columns (from_page, to_page)
    """
    print("Reading page_links table from Altertable...")

    # Query all page links
    reader = client.query("SELECT from_page, to_page FROM page_links")

    # Collect all batches into a PyArrow table
    batches = []
    for batch in reader:
        batches.append(batch.data)

    if not batches:
        raise ValueError("No data found in page_links table")

    # Combine batches into a single PyArrow table
    arrow_table = pa.Table.from_batches(batches)
    print(f"  Read {arrow_table.num_rows} links from Altertable")

    return arrow_table


def write_pagerank_to_altertable(client, pagerank_data):
    """
    Write PageRank results back to Altertable using Arrow Flight bulk insert.

    Args:
        client: Altertable client connection
        pagerank_data: List of tuples (page_id, rank)
    """
    print("\nWriting PageRank results to Altertable...")

    # Prepare data for bulk insert
    page_ids = [row[0] for row in pagerank_data]
    ranks = [row[1] for row in pagerank_data]

    # Create Arrow schema
    schema = pa.schema([
        ("page_id", pa.int32()),
        ("rank", pa.float64())
    ])

    # Create Arrow record batch
    record_batch = pa.record_batch(
        [page_ids, ranks],
        schema=schema
    )

    # Use ingest with REPLACE mode to drop and recreate table
    from altertable_flightsql.client import IngestTableMode

    print(f"  Inserting {len(pagerank_data)} rows using Arrow Flight bulk insert...")
    try:
        with client.ingest(
            table_name="pagerank_results",
            schema=schema,
            schema_name="main", # TODO: remove once backend supports it
            catalog_name=os.getenv('ALTERTABLE_CATALOG'), # TODO: remove once backend supports it
            mode=IngestTableMode.REPLACE
        ) as writer:
            writer.write(record_batch)
        print(f"  ✓ Successfully inserted {len(pagerank_data)} PageRank results")
    except Exception as e:
        print(f"  ✗ Error during ingest: {e}")
        raise


def compute_pagerank_spark(spark, arrow_table, num_iterations=10, damping_factor=0.85):
    """
    Compute PageRank using PySpark.

    Args:
        spark: SparkSession
        arrow_table: PyArrow table with page links
        num_iterations: Number of PageRank iterations
        damping_factor: PageRank damping factor (typically 0.85)

    Returns:
        List of tuples (page_id, rank) sorted by rank descending
    """
    print(f"\nComputing PageRank with {num_iterations} iterations...")

    # Convert PyArrow table to Spark DataFrame
    links_df = spark.createDataFrame(arrow_table.to_pandas())
    links_df = links_df.select(
        F.col("from_page").cast("int"),
        F.col("to_page").cast("int")
    )

    print(f"  Loaded {links_df.count()} links into Spark")

    # Get all unique pages
    from_pages = links_df.select(F.col("from_page").alias("page_id")).distinct()
    to_pages = links_df.select(F.col("to_page").alias("page_id")).distinct()
    all_pages = from_pages.union(to_pages).distinct()

    num_pages = all_pages.count()
    print(f"  Total unique pages: {num_pages}")

    # Calculate outbound link count for each page
    outbound_counts = links_df.groupBy("from_page").agg(
        F.count("to_page").alias("num_links")
    )

    # Initialize ranks (start with 1.0 for all pages)
    initial_rank = 1.0
    ranks = all_pages.withColumn("rank", F.lit(initial_rank))

    # PageRank iterations
    for iteration in range(num_iterations):
        # Join links with current ranks and outbound counts
        contributions = links_df \
            .join(ranks, links_df.from_page == ranks.page_id, "inner") \
            .join(outbound_counts, links_df.from_page == outbound_counts.from_page, "inner") \
            .select(
                F.col("to_page").alias("page_id"),
                (F.col("rank") / F.col("num_links")).alias("contribution")
            )

        # Aggregate contributions for each page
        aggregated = contributions.groupBy("page_id").agg(
            F.sum("contribution").alias("total_contribution")
        )

        # Update ranks with damping factor
        # New rank = (1 - d) + d * (sum of contributions)
        new_ranks = all_pages \
            .join(aggregated, "page_id", "left") \
            .select(
                F.col("page_id"),
                (F.lit(1 - damping_factor) +
                 F.lit(damping_factor) * F.coalesce(F.col("total_contribution"), F.lit(0.0))
                ).alias("rank")
            )

        ranks = new_ranks

        if (iteration + 1) % 2 == 0 or iteration == num_iterations - 1:
            print(f"  Iteration {iteration + 1}/{num_iterations} complete")

    # Sort by rank descending and collect results
    final_ranks = ranks.orderBy(F.col("rank").desc())

    print("\nPageRank computation complete!")
    print("\nTop 10 pages by PageRank:")
    top_10 = final_ranks.limit(10).collect()
    for idx, row in enumerate(top_10, 1):
        print(f"  {idx}. Page {row.page_id}: {row.rank:.6f}")

    # Collect all results
    results = [(row.page_id, row.rank) for row in final_ranks.collect()]

    return results


def main():
    # Get credentials from environment variables
    username = os.getenv('ALTERTABLE_USERNAME')
    password = os.getenv('ALTERTABLE_PASSWORD')
    catalog = os.getenv('ALTERTABLE_CATALOG')

    if not username or not password:
        raise ValueError("ALTERTABLE_USERNAME and ALTERTABLE_PASSWORD environment variables must be set")

    if not catalog:
        raise ValueError("ALTERTABLE_CATALOG environment variable must be set")

    print("=" * 70)
    print("PageRank Computation with PySpark and Altertable")
    print("=" * 70)
    print(f"Catalog: {catalog}")
    print(f"Username: {username}")

    # Initialize Spark session
    # Note: JAVA_HOME must be set to Java 17 or compatible version
    print("\nInitializing Spark session...")
    spark = SparkSession.builder \
        .appName("PageRank") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("  Spark session initialized")

    try:
        # Connect to Altertable
        print("\nConnecting to Altertable...")
        with Client(
            username=username,
            password=password,
            catalog=catalog
        ) as client:
            print("  Connected successfully!")

            # Read page links from Altertable
            arrow_table = read_page_links_from_altertable(client)

            # Compute PageRank with Spark
            pagerank_results = compute_pagerank_spark(
                spark,
                arrow_table,
                num_iterations=10,
                damping_factor=0.85
            )

            # Write results back to Altertable
            write_pagerank_to_altertable(client, pagerank_results)

            # Verify results
            print("\nVerifying results in Altertable...")
            reader = client.query("SELECT COUNT(*) as count FROM pagerank_results")
            for batch in reader:
                df = batch.data.to_pandas()
                count = df['count'].iloc[0]
                print(f"  Total pages with PageRank: {count}")

            # Show top 20 pages
            print("\nTop 20 pages by PageRank (from Altertable):")
            reader = client.query("""
                SELECT page_id, rank
                FROM pagerank_results
                ORDER BY rank DESC
                LIMIT 20
            """)
            for batch in reader:
                df = batch.data.to_pandas()
                for idx, row in df.iterrows():
                    print(f"  {idx + 1}. Page {row['page_id']}: {row['rank']:.6f}")

            print("\n" + "=" * 70)
            print("✓ PageRank computation complete!")
            print("  Results stored in 'pagerank_results' table")
            print("=" * 70)

    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped")


if __name__ == "__main__":
    main()
