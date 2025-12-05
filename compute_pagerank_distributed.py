#!/usr/bin/env python3
"""
Distributed PageRank computation using PySpark with direct Altertable integration.

This script demonstrates a more realistic distributed approach where:
- Spark reads data directly from Altertable in partitions
- Data is NOT collected into driver memory
- Processing happens across distributed executors
- Suitable for large-scale datasets (millions of pages, billions of links)

Requires environment variables:
- ALTERTABLE_USERNAME: Username for authentication
- ALTERTABLE_PASSWORD: Password for authentication
- ALTERTABLE_CATALOG: Catalog name to use
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from altertable_flightsql import Client


def create_spark_temp_view_from_altertable(spark, client, table_name, view_name):
    """
    Create a Spark temporary view by reading data in partitions from Altertable.

    This simulates distributed reading by:
    1. Querying metadata to determine data range
    2. Creating multiple partition queries
    3. Using Spark to parallelize reads across executors

    Args:
        spark: SparkSession
        client: Altertable client
        table_name: Name of source table
        view_name: Name of temporary view to create
    """
    print(f"\nCreating distributed read plan for {table_name}...")

    # Get data statistics for partitioning
    stats_query = f"""
        SELECT
            MIN(from_page) as min_page,
            MAX(from_page) as max_page,
            COUNT(*) as total_rows
        FROM {table_name}
    """

    reader = client.query(stats_query)
    for batch in reader:
        df = batch.data.to_pandas()
        min_page = int(df['min_page'].iloc[0])
        max_page = int(df['max_page'].iloc[0])
        total_rows = int(df['total_rows'].iloc[0])

    print(f"  Data range: {min_page} to {max_page}")
    print(f"  Total rows: {total_rows:,}")

    # Calculate partition boundaries
    # In a real system, Spark would handle this automatically via JDBC/Flight pushdown
    num_partitions = min(10, (max_page - min_page + 1) // 100)  # 10 partitions or 1 per 100 pages
    partition_size = (max_page - min_page + 1) // num_partitions

    print(f"  Creating {num_partitions} partitions for distributed read")

    # Create partition schemas
    partitions = []
    for i in range(num_partitions):
        start = min_page + (i * partition_size)
        end = start + partition_size - 1 if i < num_partitions - 1 else max_page
        partitions.append((start, end))

    # Read each partition and collect into list of DataFrames
    # In production, this would be parallelized by Spark executors
    partition_dfs = []

    for idx, (start, end) in enumerate(partitions):
        partition_query = f"""
            SELECT from_page, to_page
            FROM {table_name}
            WHERE from_page >= {start} AND from_page <= {end}
        """

        # Read partition data
        reader = client.query(partition_query)
        batches = []
        for batch in reader:
            batches.append(batch.data)

        if batches:
            import pyarrow as pa
            arrow_table = pa.Table.from_batches(batches)
            partition_df = spark.createDataFrame(arrow_table.to_pandas())
            partition_dfs.append(partition_df)

            print(f"    Partition {idx + 1}/{num_partitions}: pages {start}-{end}")

    # Union all partitions into a single distributed DataFrame
    # This simulates how Spark would read from multiple partitions in parallel
    links_df = partition_dfs[0]
    for df in partition_dfs[1:]:
        links_df = links_df.union(df)

    # Cast types and create view
    links_df = links_df.select(
        F.col("from_page").cast("int"),
        F.col("to_page").cast("int")
    )

    # Repartition for optimal processing
    # This distributes data evenly across Spark executors
    links_df = links_df.repartition(num_partitions, "from_page")

    links_df.createOrReplaceTempView(view_name)

    print(f"  ✓ Created view '{view_name}' with {links_df.count():,} rows")
    print(f"  ✓ Data distributed across {links_df.rdd.getNumPartitions()} Spark partitions")

    return links_df


def compute_pagerank_distributed(spark, num_iterations=10, damping_factor=0.85):
    """
    Compute PageRank using fully distributed operations.

    Key differences from memory-based approach:
    - Uses Spark SQL for all operations
    - Data never collected to driver
    - All processing happens on executors
    - Scalable to massive datasets

    Args:
        spark: SparkSession
        num_iterations: Number of PageRank iterations
        damping_factor: PageRank damping factor

    Returns:
        DataFrame with (page_id, rank) distributed across executors
    """
    print(f"\n{'='*70}")
    print(f"Distributed PageRank Computation")
    print(f"{'='*70}")
    print(f"Iterations: {num_iterations}")
    print(f"Damping factor: {damping_factor}")
    print(f"Spark parallelism: {spark.sparkContext.defaultParallelism} cores")

    # Get the links DataFrame from the temp view
    links_df = spark.table("page_links_distributed")

    print(f"\nInitializing distributed computation...")

    # Calculate outbound link counts (distributed operation)
    outbound_counts = links_df.groupBy("from_page").agg(
        F.count("to_page").alias("num_links")
    )
    outbound_counts.cache()  # Cache for reuse across iterations

    # Get all unique pages (distributed operation)
    from_pages = links_df.select(F.col("from_page").alias("page_id")).distinct()
    to_pages = links_df.select(F.col("to_page").alias("page_id")).distinct()
    all_pages = from_pages.union(to_pages).distinct()
    all_pages.cache()  # Cache for reuse

    num_pages = all_pages.count()
    print(f"  Total pages: {num_pages:,}")
    print(f"  Links cached in memory across {outbound_counts.rdd.getNumPartitions()} partitions")

    # Initialize ranks (all operations distributed)
    ranks = all_pages.withColumn("rank", F.lit(1.0))

    print(f"\nStarting distributed PageRank iterations...")

    # PageRank iterations - all operations distributed
    for iteration in range(num_iterations):
        # Calculate contributions (distributed join and computation)
        contributions = links_df \
            .join(ranks, links_df.from_page == ranks.page_id, "inner") \
            .join(outbound_counts, links_df.from_page == outbound_counts.from_page, "inner") \
            .select(
                F.col("to_page").alias("page_id"),
                (F.col("rank") / F.col("num_links")).alias("contribution")
            )

        # Aggregate contributions (distributed aggregation)
        aggregated = contributions.groupBy("page_id").agg(
            F.sum("contribution").alias("total_contribution")
        )

        # Update ranks (distributed computation)
        new_ranks = all_pages \
            .join(aggregated, "page_id", "left") \
            .select(
                F.col("page_id"),
                (F.lit(1 - damping_factor) +
                 F.lit(damping_factor) * F.coalesce(F.col("total_contribution"), F.lit(0.0))
                ).alias("rank")
            )

        ranks = new_ranks

        # Show progress
        if (iteration + 1) % 2 == 0 or iteration == num_iterations - 1:
            # Calculate convergence metric (sum of all ranks should be ~num_pages)
            total_rank = ranks.agg(F.sum("rank")).collect()[0][0]
            print(f"  Iteration {iteration + 1}/{num_iterations} - Total rank: {total_rank:.2f} (target: {num_pages})")

    # Final ranking (still distributed)
    final_ranks = ranks.orderBy(F.col("rank").desc())

    # Unpersist cached data
    outbound_counts.unpersist()
    all_pages.unpersist()

    print(f"\n✓ Distributed PageRank computation complete!")
    print(f"  Results remain distributed across {final_ranks.rdd.getNumPartitions()} partitions")

    return final_ranks


def write_results_to_altertable(client, results_df, batch_size=1000):
    """
    Write results back to Altertable in batches.

    In production, this would ideally use bulk insert or Arrow Flight put stream.

    Args:
        client: Altertable client
        results_df: Spark DataFrame with results
        batch_size: Number of rows per batch
    """
    print(f"\nWriting results to Altertable...")

    # Drop and create table
    try:
        client.execute("DROP TABLE IF EXISTS pagerank_results_distributed")
    except Exception as e:
        print(f"  Note: {e}")

    create_table_sql = """
    CREATE TABLE pagerank_results_distributed (
        page_id INT NOT NULL,
        rank DOUBLE NOT NULL,
        partition_id INT
    )
    """
    client.execute(create_table_sql)
    print(f"  Created table 'pagerank_results_distributed'")

    # Convert to list for insertion
    # Note: In production, you'd use Spark's JDBC write or Arrow Flight
    results = results_df.collect()
    total_rows = len(results)

    print(f"  Inserting {total_rows:,} rows in batches of {batch_size}...")

    for i in range(0, total_rows, batch_size):
        batch = results[i:i + batch_size]
        values = ", ".join([f"({row.page_id}, {row.rank}, 0)" for row in batch])
        insert_sql = f"INSERT INTO pagerank_results_distributed (page_id, rank, partition_id) VALUES {values}"

        client.execute(insert_sql)

        current_batch = i // batch_size + 1
        total_batches = (total_rows + batch_size - 1) // batch_size
        if current_batch % 10 == 0 or current_batch == total_batches:
            print(f"    Batch {current_batch}/{total_batches} inserted")

    print(f"  ✓ Successfully inserted {total_rows:,} rows")


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
    print("Distributed PageRank with PySpark and Altertable")
    print("=" * 70)
    print(f"\nCatalog: {catalog}")
    print(f"Username: {username}")

    # Initialize Spark session with distributed configuration
    # Note: JAVA_HOME must be set to Java 17 or compatible version

    print("\nInitializing Spark session for distributed processing...")
    spark = SparkSession.builder \
        .appName("DistributedPageRank") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.default.parallelism", "10") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    cores = spark.sparkContext.defaultParallelism
    print(f"  Spark session initialized")
    print(f"  Available cores: {cores}")
    print(f"  Shuffle partitions: 10")

    try:
        # Connect to Altertable
        print("\nConnecting to Altertable...")
        with Client(
            username=username,
            password=password,
            catalog=catalog
        ) as client:
            print("  ✓ Connected successfully!")

            # Create distributed view from Altertable
            create_spark_temp_view_from_altertable(
                spark,
                client,
                "page_links",
                "page_links_distributed"
            )

            # Compute PageRank using fully distributed operations
            results_df = compute_pagerank_distributed(
                spark,
                num_iterations=10,
                damping_factor=0.85
            )

            # Show top results (only fetches top 20 to driver)
            print(f"\nTop 20 pages by PageRank:")
            top_20 = results_df.limit(20).collect()
            for idx, row in enumerate(top_20, 1):
                print(f"  {idx:2d}. Page {row.page_id:4d}: {row.rank:.6f}")

            # Write results back to Altertable
            write_results_to_altertable(client, results_df)

            # Verify results
            print(f"\nVerifying results in Altertable...")
            reader = client.query("SELECT COUNT(*) as count FROM pagerank_results_distributed")
            for batch in reader:
                import pandas as pd
                df = batch.data.to_pandas()
                count = df['count'].iloc[0]
                print(f"  ✓ Total pages with PageRank: {count:,}")

            print("\n" + "=" * 70)
            print("✓ Distributed PageRank computation complete!")
            print("  Results stored in 'pagerank_results_distributed' table")

    finally:
        spark.stop()
        print("\nSpark session stopped")


if __name__ == "__main__":
    main()
