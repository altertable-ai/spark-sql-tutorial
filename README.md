# Spark SQL Tutorial - PageRank Demo

A demonstration project that computes PageRank using PySpark on data stored in Altertable via FlightSQL.

## Overview

This project showcases:

- **Data Generation**: Creates a realistic web page link graph with 1,000 pages
- **Distributed Computing**: Uses PySpark to compute PageRank in a distributed manner
- **FlightSQL Integration**: Reads from and writes to Altertable using Apache Arrow FlightSQL
- **Efficient Bulk Insert**: Uses Arrow Flight's `ingest` method for high-performance data loading

## What is PageRank?

PageRank is the algorithm Google used to rank web pages in search results. It works by:

1. Treating web pages as nodes in a graph
2. Links between pages as directed edges
3. Iteratively computing importance scores based on incoming links
4. Pages linked by important pages become important themselves

## Requirements

- Python 3.13+
- Java 17 (installed via Homebrew)
- uv (Python package manager)
- Access to an Altertable instance

## Project Structure

```
setup_pagerank_data.py           # Generates and loads sample link data
compute_pagerank.py              # Computes PageRank using PySpark
compute_pagerank_distributed.py  # Computes PageRank using PySpark in a distributed manner
pyproject.toml                   # Project dependencies
README.md                        # This file
```

## Dependencies

- **pyspark** (4.0.1) - Distributed computing framework
- **altertable-flightsql** (0.2.1) - FlightSQL client for Altertable
- **pandas** (2.3.3) - Data manipulation
- **pyarrow** (22.0.0) - Columnar data format

## Setup

### 1. Install Java 17

The project requires Java 17 for PySpark compatibility:

```bash
brew install openjdk@17
```

### 2. Set Environment Variables

Configure Java and your Altertable connection:

```bash
# Set JAVA_HOME to Java 17 (required for PySpark compatibility)
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home

# Configure Altertable connection
export ALTERTABLE_USERNAME="your_username"
export ALTERTABLE_PASSWORD="your_password"
export ALTERTABLE_CATALOG="your_catalog"
```

**Note**: The `JAVA_HOME` path shown above is for Homebrew's openjdk@17 on macOS. Adjust the path based on your system:

- **macOS (Homebrew)**: `/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`
- **Linux**: Typically `/usr/lib/jvm/java-17-openjdk` or similar
- **Use `java_home` on macOS**: `export JAVA_HOME=$(/usr/libexec/java_home -v 17)`

### 3. Install Dependencies

Dependencies are automatically managed by `uv`:

```bash
uv sync
```

## Usage

### Step 1: Generate Sample Data

Create a table with sample page link data (1,000 pages, ~12,000 links):

```bash
uv run setup_pagerank_data.py
```

This script will:

- Connect to your Altertable instance
- Create a `page_links` table with columns `from_page` and `to_page`
- Generate realistic link data using power law distribution
- Insert ~12,000 page-to-page links
- Display sample data for verification

**Output:**

```
Connecting to Altertable...
  Catalog: your_catalog
  Username: your_username

Connected successfully!

Generating page link data...
  Generated 12458 links for 1000 pages

Inserting data...
  Batch 125/125 inserted

  Successfully inserted 12458 links into page_links table
```

### Step 2: Compute PageRank

Run the PageRank computation using PySpark:

```bash
uv run compute_pagerank.py
```

This script will:

- Initialize a local Spark session (using Java 17)
- Read the `page_links` table from Altertable
- Compute PageRank over 10 iterations with damping factor 0.85
- Store results in a new `pagerank_results` table
- Display the top 20 pages by rank

**Output:**

```
======================================================================
PageRank Computation with PySpark and Altertable
======================================================================

Catalog: your_catalog
Username: your_username

Initializing Spark session...
  Spark session initialized

Connecting to Altertable...
  Connected successfully!

Reading page_links table from Altertable...
  Read 12458 links from Altertable

Computing PageRank with 10 iterations...
  Loaded 12458 links into Spark
  Total unique pages: 1000
  Iteration 10/10 complete

PageRank computation complete!

Top 10 pages by PageRank:
  1. Page 42: 2.847291
  2. Page 156: 2.634819
  3. Page 89: 2.512347
  ...

  PageRank computation complete!
  Results stored in 'pagerank_results' table
```

### Step 3: Distributed PageRank (Production Approach)

For a more realistic, production-ready approach that scales to large datasets:

```bash
uv run compute_pagerank_distributed.py
```

**Key Differences from Basic Approach:**

| Aspect           | Basic                     | Distributed                        |
| ---------------- | ------------------------- | ---------------------------------- |
| **Data Loading** | All data to driver memory | Partitioned reads across executors |
| **Processing**   | Driver coordinates        | Executors process in parallel      |
| **Memory**       | Driver bottleneck         | Distributed across cluster         |
| **Scalability**  | Thousands of pages        | Millions/billions of pages         |
| **Use Case**     | Learning, prototyping     | Production systems                 |

The distributed script:

- Reads data in partitions (simulates distributed JDBC/Flight reads)
- Shows partition information during execution
- Keeps all data distributed across Spark executors
- Never collects full dataset to driver
- More realistic for real-world large-scale PageRank

**Output includes partition information:**

```
Creating distributed read plan for page_links...
  Data range: 0 to 999
  Total rows: 12,458
  Creating 10 partitions for distributed read
    Partition 1/10: pages 0-99
    Partition 2/10: pages 100-199
    ...
  ✓ Data distributed across 10 Spark partitions

Distributed PageRank Computation
  Spark parallelism: 8 cores
  Iteration 10/10 - Total rank: 1000.00 (target: 1000)
```

## Querying Results

After running both scripts, you can query the results from Altertable:

### View Top Pages

```sql
SELECT page_id, rank
FROM pagerank_results
ORDER BY rank DESC
LIMIT 20;
```

### Compare Links to Rank

```sql
SELECT
    pr.page_id,
    pr.rank,
    COUNT(pl.from_page) as incoming_links
FROM pagerank_results pr
LEFT JOIN page_links pl ON pr.page_id = pl.to_page
GROUP BY pr.page_id, pr.rank
ORDER BY pr.rank DESC
LIMIT 20;
```

### Average Rank by Incoming Links

```sql
SELECT
    link_count,
    AVG(rank) as avg_rank,
    COUNT(*) as num_pages
FROM (
    SELECT
        pr.page_id,
        pr.rank,
        COUNT(pl.from_page) as link_count
    FROM pagerank_results pr
    LEFT JOIN page_links pl ON pr.page_id = pl.to_page
    GROUP BY pr.page_id, pr.rank
)
GROUP BY link_count
ORDER BY link_count;
```

## Troubleshooting

### Java Version Issues

If you see `UnsupportedOperationException: getSubject is not supported` or other Java-related errors:

- Ensure Java 17 is installed: `brew install openjdk@17`
- Set `JAVA_HOME` environment variable to Java 17:
  ```bash
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
  ```
- Verify Java version being used:
  ```bash
  echo $JAVA_HOME
  $JAVA_HOME/bin/java -version  # Should show version 17
  ```
- PySpark 4.x requires Java 17 or earlier (not compatible with Java 18+)

### Memory Issues

If Spark runs out of memory:

- Increase driver memory in `compute_pagerank.py`:
  ```python
  .config("spark.driver.memory", "4g")  # Increase from 2g to 4g
  ```

## Configuration

### Environment Variables

| Variable              | Required | Default | Description                                |
| --------------------- | -------- | ------- | ------------------------------------------ |
| `JAVA_HOME`           | Yes      | -       | Path to Java 17 installation (for PySpark) |
| `ALTERTABLE_USERNAME` | Yes      | -       | Altertable username                        |
| `ALTERTABLE_PASSWORD` | Yes      | -       | Altertable password                        |
| `ALTERTABLE_CATALOG`  | Yes      | -       | Catalog name                               |

### PageRank Parameters

Modify these in `compute_pagerank.py`:

- `num_iterations`: Number of PageRank iterations (default: 10)
- `damping_factor`: Probability of following links (default: 0.85)
- `spark.driver.memory`: Spark driver memory (default: 2g)

### Data Generation Parameters

Modify these in `setup_pagerank_data.py`:

- `num_pages`: Total number of pages (default: 1,000)
- `min_links`: Minimum outbound links per page (default: 5)
- `max_links`: Maximum outbound links per page (default: 20)
