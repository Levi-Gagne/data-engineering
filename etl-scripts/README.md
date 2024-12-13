# Spark ETL Examples

This repository contains a collection of PySpark-based ETL scripts designed for data ingestion, transformation, and analytics. These scripts demonstrate best practices for processing large-scale data and include examples of both batch (historical) and incremental ingestion.

## Repository Overview

This repository is structured as follows:

| **File Name**                   | **Description**                                                      |
| ------------------------------- | -------------------------------------------------------------------- |
| `OBJ-HISTORICAL.py`             | Batch processing script for historical data ingestion using PySpark. |
| `OBJ-INCREMENTAL.py`            | Incremental data ingestion script for near real-time updates.        |
| `Fleet_OOS_Stg-HIS.py`          | Historical ingestion for fleet out-of-stock staging data.            |
| `Fleet_OOS_Stg-INC.py`          | Incremental ingestion for fleet out-of-stock staging data.           |
| `CDS_FLEET_OUT_OF_STOCK_LRF.py` | Fleet out-of-stock metrics and analytics.                            |
| `CDS_KPI_LMG.py`                | KPI calculations and large-scale analytics for data insights.        |
| `CDS_RET_CRTSY_TRP_PGM_LRF.py`  | Courtesy trip program reporting and data analysis.                   |

## Features

- **Batch and Incremental ETL**: Examples include both one-time historical ingestion and ongoing incremental updates.
- **Scalability**: Scripts leverage PySpark for distributed data processing, making them suitable for large datasets.
- **Adaptability**: Easily configurable with placeholders for database connections, file paths, and custom logic.

## Setup Instructions

1. Clone this repository:

   ```bash
   git clone https://github.com/yourusername/spark-etl-examples.git
   ```

2. Install dependencies:

   - Python 3.x
   - PySpark:
     ```bash
     pip install pyspark
     ```

3. Update placeholders in the scripts to match your environment:

   - Replace `DATABASE_NAME`, `TABLE_NAME`, `S3_BUCKET`, etc., with your specific values.

## Usage

### Running Batch (Historical) Scripts

Example: `OBJ-HISTORICAL.py`

1. Configure the script by updating:
   - Source paths for historical data.
   - Target paths or database tables for output.
2. Run the script:
   ```bash
   spark-submit OBJ-HISTORICAL.py
   ```

### Running Incremental Scripts

Example: `OBJ-INCREMENTAL.py`

1. Configure the script for incremental ingestion by updating:
   - Source, checkpoint, and target paths.
2. Execute the script:
   ```bash
   spark-submit OBJ-INCREMENTAL.py
   ```

### Analytics and Reporting Scripts

Example: `CDS_KPI_LMG.py`

1. Configure the input data sources and output destinations.
2. Run the script:
   ```bash
   spark-submit CDS_KPI_LMG.py
   ```

## File-Specific Notes

- **`OBJ-HISTORICAL.py`**: Demonstrates robust error handling for large-scale batch ingestion.
- **`OBJ-INCREMENTAL.py`**: Includes logic for checkpointing to ensure fault tolerance in incremental updates.
- **`Fleet_OOS_Stg-HIS.py` and `Fleet_OOS_Stg-INC.py`**: Focus on fleet out-of-stock data with specific staging logic.
- **`CDS_FLEET_OUT_OF_STOCK_LRF.py`**: Provides metrics and analytics for fleet data.
- **`CDS_RET_CRTSY_TRP_PGM_LRF.py`**: Handles program-specific reporting for courtesy trips.

## Placeholder Details

To ensure sensitive information is protected, replace placeholders in the scripts with your environment-specific values:

- `DATABASE_NAME`: Replace with your database name.
- `TABLE_NAME`: Replace with your table name.
- `S3_BUCKET`: Replace with your S3 bucket name.
- `API_KEY`: Replace with your API key if applicable.

## Contributions

Contributions are welcome! If you have additional scripts or improvements, feel free to submit a pull request or open an issue.

## License

This repository is licensed under the MIT License. See `LICENSE` for more details.
