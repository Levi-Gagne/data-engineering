# Courtesy Trip Program (CTP) ETL Scripts

This directory contains ETL scripts and related resources for processing Courtesy Trip Program (CTP) data.

## Files

- **`CDS_RET_CRTSY_TRP_PGM_LRF.py`**: PySpark script for processing CTP data and loading it into the target system.
- **`OnTRAC_STG-HIS.py`**: Processes historical OnTRAC staging data.
- **`OnTRAC_STG-INC.py`**: Processes incremental OnTRAC staging data.
- **`ret_crtsy_trp_pgm_ddl.txt`**: SQL schema for the staging table `ret_crtsy_trp_pgm_stg`.
- **`cds_ret_crtsy_trp_pgm_DDL.txt`**: SQL schema for the final table `cds_ret_crtsy_trp_pgm`.

## Workflow

1. **Staging**:
   - Process historical (`OnTRAC_STG-HIS.py`) or incremental (`OnTRAC_STG-INC.py`) data into the staging table defined in `ret_crtsy_trp_pgm_ddl.txt`.

2. **Processing**:
   - Use `CDS_RET_CRTSY_TRP_PGM_LRF.py` to transform and load data into the final table using the schema in `cds_ret_crtsy_trp_pgm_DDL.txt`.

3. **Output**:
   - Data is stored in Hive tables for further analysis and reporting.

## Prerequisites

- Python 3.7+, PySpark, and Hive access configured.
- Parquet format used for table storage.

## Authors

- Levi Gagne
