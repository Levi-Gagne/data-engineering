"""
================================================================================
 FILE: create_grocery_producer_dimension.py
================================================================================
PURPOSE:
  This script creates (or overwrites) a dimension table named
  'dq_dev.lmg_sandbox.grocery_producer_dimension_tbl' containing producer metadata.

OVERVIEW:
  - Pulls producer information (name, state, location) from an in-memory dictionary
    (i.e., "source location" is data created from the notebook).
  - Creates a Spark DataFrame with columns:
      1. producer_name
      2. state
      3. location
      4. producer_id  (concatenation: "producer_name|state")
  - Overwrites/creates the dimension table in Unity Catalog/Hive.

SOURCE LOCATION:
  - Data is created ad hoc in the notebook itself (no file path or external DB).
    Specifically, the in-memory "producer_location_dict" is our single source of truth.

TARGET LOCATION:
  - Databricks Delta table: dq_dev.lmg_sandbox.grocery_producer_dimension_tbl

FREQUENCY TO RUN:
  - Ad Hoc: Run only when we need to add or modify producers in the dictionary
    (i.e., not on a scheduled basis).

USAGE:
  1) Confirm that "producer_location_dict" is up to date with new or existing suppliers.
  2) Execute this script (in a Databricks notebook or other environment).
  3) The script overwrites the target dimension table.

NOTES:
  - Provide ad hoc updates to the dictionary whenever new suppliers appear.
  - The table is overwritten, so only run when you intend to replace previous data.

================================================================================
"""

import sys
import pytz
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit



producer_location_dict = {
    "Iowa": {
        "Green Acres": "42.3497,-94.7254",
        "Farm Fresh": "41.8528,-93.2314",
        "Prairie Fields": "42.1102,-93.9121",
        "Harvest Home": "40.9523,-91.8776",
        "Cedar Creek": "42.8812,-95.8876",
        "Golden Pasture": "43.0213,-93.8214",
        "Row & Row": "41.6211,-95.1332",
        "Hawkeye Hills": "40.9854,-94.3512",
        "Cornfield HQ": "42.3319,-90.8842",
        "Rustic Ranch": "41.4311,-94.8632"
    },
    "Wisconsin": {
        "Dairy Dream": "44.2113,-89.3211",
        "Cheese Co-op": "46.2211,-90.2311",
        "Old Country Cheese": "44.1214,-87.4511",
        "Lakeside Dairy": "43.8922,-88.4455",
        "Badger Cheese": "45.2311,-88.9876",
        "Creamy Delights": "42.8811,-89.4512",
        "Farmhouse Cheddar": "44.1122,-86.9912",
        "Melt & Munch": "45.9122,-91.2311",
        "Milwaukee Molds": "43.0894,-87.9494",
        "Moo & Chew": "44.7221,-89.5522"
    },
    "Idaho": {
        "Spud Nation": "47.0231,-116.4421",
        "Gem State Potatoes": "43.8811,-114.9912",
        "Russet Ranch": "46.2112,-111.4422",
        "Tater Town": "44.9122,-117.0233",
        "Baked Bliss": "44.0034,-113.1345",
        "Golden Spud": "42.8311,-112.9877",
        "Boise Fields": "43.6211,-116.2112",
        "Rocky Tuber": "44.8123,-115.9022",
        "IdaGrow Farms": "42.4455,-111.3452",
        "Potato Plenty": "46.2212,-116.8914"
    },
    "Washington": {
        "Evergreen Orchard": "46.1234,-121.4321",
        "Rainier Apples": "48.1123,-122.4512",
        "Columbia Crisp": "47.5822,-120.9122",
        "Cascade Core": "45.6221,-122.8912",
        "Yakima Bounty": "46.6021,-120.5059",
        "Apple Valley": "47.1822,-119.8122",
        "Seattle Seeds": "47.6097,-122.3331",
        "Spokane Bloom": "47.6588,-117.4260",
        "Puget Produce": "47.1900,-122.3021",
        "Orchard Oasis": "46.9901,-122.8712"
    },
    "Georgia": {
        "Peach Perfect": "31.2411,-83.9911",
        "Southern Orchard": "33.1122,-84.7811",
        "Savannah Sweet": "32.0812,-81.0912",
        "Magnolia Harvest": "32.9134,-84.2922",
        "Peachy Keen": "34.3122,-84.9001",
        "Dixie Orchards": "33.2211,-83.3312",
        "Peach Please": "31.9922,-83.8854",
        "Georgia Gold": "33.9511,-84.5612",
        "Sweetwater Grove": "32.6811,-83.7212",
        "Chattahoochee Chomp": "34.2212,-85.8922"
    },
    "Florida": {
        "Sunshine Grove": "27.9011,-82.3312",
        "Citrus Coast": "29.3211,-83.7912",
        "Orange Oasis": "28.5211,-81.4512",
        "Everglade Grove": "25.9213,-81.4412",
        "Tropic Twist": "27.6012,-80.7812",
        "Palm Paradise": "26.7111,-80.2112",
        "Manatee Manor": "27.3312,-82.5312",
        "Mango Mesa": "26.3312,-81.2312",
        "Sarasota Citrus": "27.3364,-82.5307",
        "Gator Grove": "29.6516,-82.3248"
    },
    "Maine": {
        "Lobster Landing": "44.6372,-68.4584",
        "Crustacean Cruise": "44.9112,-66.9213",
        "Downeast Catch": "45.2311,-67.9912",
        "Mariner's Mark": "43.7612,-69.4555",
        "Bluefin Bounty": "44.2211,-68.4422",
        "Harbor Harvest": "44.3312,-69.7712",
        "Atlantic Aboard": "44.5812,-67.8812",
        "Portland Pots": "43.6591,-70.2568",
        "Tidal Trawlers": "44.3312,-68.8999",
        "Maine Morsels": "45.0112,-69.2212"
    },
    "Vermont": {
        "Maple Haven": "44.0012,-72.7312",
        "Sugarwood Farms": "44.5812,-73.2221",
        "Green Mountain Syrup": "43.7212,-72.6889",
        "Sap & Tap": "44.3111,-73.0122",
        "Sweetleaf Grove": "43.9912,-72.3312",
        "Vermont Gold": "44.2112,-72.7654",
        "Champlain Drip": "44.5111,-73.0099",
        "Granite State Maple": "43.9212,-72.5312",
        "Leafy Luxury": "44.1122,-72.9122",
        "Northeast Nectar": "44.7712,-72.2122"
    },
    "Texas": {
        "Longhorn Ranch": "31.6012,-98.2312",
        "Alamo Acres": "29.4212,-98.6212",
        "Brazos Beef": "31.0022,-95.3122",
        "Lone Star Range": "30.2122,-99.8922",
        "Rio Grande Grazers": "26.3412,-98.2612",
        "Cattle Country": "34.0212,-101.9212",
        "High Plains Herd": "34.6312,-100.2212",
        "Bluebonnet Beef": "32.1412,-95.8122",
        "Mesquite Meadows": "29.8122,-95.7012",
        "Yellow Rose Range": "32.5412,-97.3412"
    },
    "Alaska": {
        "Northern Nets": "60.2912,-151.3512",
        "Arctic Anglers": "66.3312,-159.0012",
        "Frosty Fishing": "61.2112,-149.9002",
        "Aleutian Afloat": "52.3412,-174.1112",
        "Kodiak Catch": "57.7912,-152.4012",
        "Salmon Shoals": "58.1212,-135.4512",
        "Juneau Journeys": "58.3012,-134.4012",
        "Glacial Gillnets": "59.5312,-139.7012",
        "Polar Pursuit": "70.5012,-148.5112",
        "Bering Bounty": "64.8312,-165.4312"
    }
}


def create_producer_dimension_table(
    spark: SparkSession,
    table_name: str,
    location_dict: dict
) -> None:
    """
    Create (or overwrite) a Delta table with columns:
      producer_name, state, location, producer_id (producer_name|state)

    :param spark: an active SparkSession
    :param table_name: the target Delta table name in the format 'catalog.schema.table'
    :param location_dict: a nested dictionary mapping state -> {producer_name: location}
    """

    if not location_dict:
        raise ValueError("[ERROR] The 'location_dict' is empty. Cannot create a dimension table.")

    # We'll build a list of rows for DataFrame creation
    rows = []
    for state, producers_map in location_dict.items():
        for producer_name, location_str in producers_map.items():
            producer_id = f"{producer_name}|{state}"
            rows.append((producer_name, state, location_str, producer_id))

    if not rows:
        raise ValueError("[ERROR] No producers found in 'location_dict'. No rows to write.")

    # Define the schema for our DataFrame
    schema = StructType([
        StructField("producer_name", StringType(), True),
        StructField("state",         StringType(), True),
        StructField("location",      StringType(), True),
        StructField("producer_id",   StringType(), True)
    ])

    # Create the DataFrame
    df = spark.createDataFrame(rows, schema)

    row_count = df.count()
    print(f"[INFO] Created DataFrame with {row_count} rows.")

    # Overwrite the dimension table
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name)
    )

    print(f"[SUCCESS] Dimension table '{table_name}' created/overwritten with {row_count} rows.")


if __name__ == "__main__":
    # Initialize Spark
    spark: SparkSession = SparkSession.builder.getOrCreate()

    # Print environment info
    print(f"[INFO] Python Version: {sys.version}")
    print(f"[INFO] Spark Version: {spark.version}")

    # Just for clarity, let's get current Chicago time
    chicago_tz = pytz.timezone("America/Chicago")
    now_str = datetime.now(chicago_tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[INFO] Script started. Chicago time: {now_str}")

    # Our target dimension table name
    # We like your naming: grocery_producer_dimension_tbl
    dimension_table = "dq_dev.lmg_sandbox.grocery_producer_dimension_tbl"

    # Create the table (overwrites if it already exists)
    try:
        create_producer_dimension_table(
            spark=spark,
            table_name=dimension_table,
            location_dict=producer_location_dict
        )
    except Exception as e:
        print(f"[ERROR] Failed to create dimension table: {str(e)}")
        sys.exit(1)

    print(f"[INFO] Script finished successfully.")
