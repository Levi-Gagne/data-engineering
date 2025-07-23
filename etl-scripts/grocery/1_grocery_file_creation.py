"""
POC Data Pipeline: Daily Random Grocery Supply Data Generation

Overview:
---------
This script is part of a proof-of-concept (POC) data pipeline. It runs once per day (on a schedule)
to create up to 10 CSV files (one file per state) in the specified Databricks volume path. 
Each file follows the pattern:
    /Volumes/dq_dev/lmg_sandbox/grocery_supply_data/{state}_grocery_supply_{YYYYMMDD}.csv

Logic:
------
1. For each state (Iowa, Wisconsin, Idaho, Washington, Georgia, Florida, Maine, Vermont, Texas, Alaska):
   a. Gather the list of supplier_names and produce_type_options from a dictionary.
   b. Create a cross product of (supplier_name × produce_type). 
      - This is the maximum possible set of rows for that state.
   c. Randomly choose how many of these pairs to include today (between 1 and the maximum possible).
   d. For each chosen pair, generate a row:
      - supplier_name
      - produce_type
      - quantity (random float within the state's configured quantity range)
      - today_date (YYYYMMDD format)
2. Save each state's data as a CSV file, named "{state}_grocery_supply_{YYYYMMDD}.csv", 
   to the folder "/Volumes/dq_dev/lmg_sandbox/grocery_supply_data".
3. The pipeline can be scheduled daily in Databricks Jobs or another orchestrator.

Columns:
--------
- supplier_name (STRING)
- produce_type (STRING)
- quantity (FLOAT)
- today_date (STRING, YYYYMMDD)

Note:
-----
This code intentionally overwrites the CSV file if it already exists.
You can modify this behavior to handle duplicates or add data-quality checks as needed.

Job Description:
----
1. Purpose

This job is part of a proof-of-concept data pipeline that produces mock grocery-supply records each day.
2. Schedule

Runs daily at 9:30 AM Central Time (UTC–5/–6 depending on DST).
Overwrites any existing file for the same date to ensure clean, up-to-date data.
3. Data Creation

Generates mock data by combining supplier names and produce types for ten states (Iowa, Wisconsin, Idaho, Washington, Georgia, Florida, Maine, Vermont, Texas, Alaska).
Randomly selects a subset of possible combinations to simulate daily variations.
4. Target Systems
    - Saves output CSV files to: 
        - /Volumes/dq_dev/lmg_sandbox/grocery_supply_data/{state}_grocery_supply_{YYYYMMDD}.csv  
"""

import sys
import random
from datetime import datetime
import pytz
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark (Databricks generally provides a pre-configured SparkSession).
spark: SparkSession = SparkSession.builder.getOrCreate()

# 1) Print environment info
print(f"[INFO] Spark Version: {spark.version}")
print(f"[INFO] Python Version: {sys.version}")

# 2) Function to get date (YYYYMMDD) based on America/Chicago time
def get_chicago_date_str() -> str:
    """
    Return today's date in YYYYMMDD format, based on America/Chicago local time.

    :return: A string representing today's date in YYYYMMDD format (Chicago time).
    """
    chicago_tz = pytz.timezone("America/Chicago")
    chicago_now = datetime.now(chicago_tz)
    return chicago_now.strftime("%Y%m%d")

# 3) Define a unified schema for the staging data
unified_schema: StructType = StructType([
    StructField("supplier_name", StringType(), True),
    StructField("produce_type", StringType(), True),
    StructField("quantity", FloatType(), True),
    StructField("today_date", StringType(), True)
])

# 4) Dictionary with each state's info: suppliers, produce, and quantity range
states_info: dict = {
    "Iowa": {
        "supplier_names": [
            "Green Acres", "Farm Fresh", "Prairie Fields", "Harvest Home",
            "Cedar Creek", "Golden Pasture", "Row & Row", "Hawkeye Hills",
            "Cornfield HQ", "Rustic Ranch"
        ],
        "produce_type_options": ["Yellow Dent", "Flint", "Sweet", "Popcorn"],
        "quantity_range": (50, 500)
    },
    "Wisconsin": {
        "supplier_names": [
            "Dairy Dream", "Cheese Co-op", "Old Country Cheese", "Lakeside Dairy",
            "Badger Cheese", "Creamy Delights", "Farmhouse Cheddar", "Melt & Munch",
            "Milwaukee Molds", "Moo & Chew"
        ],
        "produce_type_options": ["Cheddar", "Gouda", "Swiss", "Mozzarella"],
        "quantity_range": (200, 2000)
    },
    "Idaho": {
        "supplier_names": [
            "Spud Nation", "Gem State Potatoes", "Russet Ranch", "Tater Town",
            "Baked Bliss", "Golden Spud", "Boise Fields", "Rocky Tuber",
            "IdaGrow Farms", "Potato Plenty"
        ],
        "produce_type_options": ["Russet Burbank", "Yukon Gold", "Red Pontiac"],
        "quantity_range": (50, 500)
    },
    "Washington": {
        "supplier_names": [
            "Evergreen Orchard", "Rainier Apples", "Columbia Crisp", "Cascade Core",
            "Yakima Bounty", "Apple Valley", "Seattle Seeds", "Spokane Bloom",
            "Puget Produce", "Orchard Oasis"
        ],
        "produce_type_options": ["Red Delicious", "Granny Smith", "Honeycrisp"],
        "quantity_range": (1, 10)
    },
    "Georgia": {
        "supplier_names": [
            "Peach Perfect", "Southern Orchard", "Savannah Sweet", "Magnolia Harvest",
            "Peachy Keen", "Dixie Orchards", "Peach Please", "Georgia Gold",
            "Sweetwater Grove", "Chattahoochee Chomp"
        ],
        "produce_type_options": ["Elberta", "Belle of Georgia", "Redhaven"],
        "quantity_range": (50, 400)
    },
    "Florida": {
        "supplier_names": [
            "Sunshine Grove", "Citrus Coast", "Orange Oasis", "Everglade Grove",
            "Tropic Twist", "Palm Paradise", "Manatee Manor", "Mango Mesa",
            "Sarasota Citrus", "Gator Grove"
        ],
        "produce_type_options": ["Valencia", "Navel", "Blood Orange"],
        "quantity_range": (50, 400)
    },
    "Maine": {
        "supplier_names": [
            "Lobster Landing", "Crustacean Cruise", "Downeast Catch", "Mariner's Mark",
            "Bluefin Bounty", "Harbor Harvest", "Atlantic Aboard", "Portland Pots",
            "Tidal Trawlers", "Maine Morsels"
        ],
        "produce_type_options": ["Maine Lobster"],
        "quantity_range": (100, 800)
    },
    "Vermont": {
        "supplier_names": [
            "Maple Haven", "Sugarwood Farms", "Green Mountain Syrup", "Sap & Tap",
            "Sweetleaf Grove", "Vermont Gold", "Champlain Drip", "Granite State Maple",
            "Leafy Luxury", "Northeast Nectar"
        ],
        "produce_type_options": ["Grade A Amber", "Grade A Dark"],
        "quantity_range": (5, 50)
    },
    "Texas": {
        "supplier_names": [
            "Longhorn Ranch", "Alamo Acres", "Brazos Beef", "Lone Star Range",
            "Rio Grande Grazers", "Cattle Country", "High Plains Herd", "Bluebonnet Beef",
            "Mesquite Meadows", "Yellow Rose Range"
        ],
        "produce_type_options": ["Cattle", "Beef"],
        "quantity_range": (50, 500)
    },
    "Alaska": {
        "supplier_names": [
            "Northern Nets", "Arctic Anglers", "Frosty Fishing", "Aleutian Afloat",
            "Kodiak Catch", "Salmon Shoals", "Juneau Journeys", "Glacial Gillnets",
            "Polar Pursuit", "Bering Bounty"
        ],
        "produce_type_options": ["Salmon", "Wild Salmon"],
        "quantity_range": (20, 300)
    }
}

def main() -> None:
    """
    Main function of the POC pipeline:
      1) Iterates over each state in 'states_info'.
      2) Creates a cross product of (supplier_name × produce_type).
      3) Chooses a random subset of these pairs (between 1 and the maximum possible),
         ensuring no duplicates in the same file.
      4) Generates rows with:
         - supplier_name
         - produce_type
         - quantity (a random float in the state's range)
         - today_date (from Chicago time)
      5) Overwrites each state's CSV file at:
         /Volumes/dq_dev/lmg_sandbox/grocery_supply_data/{state}_grocery_supply_{YYYYMMDD}.csv

    This script is typically run once per day to produce 10 CSV files (one per state).
    Multiple runs in a day will overwrite the same file for that date.
    """

    # Directory for output
    base_path: str = "/Volumes/dq_dev/lmg_sandbox/grocery_supply_data"

    # Get date (YYYYMMDD) in Chicago time
    date_str: str = get_chicago_date_str()
    print(f"[INFO] Using date_str (Chicago Time) = {date_str}")

    created_files = []  # Track file names for final summary

    for state, info in states_info.items():
        supplier_names = info["supplier_names"]
        produce_types = info["produce_type_options"]
        min_q, max_q = info["quantity_range"]

        # Build the full cross product: all possible (supplier_name, produce_type)
        cross_product = []
        for supp_name in supplier_names:
            for prod_type in produce_types:
                cross_product.append((supp_name, prod_type))

        max_possible: int = len(cross_product)
        subset_size: int = random.randint(1, max_possible)

        # Randomly sample subset_size combos without replacement
        chosen_pairs = random.sample(cross_product, subset_size)

        # Create data rows
        data_rows = []
        for (supp_name, prod_type) in chosen_pairs:
            quantity_val = float(f"{random.uniform(min_q, max_q):.2f}")

            row_dict = {
                "supplier_name": supp_name,
                "produce_type": prod_type,
                "quantity": quantity_val,
                "today_date": date_str
            }
            data_rows.append(Row(**row_dict))

        # Convert to a Spark DataFrame using the unified_schema
        df: DataFrame = spark.createDataFrame(data_rows, schema=unified_schema)

        # Build output path
        filename: str = f"{state}_grocery_supply_{date_str}.csv"
        full_path: str = f"{base_path}/{filename}"

        # Write the CSV (overwriting for demonstration)
        df.write.option("header", "true").mode("overwrite").csv(full_path)

        print(f"[INFO] Created/Overwrote {len(data_rows)} rows for state={state} -> {filename}")
        created_files.append(filename)

    # Final summary
    print(f"\n[SUMMARY] Files Created/Overwritten: {len(created_files)}")
    for fname in created_files:
        print(f"  - {fname}")


if __name__ == "__main__":
    main()
