import sys
import pytz
import random
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize Spark Session
spark: SparkSession = SparkSession.builder.getOrCreate()

def current_date_variables() -> Tuple[str, str, datetime]:
    """
    Sets Chicago timezone date variables and logs runtime info.
    
    Returns:
        today_str_chicago: Current date in YYYYMMDD format (for internal use).
        formatted_date: Current date in YYYY-MM-DD format (for logging).
        current_dt: Current datetime (used as simulation reference).
    """
    chicago_tz = pytz.timezone("America/Chicago")
    current_dt = datetime.now(chicago_tz)
    today_str_chicago = current_dt.strftime("%Y%m%d")
    formatted_date = current_dt.strftime("%Y-%m-%d")
    time_stamp = current_dt.strftime("%H:%M:%S %Z")
    
    log_info = lambda: print(
        f"[INFO] Current Date: {formatted_date}, Runtime Start: {time_stamp}\n"
        f"[INFO] Python Version: {sys.version}\n"
        f"[INFO] Spark Version: {spark.version}"
    )
    log_info()
    
    return today_str_chicago, formatted_date, current_dt

# Define schema for the target Delta table with an additional 'unit' column
csv_schema: StructType = StructType([
    StructField("commodity", StringType(), False),
    StructField("price", FloatType(), False),
    StructField("unit", StringType(), False),
    StructField("date", StringType(), False)
])

# Market Price Dictionary with 10 commodities (including Lobster)
market_price_dict: Dict[str, Dict[str, Any]] = {
    "Corn": {
        "unit": "bushel",
        "average_price": 5.00,
        "min_price": 3.11,
        "max_price": 8.16,
        "trend": "steady",
        "conversion_to_pound": 56  # 1 bushel ≈ 56 lbs
    },
    "Oranges": {
        "unit": "pound",
        "average_price": 1.40,
        "min_price": 1.14,
        "max_price": 1.56,
        "trend": "seasonal",
        "conversion_to_pound": 1
    },
    "Cheese": {
        "unit": "pound",
        "average_price": 5.00,
        "min_price": 4.00,
        "max_price": 6.00,
        "trend": "steady",
        "conversion_to_pound": 1
    },
    "Cattle": {
        "unit": "hundredweight",
        "average_price": 140.00,
        "min_price": 100.00,
        "max_price": 180.44,
        "trend": "increasing",
        "conversion_to_pound": 100  # 1 hundredweight = 100 lbs
    },
    "Peaches": {
        "unit": "pound",
        "average_price": 1.50,
        "min_price": 1.00,
        "max_price": 2.00,
        "trend": "seasonal",
        "conversion_to_pound": 1
    },
    "Maple Syrup": {
        "unit": "gallon",
        "average_price": 50.00,
        "min_price": 45.00,
        "max_price": 60.00,
        "trend": "steady",
        "conversion_to_pound": 11  # 1 gallon ≈ 11 lbs
    },
    "Potatoes": {
        "unit": "pound",
        "average_price": 0.80,
        "min_price": 0.55,
        "max_price": 1.00,
        "trend": "decreasing",
        "conversion_to_pound": 1
    },
    "Apples": {
        "unit": "pound",
        "average_price": 1.35,
        "min_price": 1.20,
        "max_price": 1.50,
        "trend": "steady",
        "conversion_to_pound": 1
    },
    "Salmon": {
        "unit": "pound",
        "average_price": 10.50,
        "min_price": 8.00,
        "max_price": 12.00,
        "trend": "increasing",
        "conversion_to_pound": 1
    },
    "Lobster": {
        "unit": "pound",
        "average_price": 12.00,
        "min_price": 10.00,
        "max_price": 15.00,
        "trend": "seasonal",
        "conversion_to_pound": 1
    }
}

def simulate_price_series_csv(commodity: str, price_info: dict, base_date: datetime,
                              today_index: int = 274, total_days: int = 365) -> List[Tuple[str, float, str, str]]:
    """
    Simulates a 365-day price series for a given commodity.
    
    'Today' is set as day 'today_index' (default=274). For each day, computes a price based on the trend:
      - "steady": Small random fluctuations around the average.
      - "increasing": Interpolates from min_price (day 1) to average (day today_index),
                      then from average to max_price (day 365), plus noise.
      - "decreasing": Interpolates in reverse.
      - "seasonal": Uses a sine function to simulate seasonal oscillations.
    
    Args:
        commodity: Commodity name.
        price_info: Dictionary with price parameters and trend.
        base_date: The simulation start date (day 1).
        today_index: Simulation day corresponding to 'today' (default=274).
        total_days: Total days in simulation (default=365).
    
    Returns:
        A list of tuples (commodity, price, unit, date) with date in YYYY-MM-DD format.
    """
    rows: List[Tuple[str, float, str, str]] = []
    avg_price = price_info["average_price"]
    min_price = price_info["min_price"]
    max_price = price_info["max_price"]
    trend = price_info["trend"]
    unit = price_info["unit"]
    
    for day in range(1, total_days + 1):
        current_date = base_date + timedelta(days=(day - 1))
        date_str = current_date.strftime("%Y-%m-%d")
        
        if trend == "steady":
            noise = random.uniform(-0.03 * avg_price, 0.03 * avg_price)
            price = avg_price + noise
        elif trend == "increasing":
            if day < today_index:
                base_val = min_price + (avg_price - min_price) * ((day - 1) / (today_index - 1))
            elif day > today_index:
                base_val = avg_price + (max_price - avg_price) * ((day - today_index) / (total_days - today_index))
            else:
                base_val = avg_price
            noise = random.uniform(-0.03 * avg_price, 0.03 * avg_price)
            price = base_val + noise
        elif trend == "decreasing":
            if day < today_index:
                base_val = max_price - (max_price - avg_price) * ((day - 1) / (today_index - 1))
            elif day > today_index:
                base_val = avg_price - (avg_price - min_price) * ((day - today_index) / (total_days - today_index))
            else:
                base_val = avg_price
            noise = random.uniform(-0.03 * avg_price, 0.03 * avg_price)
            price = base_val + noise
        elif trend == "seasonal":
            amplitude = (max_price - min_price) / 2.0
            phase = -2 * math.pi * (today_index / total_days)
            base_val = avg_price + amplitude * math.sin(2 * math.pi * day / total_days + phase)
            noise = random.uniform(-0.03 * avg_price, 0.03 * avg_price)
            price = base_val + noise
        else:
            price = avg_price
        
        price = round(max(min_price, min(price, max_price)), 2)
        rows.append((commodity, price, unit, date_str))
    
    return rows

def generate_yearly_table(target_table: str) -> None:
    """
    Generates simulated price data for one full year (365 days) for each commodity and writes 
    the result to a Delta table.
    
    'Today' is set as day 274 so that the simulated price on that day is near the average.
    
    The table will have columns: commodity, price, unit, date (YYYY-MM-DD).
    
    Args:
        target_table: The target Delta table name (e.g., "dq_dev.lmg_sandbox.grocery_yearly_market_data").
    """
    try:
        today_str_chicago, formatted_date, current_dt = current_date_variables()
        today_index = 274
        total_days = 365
        # Set base_date so that day 'today_index' corresponds to current_dt
        base_date = current_dt - timedelta(days=(today_index - 1))
        print(f"[INFO] Simulating data from {base_date.strftime('%Y-%m-%d')} to {(base_date + timedelta(days=total_days - 1)).strftime('%Y-%m-%d')}")
        
        all_rows: List[Tuple[str, float, str, str]] = []
        for commodity, info in market_price_dict.items():
            try:
                rows = simulate_price_series_csv(commodity, info, base_date, today_index, total_days)
                all_rows.extend(rows)
            except Exception as e:
                print(f"[ERROR] Failed to simulate data for {commodity}: {e}", file=sys.stderr)
        
        # Create DataFrame with columns: commodity, price, unit, date
        df: DataFrame = spark.createDataFrame(all_rows, csv_schema)
        
        # Write to Delta table; overwrite if exists
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        
        print(f"[SUCCESS] Delta table '{target_table}' created with {df.count()} rows.")
        print("[INFO] Sample Data:")
        df.show(10)
    except Exception as e:
        print(f"[ERROR] Failed to generate yearly table: {e}", file=sys.stderr)

# Run the table generation; write the simulated yearly data to the target Delta table.
target_table = "dq_dev.lmg_sandbox.grocery_yearly_market_data"
generate_yearly_table(target_table)
