import sys
import pytz
from datetime import datetime
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import current_timestamp

# Placeholder for the full grocery store dictionary with 100 rows.
# The dictionary structure is as follows:
# {
#    "StateName": {
#         "CityName": {"store_id_code": "XXYYY", "phone_number": "555-010-xxxx"},
#         ...
#    },
#    ...
# }
store_info_dict = {
    "Iowa": {
        "Des Moines": {"store_id_code": "IADSM", "phone_number": "555-010-1000", "location": "41.5868,-93.6250"},
        "Cedar Rapids": {"store_id_code": "IACDR", "phone_number": "555-010-1001", "location": "41.9779,-91.6656"},
        "Davenport": {"store_id_code": "IADAV", "phone_number": "555-010-1002", "location": "41.5236,-90.5776"},
        "Iowa City": {"store_id_code": "IAICY", "phone_number": "555-010-1003", "location": "41.6611,-91.5302"},
        "Council Bluffs": {"store_id_code": "IACOB", "phone_number": "555-010-1004", "location": "41.2619,-95.8608"},
        "Ames": {"store_id_code": "IAAME", "phone_number": "555-010-1005", "location": "42.0347,-93.6199"},
        "West Des Moines": {"store_id_code": "IAWDM", "phone_number": "555-010-1006", "location": "41.5771,-93.7116"},
        "Dubuque": {"store_id_code": "IADUB", "phone_number": "555-010-1007", "location": "42.5006,-90.6646"},
        "Waterloo": {"store_id_code": "IAWAT", "phone_number": "555-010-1008", "location": "42.5020,-92.3426"},
        "Sioux City": {"store_id_code": "IASIC", "phone_number": "555-010-1009", "location": "42.4990,-96.4003"}
    },
    "Wisconsin": {
        "Madison": {"store_id_code": "WIMAD", "phone_number": "555-010-1010", "location": "43.0731,-89.4012"},
        "Milwaukee": {"store_id_code": "WIMIL", "phone_number": "555-010-1011", "location": "43.0389,-87.9065"},
        "Green Bay": {"store_id_code": "WIGRB", "phone_number": "555-010-1012", "location": "44.5133,-87.9901"},
        "Kenosha": {"store_id_code": "WIKEN", "phone_number": "555-010-1013", "location": "42.5847,-87.8212"},
        "Racine": {"store_id_code": "WIRAC", "phone_number": "555-010-1014", "location": "42.7261,-87.7829"},
        "Appleton": {"store_id_code": "WIAPP", "phone_number": "555-010-1015", "location": "44.2619,-88.4154"},
        "Oshkosh": {"store_id_code": "WIOSH", "phone_number": "555-010-1016", "location": "44.0247,-88.5426"},
        "Eau Claire": {"store_id_code": "WIECL", "phone_number": "555-010-1017", "location": "44.8113,-91.4985"},
        "La Crosse": {"store_id_code": "WILAC", "phone_number": "555-010-1018", "location": "43.8014,-91.2396"},
        "Wausau": {"store_id_code": "WIWAU", "phone_number": "555-010-1019", "location": "44.9591,-89.6300"}
    },
    "Idaho": {
        "Boise": {"store_id_code": "IDBSE", "phone_number": "555-010-1020", "location": "43.6150,-116.2023"},
        "Meridian": {"store_id_code": "IDMRD", "phone_number": "555-010-1021", "location": "43.6128,-116.3910"},
        "Nampa": {"store_id_code": "IDNAM", "phone_number": "555-010-1022", "location": "43.5407,-116.5635"},
        "Idaho Falls": {"store_id_code": "IDFAL", "phone_number": "555-010-1023", "location": "43.4917,-112.0338"},
        "Pocatello": {"store_id_code": "IDPOC", "phone_number": "555-010-1024", "location": "42.8711,-112.4455"},
        "Caldwell": {"store_id_code": "IDCAL", "phone_number": "555-010-1025", "location": "43.6629,-116.6870"},
        "Coeur d'Alene": {"store_id_code": "IDCOD", "phone_number": "555-010-1026", "location": "47.6777,-116.7805"},
        "Twin Falls": {"store_id_code": "IDTWF", "phone_number": "555-010-1027", "location": "42.5610,-114.4608"},
        "Lewiston": {"store_id_code": "IDLEW", "phone_number": "555-010-1028", "location": "46.3740,-117.0153"},
        "Rexburg": {"store_id_code": "IDREX", "phone_number": "555-010-1029", "location": "43.8256,-111.7884"}
    },
    "Washington": {
        "Seattle": {"store_id_code": "WASEA", "phone_number": "555-010-1030", "location": "47.6062,-122.3321"},
        "Spokane": {"store_id_code": "WASPO", "phone_number": "555-010-1031", "location": "47.6588,-117.4260"},
        "Tacoma": {"store_id_code": "WATAC", "phone_number": "555-010-1032", "location": "47.2529,-122.4443"},
        "Vancouver": {"store_id_code": "WAVAN", "phone_number": "555-010-1033", "location": "45.6318,-122.6615"},
        "Bellevue": {"store_id_code": "WABLV", "phone_number": "555-010-1034", "location": "47.6101,-122.2015"},
        "Kent": {"store_id_code": "WAKEN", "phone_number": "555-010-1035", "location": "47.3809,-122.2348"},
        "Everett": {"store_id_code": "WAEVE", "phone_number": "555-010-1036", "location": "47.9789,-122.2021"},
        "Olympia": {"store_id_code": "WAOLY", "phone_number": "555-010-1037", "location": "47.0379,-122.9007"},
        "Bellingham": {"store_id_code": "WABLG", "phone_number": "555-010-1038", "location": "48.7491,-122.4787"},
        "Redmond": {"store_id_code": "WARED", "phone_number": "555-010-1039", "location": "47.6740,-122.1215"}
    },
    "Georgia": {
        "Atlanta": {"store_id_code": "GAATL", "phone_number": "555-010-1040", "location": "33.7490,-84.3880"},
        "Savannah": {"store_id_code": "GASAV", "phone_number": "555-010-1041", "location": "32.0809,-81.0912"},
        "Augusta": {"store_id_code": "GAAUG", "phone_number": "555-010-1042", "location": "33.4735,-82.0105"},
        "Columbus": {"store_id_code": "GACOL", "phone_number": "555-010-1043", "location": "32.4600,-84.9877"},
        "Macon": {"store_id_code": "GAMAC", "phone_number": "555-010-1044", "location": "32.8407,-83.6324"},
        "Athens": {"store_id_code": "GAATH", "phone_number": "555-010-1045", "location": "33.9519,-83.3576"},
        "Sandy Springs": {"store_id_code": "GASND", "phone_number": "555-010-1046", "location": "33.9304,-84.3733"},
        "Roswell": {"store_id_code": "GAROS", "phone_number": "555-010-1047", "location": "34.0234,-84.3493"},
        "Albany": {"store_id_code": "GAALB", "phone_number": "555-010-1048", "location": "31.5785,-84.1550"},
        "Johns Creek": {"store_id_code": "GAJCR", "phone_number": "555-010-1049", "location": "33.9716,-84.1980"}
    },
    "Florida": {
        "Miami": {"store_id_code": "FLMIA", "phone_number": "555-010-1050", "location": "25.7617,-80.1918"},
        "Orlando": {"store_id_code": "FLORL", "phone_number": "555-010-1051", "location": "28.5383,-81.3792"},
        "Tampa": {"store_id_code": "FLTAM", "phone_number": "555-010-1052", "location": "27.9506,-82.4572"},
        "Jacksonville": {"store_id_code": "FLJAX", "phone_number": "555-010-1053", "location": "30.3322,-81.6557"},
        "Tallahassee": {"store_id_code": "FLTAL", "phone_number": "555-010-1054", "location": "30.4383,-84.2807"},
        "St. Petersburg": {"store_id_code": "FLSTP", "phone_number": "555-010-1055", "location": "27.7676,-82.6403"},
        "Hialeah": {"store_id_code": "FLHIA", "phone_number": "555-010-1056", "location": "25.8576,-80.2781"},
        "Fort Lauderdale": {"store_id_code": "FLFTL", "phone_number": "555-010-1057", "location": "26.1224,-80.1373"},
        "Gainesville": {"store_id_code": "FLGNV", "phone_number": "555-010-1058", "location": "29.6516,-82.3248"},
        "Sarasota": {"store_id_code": "FLSAR", "phone_number": "555-010-1059", "location": "27.3364,-82.5307"}
    },
    "Maine": {
        "Portland": {"store_id_code": "MEPOR", "phone_number": "555-010-1060", "location": "43.6615,-70.2553"},
        "Augusta": {"store_id_code": "MEAUG", "phone_number": "555-010-1061", "location": "44.3106,-69.7795"},
        "Bangor": {"store_id_code": "MEBAN", "phone_number": "555-010-1062", "location": "44.8012,-68.7778"},
        "Lewiston": {"store_id_code": "MELEW", "phone_number": "555-010-1063", "location": "44.1000,-70.1833"},
        "South Portland": {"store_id_code": "MESTP", "phone_number": "555-010-1064", "location": "43.6615,-70.2553"},
        "Auburn": {"store_id_code": "MEAUB", "phone_number": "555-010-1065", "location": "44.0959,-70.2105"},
        "Biddeford": {"store_id_code": "MEBID", "phone_number": "555-010-1066", "location": "43.5000,-70.4500"},
        "Saco": {"store_id_code": "MESAC", "phone_number": "555-010-1067", "location": "43.5000,-70.4500"},
        "Westbrook": {"store_id_code": "MEWES", "phone_number": "555-010-1068", "location": "43.7000,-70.3500"},
        "Old Orchard Beach": {"store_id_code": "MEORB", "phone_number": "555-010-1069", "location": "43.5000,-70.4500"}
    },
    "Vermont": {
        "Burlington": {"store_id_code": "VTBRL", "phone_number": "555-010-1070", "location": "44.4759,-73.2121"},
        "Montpelier": {"store_id_code": "VTMON", "phone_number": "555-010-1071", "location": "44.2601,-72.5754"},
        "St. Albans": {"store_id_code": "VTSAL", "phone_number": "555-010-1072", "location": "44.8100,-73.1000"},
        "Rutland": {"store_id_code": "VTRUT", "phone_number": "555-010-1073", "location": "43.6100,-72.9720"},
        "Barre": {"store_id_code": "VTBAR", "phone_number": "555-010-1074", "location": "44.1985,-72.5019"},
        "Newport": {"store_id_code": "VTNEW", "phone_number": "555-010-1075", "location": "44.9413,-72.3278"},
        "Brattleboro": {"store_id_code": "VTBRB", "phone_number": "555-010-1076", "location": "42.8500,-72.5650"},
        "Vergennes": {"store_id_code": "VTVER", "phone_number": "555-010-1077", "location": "44.2368,-73.0331"},
        "Bennington": {"store_id_code": "VTBEN", "phone_number": "555-010-1078", "location": "42.8800,-73.2000"},
        "Windsor": {"store_id_code": "VTWIN", "phone_number": "555-010-1079", "location": "43.4900,-73.1100"}
    },
    "Texas": {
        "Houston": {"store_id_code": "TXHOU", "phone_number": "555-010-1080", "location": "29.7604,-95.3698"},
        "Dallas": {"store_id_code": "TXDAL", "phone_number": "555-010-1081", "location": "32.7767,-96.7970"},
        "Austin": {"store_id_code": "TXAUS", "phone_number": "555-010-1082", "location": "30.2672,-97.7431"},
        "San Antonio": {"store_id_code": "TXSAT", "phone_number": "555-010-1083", "location": "29.4241,-98.4936"},
        "Fort Worth": {"store_id_code": "TXFTW", "phone_number": "555-010-1084", "location": "32.7555,-97.3308"},
        "El Paso": {"store_id_code": "TXELP", "phone_number": "555-010-1085", "location": "31.7619,-106.4850"},
        "Arlington": {"store_id_code": "TXARN", "phone_number": "555-010-1086", "location": "32.7357,-97.1081"},
        "Corpus Christi": {"store_id_code": "TXCRC", "phone_number": "555-010-1087", "location": "27.8006,-97.3964"},
        "Plano": {"store_id_code": "TXPLA", "phone_number": "555-010-1088", "location": "33.0198,-96.6989"},
        "Lubbock": {"store_id_code": "TXLUB", "phone_number": "555-010-1089", "location": "33.5779,-101.8552"}
    },
    "Alaska": {
        "Anchorage": {"store_id_code": "AKANC", "phone_number": "555-010-1090", "location": "61.2181,-149.9003"},
        "Fairbanks": {"store_id_code": "AKFAI", "phone_number": "555-010-1091", "location": "64.8378,-147.7164"},
        "Juneau": {"store_id_code": "AKJUN", "phone_number": "555-010-1092", "location": "58.3019,-134.4197"},
        "Sitka": {"store_id_code": "AKSIT", "phone_number": "555-010-1093", "location": "57.0531,-135.3300"},
        "Ketchikan": {"store_id_code": "AKKET", "phone_number": "555-010-1094", "location": "55.3422,-131.6461"},
        "Wasilla": {"store_id_code": "AKWAS", "phone_number": "555-010-1095", "location": "61.5811,-149.4397"},
        "Kodiak": {"store_id_code": "AKKOD", "phone_number": "555-010-1096", "location": "57.7900,-152.4072"},
        "Bethel": {"store_id_code": "AKBET", "phone_number": "555-010-1097", "location": "60.7880,-161.7558"},
        "Palmer": {"store_id_code": "AKPAL", "phone_number": "555-010-1098", "location": "61.5992,-149.1100"},
        "Homer": {"store_id_code": "AKHOM", "phone_number": "555-010-1099", "location": "59.6425,-151.5483"}
    }
}

def create_store_dimension_table(spark: SparkSession, table_name: str, store_dict: dict) -> None:
    """
    Create (or overwrite) a Delta table with columns:
      store_id_code, city, state, latitude, longitude, phone_number, update_timestamp

    :param spark: an active SparkSession
    :param table_name: target Delta table in the format 'catalog.schema.table'
    :param store_dict: nested dictionary mapping state -> {city: {store_id_code, phone_number}}
    """
    if not store_dict:
        raise ValueError("[ERROR] The 'store_dict' is empty. Cannot create a dimension table.")
    
    rows = []
    # Build a list of rows from the dictionary.
    for state, cities in store_dict.items():
        for city, info in cities.items():
            # Generate random coordinates within approximate US ranges.
            latitude = round(random.uniform(25.0, 49.0), 6)
            longitude = round(random.uniform(-124.0, -67.0), 6)
            rows.append((info["store_id_code"], city, state, latitude, longitude, info["phone_number"]))
    
    if not rows:
        raise ValueError("[ERROR] No rows created from 'store_dict'.")
    
    # Define the schema for our DataFrame.
    # Note: Using StringType for store_id_code to match the letter-based code from our dictionary.
    schema = StructType([
        StructField("store_id_code", StringType(), False),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("phone_number", StringType(), True),
        StructField("update_timestamp", TimestampType(), True)
    ])
    
    print("[INFO] Creating DataFrame from store dictionary...")
    # Create DataFrame; rows do not include the timestamp yet.
    df = spark.createDataFrame(rows, schema=schema.drop("update_timestamp"))
    print(f"[INFO] DataFrame created with {df.count()} rows.")
    
    # Add update_timestamp column using Spark's current_timestamp() function.
    df = df.withColumn("update_timestamp", current_timestamp())
    print("[INFO] Added update_timestamp column with current timestamp (Chicago time).")
    
    # Check if the target table exists; if not, create it using the defined schema.
    if not spark.catalog.tableExists(table_name):
        print(f"[INFO] Target table {table_name} does not exist. Creating empty Delta table with defined schema.")
        emptyDF = spark.createDataFrame([], schema)
        emptyDF.write.format("delta").saveAsTable(table_name)
    else:
        print(f"[INFO] Target table {table_name} exists.")
    
    print(f"[INFO] Overwriting target table {table_name} with new data...")
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"[SUCCESS] Dimension table '{table_name}' has been overwritten with {df.count()} rows.")

if __name__ == "__main__":
    # Initialize SparkSession.
    spark = SparkSession.builder.getOrCreate()
    
    # Print environment info.
    print(f"[INFO] Python Version: {sys.version}")
    print(f"[INFO] Spark Version: {spark.version}")
    
    # Get current Chicago time for logging.
    chicago_tz = pytz.timezone("America/Chicago")
    now_str = datetime.now(chicago_tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[INFO] Script started. Chicago time: {now_str}")
    
    # Target dimension table name.
    dimension_table = "dq_dev.lmg_sandbox.grocery_store_dim_tbl"
    
    try:
        create_store_dimension_table(spark, dimension_table, store_info_dict)
    except Exception as e:
        print(f"[ERROR] Failed to create dimension table: {str(e)}")
        sys.exit(1)
    
    print("[INFO] Script finished successfully.")
