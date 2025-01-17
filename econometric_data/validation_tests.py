import os
import json
import pandas as pd
from datetime import datetime
from IPython.display import display, HTML

# ✅ Load selected variables from JSON file
def load_selected_variables(json_file="econometric_data/selected_variables.json"):
    with open(json_file, 'r') as f:
        return json.load(f)

# ✅ Validation function for monthly data (Linear Regression)
def validate_monthly_data(selected_data):
    """
    Validate linear regression data with time series data.
    """
    try:
        start_date = pd.to_datetime(selected_data["start_date"])
        end_date = pd.to_datetime(selected_data["end_date"])
        expected_observations = (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month) + 1

        display(HTML(
            f"<b>🔎 <span style='color:#FF6347;'>Step 1:</span> <span style='color:#F5F5F5;'>Validating Monthly Data (Linear Regression)</span></b>"
        ))
        print(f"Expected number of observations: {expected_observations}\n")

        # ✅ Validate Y Variable
        validate_y_variable(selected_data, date_filter=True, start_date=start_date, end_date=end_date)

        # ✅ Validate X Variables
        validate_x_variables(selected_data, date_filter=True, start_date=start_date, end_date=end_date)

        print("✅ Monthly Data Validation Completed.")

    except Exception as e:
        handle_error(e)

# ✅ General validation for Y Variable
def validate_y_variable(selected_data, date_filter=False, start_date=None, end_date=None):
    y_info = selected_data["y"]
    y_file_path = y_info["file_path"]
    y_column = y_info["variable"]

    y_df = pd.read_csv(y_file_path)

    # ✅ Convert the date column to datetime if date filtering is needed
    if date_filter:
        y_df[y_info["date_column"]] = pd.to_datetime(y_df[y_info["date_column"]])
        y_df = y_df[(y_df[y_info["date_column"]] >= start_date) & (y_df[y_info["date_column"]] <= end_date)]

    y_observations = len(y_df)
    y_missing_values = y_df[y_column].isnull().sum()

    print(f"📊 Y Variable: {y_column}")
    print(f"  - Total Observations: {y_observations}")
    print(f"  - Missing Values: {y_missing_values}")

    if y_missing_values > 0:
        print(f"❌ Warning: Y Variable {y_column} has {y_missing_values} missing values.")

# ✅ General validation for X Variables
def validate_x_variables(selected_data, date_filter=False, start_date=None, end_date=None):
    for x_var in selected_data["x"]:
        x_file_path = x_var["file_path"]
        x_column = x_var["variable"]

        x_df = pd.read_csv(x_file_path)

        # ✅ Convert the date column to datetime if date filtering is needed
        if date_filter:
            x_df[x_var["date_column"]] = pd.to_datetime(x_df[x_var["date_column"]])
            x_df = x_df[(x_df[x_var["date_column"]] >= start_date) & (x_df[x_var["date_column"]] <= end_date)]

        x_observations = len(x_df)
        x_missing_values = x_df[x_column].isnull().sum()
        x_unique_values = x_df[x_column].nunique()

        print(f"📊 X Variable: {x_column}")
        print(f"  - Total Observations: {x_observations}")
        print(f"  - Unique Values: {x_unique_values}")
        print(f"  - Missing Values: {x_missing_values}")

        # ✅ Determine the type of the variable
        var_type = x_var.get("type", "continuous")

        if x_missing_values > 0:
            print(f"❌ Warning: X Variable {x_column} has {x_missing_values} missing values.")

        if var_type == "categorical":
            if x_unique_values != 2:
                print(f"❌ Warning: X Variable {x_column} is categorical but has {x_unique_values} unique values. It must have exactly 2 unique values.")
            else:
                print(f"✅ X Variable {x_column} is properly binary.\n")
        else:
            print(f"✅ X Variable {x_column} is continuous.\n")


# ✅ Handle errors
def handle_error(e):
    display(HTML(
        f"<b style='color:#FF4500;'>❌ Error:</b> <span style='color:#F5F5F5;'>{str(e)}</span>"
    ))

# ✅ Main function to run validation
def run_validation():
    selected_data = load_selected_variables()
    model_type = selected_data["model"].lower()

    if model_type == "linear regression":
        validate_monthly_data(selected_data)
    elif model_type == "logistic regression":
        validate_logistic_data(selected_data)
    else:
        print(f"❌ Validation for model type '{model_type}' is not implemented yet.")

# ✅ Entry point for validation if needed as a script
if __name__ == "__main__":
    run_validation()
