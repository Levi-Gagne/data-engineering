# econometric_data/validation_tests.py



import os
import json
import pandas as pd
from IPython.display import display, HTML

from econometric_data.colorConfig import C

# ✅ Load selected variables from JSON file
def load_selected_variables(json_file="econometric_data/selected_variables.json"):
    with open(json_file, 'r') as f:
        return json.load(f)

# ✅ Validation for Linear Regression
def validate_linear_data(selected_data):
    """
    Validate Linear Regression data.
    Rules:
    - Y Variable must be continuous
    - X Variables must be continuous
    - No additional inputs required
    """
    try:
        display(HTML(
            f"<b>🔎 <span style='color:#4A90E2;'>Validating Linear Regression Data</span></b>"
        ))
        print(f"{C.b}{C.moccasin}Rules:{C.r}")
        print("  - Y Variable must be continuous.")
        print("  - X Variables must be continuous.")
        print("  - No additional inputs required.\n")

        validate_y_variable(selected_data, is_continuous=True)
        validate_x_variables(selected_data, is_continuous=True)

        print("✅ Linear Regression Data Validation Completed.\n")
    except Exception as e:
        handle_error(e)

# ✅ Validation for Logistic Regression
def validate_logistic_data(selected_data):
    """
    Validate Logistic Regression data.
    Rules:
    - Y Variable must be binary (0/1)
    - X Variables can be categorical or continuous
    - No additional inputs required
    """
    try:
        display(HTML(
            f"<b>🔎 <span style='color:#4A90E2;'>Validating Logistic Regression Data</span></b>"
        ))
        print(f"{C.b}{C.moccasin}Rules:{C.r}")
        print("  - Y Variable must be binary (0/1).")
        print("  - X Variables can be categorical or continuous.")
        print("  - No additional inputs required.\n")

        validate_y_variable(selected_data, is_binary=True)
        validate_x_variables(selected_data)

        print("✅ Logistic Regression Data Validation Completed.\n")
    except Exception as e:
        handle_error(e)

# ✅ Validation for Lasso Regression
def validate_lasso_data(selected_data):
    """
    Validate Lasso Regression data.
    Rules:
    - Y Variable must be continuous
    - X Variables can be continuous or one-hot encoded categorical
    - Requires alpha (regularization strength)
    """
    try:
        display(HTML(
            f"<b>🔎 <span style='color:#4A90E2;'>Validating Lasso Regression Data</span></b>"
        ))
        print("Rules:")
        print("  - Y Variable must be continuous.")
        print("  - X Variables can be continuous or one-hot encoded categorical.")
        print("  - Requires alpha (regularization strength).\n")

        validate_y_variable(selected_data, is_continuous=True)
        validate_x_variables(selected_data)
        validate_alpha(selected_data)

        print("✅ Lasso Regression Data Validation Completed.\n")
    except Exception as e:
        handle_error(e)

# ✅ Placeholder for unimplemented models
def placeholder_validation(selected_data, model_name):
    """
    Inform the user that validation for the given model is not yet implemented.
    """
    print(f"🚧 Sorry, validation for {model_name} is not yet implemented. Stay tuned!")

# ✅ General validation for Y Variable
def validate_y_variable(selected_data, is_continuous=False, is_binary=False):
    y_info = selected_data["y"]
    y_file_path = y_info["file_path"]
    y_column = y_info["variable"]

    y_df = pd.read_csv(y_file_path)
    y_missing_values = y_df[y_column].isnull().sum()
    y_unique_values = y_df[y_column].nunique()

    print(f"📊 Y Variable: {y_column}")
    print(f"  - Missing Values: {y_missing_values}")
    print(f"  - Unique Values: {y_unique_values}\n")

    if y_missing_values > 0:
        print(f"❌ Warning: Y Variable {y_column} has {y_missing_values} missing values.")

    if is_continuous and not pd.api.types.is_numeric_dtype(y_df[y_column]):
        print(f"❌ Warning: Y Variable {y_column} must be continuous.")
    if is_binary and y_unique_values != 2:
        print(f"❌ Warning: Y Variable {y_column} must be binary (0/1).")

# ✅ General validation for X Variables
def validate_x_variables(selected_data, is_continuous=False):
    for x_var in selected_data["x"]:
        x_file_path = x_var["file_path"]
        x_column = x_var["variable"]

        x_df = pd.read_csv(x_file_path)
        x_missing_values = x_df[x_column].isnull().sum()
        x_unique_values = x_df[x_column].nunique()

        print(f"📊 X Variable: {x_column}")
        print(f"  - Missing Values: {x_missing_values}")
        print(f"  - Unique Values: {x_unique_values}\n")

        if x_missing_values > 0:
            print(f"❌ Warning: X Variable {x_column} has {x_missing_values} missing values.")
        if is_continuous and not pd.api.types.is_numeric_dtype(x_df[x_column]):
            print(f"❌ Warning: X Variable {x_column} must be continuous.")

# ✅ Validation for alpha (Lasso Regression)
def validate_alpha(selected_data):
    alpha = selected_data.get("alpha", None)
    if alpha is None:
        print(f"❌ Error: Alpha (regularization strength) is required for Lasso Regression.")
    elif not isinstance(alpha, (int, float)) or alpha <= 0:
        print(f"❌ Error: Alpha must be a positive number.")
    else:
        print(f"✅ Alpha (regularization strength): {alpha} is valid.\n")

# ✅ Handle errors
def handle_error(e):
    display(HTML(
        f"<b style='color:#FF4500;'>❌ Error:</b> <span style='color:#F5F5F5;'>{str(e)}</span>"
    ))

# ✅ Main function to run validation
def run_validation():
    selected_data = load_selected_variables()
    model_type = selected_data["model"].lower()

    validation_map = {
        "linear regression": validate_linear_data,
        "logistic regression": validate_logistic_data,
        "lasso regression": validate_lasso_data,
        "random forest classification": lambda sd: placeholder_validation(sd, "Random Forest Classification"),
        "random forest regression": lambda sd: placeholder_validation(sd, "Random Forest Regression"),
        "arima": lambda sd: placeholder_validation(sd, "ARIMA"),
        "probit model": lambda sd: placeholder_validation(sd, "Probit Model"),
        "tobit model": lambda sd: placeholder_validation(sd, "Tobit Model"),
    }

    if model_type in validation_map:
        validation_map[model_type](selected_data)
    else:
        print(f"❌ Validation for model type '{model_type}' is not implemented yet.")

# ✅ Entry point for validation if needed as a script
if __name__ == "__main__":
    run_validation()
