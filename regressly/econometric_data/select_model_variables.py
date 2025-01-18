# econometric_data/select_model_variables.py


import json
import os
import importlib

def display_model_variables():
    """
    Reads the model type from the JSON file and dynamically calls the appropriate regression model's display_widgets function.
    """
    # Load the JSON file
    json_file = "econometric_data/date_and_model_selection.json"
    if not os.path.exists(json_file):
        print("❌ JSON file not found.")
        return

    with open(json_file, 'r') as f:
        selection_data = json.load(f)

    model_type = selection_data.get("model", "").lower()

    try:
        # Dynamically load the correct module
        if model_type == "linear regression":
            module = importlib.import_module("econometric_data.econometric_modes.linear_regression_model")
            module.display_widgets()
        elif model_type == "logistic regression":
            module = importlib.import_module("econometric_data.econometric_modes.logistic_regression_model")
            module.display_widgets()
        elif model_type == "lasso regression":
            module = importlib.import_module("econometric_data.econometric_modes.lasso_regression_model")
            module.display_widgets()
        elif model_type == "random forest classification":
            module = importlib.import_module("econometric_data.econometric_modes.randomforest_classification_regression_model")
            module.display_widgets()
        elif model_type == "random forest regression":
            module = importlib.import_module("econometric_data.econometric_modes.randomforest_regression_regression_model")
            module.display_widgets()
        else:
            print("❌ Unsupported model type.")
    except ImportError as e:
        print(f"❌ Error importing module: {e}")
    except AttributeError as e:
        print(f"❌ Error: Function not found in module: {e}")


def run_regression_model():
    """
    Dynamically load and run the appropriate regression model based on the JSON selection.
    """
    # Load the JSON file
    json_file = "econometric_data/date_and_model_selection.json"
    if not os.path.exists(json_file):
        print("❌ JSON file not found.")
        return

    with open(json_file, 'r') as f:
        selection_data = json.load(f)

    model_type = selection_data.get("model", "").lower()

    try:
        # Dynamically load and run the correct regression module
        if model_type == "linear regression":
            module = importlib.import_module("econometric_data.econometric_modes.run_linear_regression")
            module.run_model()
        elif model_type == "logistic regression":
            module = importlib.import_module("econometric_data.econometric_modes.run_logistic_regression")
            module.run_model()
        elif model_type == "lasso regression":
            module = importlib.import_module("econometric_data.econometric_modes.run_lasso_regression")
            module.run_model()
        elif model_type == "random forest classification":
            module = importlib.import_module("econometric_data.econometric_modes.run_randomforest_classification")
            module.run_model()
        elif model_type == "random forest regression":
            module = importlib.import_module("econometric_data.econometric_modes.run_randomforest_regression")
            module.run_model()
        else:
            print("❌ Unsupported model type.")
    except ImportError as e:
        print(f"❌ Error importing module: {e}")
    except AttributeError as e:
        print(f"❌ Error: Function not found in module: {e}")