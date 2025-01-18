# econometric_data/econometric_modes/__init__.py

import json
import importlib

def display_model_selection(json_file="econometric_data/date_and_model_selection.json"):
    """
    Dynamically load and call the correct model's display_widgets function based on the model type in the JSON file.
    """
    try:
        # Read the JSON file to determine the model type
        with open(json_file, 'r') as f:
            selected_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{json_file}' was not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: The file '{json_file}' contains invalid JSON.")
        return

    # Get the model type from the JSON
    model_type = selected_data.get("model", "").lower()

    # Map model types to their respective module paths
    model_map = {
        "linear regression": "econometric_data.econometric_modes.linear_regression_model",
        "logistic regression": "econometric_data.econometric_modes.logistic_regression_model",
        "lasso regression": "econometric_data.econometric_modes.lasso_regression_model",
        "random forest classification": "econometric_data.econometric_modes.randomforest_classification_regression_model",
        "random forest regression": "econometric_data.econometric_modes.randomforest_regression_regression_model",
    }

    # Check if the model type is supported
    if model_type in model_map:
        try:
            # Dynamically import the correct model file
            model_module = importlib.import_module(model_map[model_type])
            # Call the display_widgets function from the imported module
            model_module.display_widgets()
        except ImportError:
            print(f"Error: The module for '{model_type}' could not be imported.")
        except AttributeError:
            print(f"Error: The module for '{model_type}' does not have a 'display_widgets' function.")
    else:
        print(f"Error: Unsupported model type '{model_type}'. Please select a valid model.")