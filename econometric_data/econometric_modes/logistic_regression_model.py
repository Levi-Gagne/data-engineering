# econometric_data/econometric_modes/logistic_regression_model.py


import os
import json
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
from econometric_data.colorConfig import C

def load_selection_data(json_file="econometric_data/date_and_model_selection.json"):
    """
    Load the JSON file containing datasets and selected model type.
    """
    with open(json_file, 'r') as f:
        return json.load(f)

def create_widgets(column_names):
    """
    Create interactive widgets for selecting Y, Categorical X, and Continuous X variables for logistic regression.
    """
    y_variable_text = widgets.HTML(value=f"<b>Selecting variables for <span style='color:#228B22'>Logistic Regression</span> model:</b>")
    dependent_variable_text = widgets.HTML(value="<b>Please Select the '<span style='color:#1E90FF'>Dependent Variable</span>' for the Regression:</b>")

    x_categorical_text = widgets.HTML(value="<b>Please Select the '<span style='color:#FF8C00'>Categorical Independent Variables</span>':</b>")
    x_continuous_text = widgets.HTML(value="<b>Please Select the '<span style='color:#FF8C00'>Continuous Independent Variables</span>':</b>")

    y_dropdown = widgets.Dropdown(
        options=column_names,
        description='Y Variable:',
    )
    x_categorical_select = widgets.SelectMultiple(
        options=column_names,
        description='Categorical X:',
    )
    x_continuous_select = widgets.SelectMultiple(
        options=column_names,
        description='Continuous X:',
    )

    variable_selection_output = widgets.Output()

    return {
        "y_variable_text": y_variable_text,
        "dependent_variable_text": dependent_variable_text,
        "x_categorical_text": x_categorical_text,
        "x_continuous_text": x_continuous_text,
        "y_dropdown": y_dropdown,
        "x_categorical_select": x_categorical_select,
        "x_continuous_select": x_continuous_select,
        "variable_selection_output": variable_selection_output
    }

def confirm_selections(b, widgets_dict, all_columns):
    """
    Confirm the selected Y, Categorical X, and Continuous X variables, and save them to a JSON file.
    """
    y_variable = widgets_dict["y_dropdown"].value
    x_categorical_variables = list(widgets_dict["x_categorical_select"].value)
    x_continuous_variables = list(widgets_dict["x_continuous_select"].value)

    variable_selection_output = widgets_dict["variable_selection_output"]

    if y_variable and (x_categorical_variables or x_continuous_variables):
        with variable_selection_output:
            clear_output()
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#32CD32;'>✅ Y Variable: <span style='color:#FF69B4'>{y_variable}</span></b>"
                )
            )
            if x_categorical_variables:
                variable_selection_output.append_display_data(
                    widgets.HTML(
                        value=f"<b style='color:#1E90FF;'>Categorical X Variables: <span style='color:#FFFFF0'>{', '.join(x_categorical_variables)}</span></b>"
                    )
                )
            if x_continuous_variables:
                variable_selection_output.append_display_data(
                    widgets.HTML(
                        value=f"<b style='color:#1E90FF;'>Continuous X Variables: <span style='color:#FFFFF0'>{', '.join(x_continuous_variables)}</span></b>"
                    )
                )

            # Create the JSON structure
            variable_data = {
                "model": "Logistic Regression",
                "y": {
                    "variable": y_variable,
                    "file_name": all_columns[y_variable]["file_name"],
                    "file_path": all_columns[y_variable]["file_path"]
                },
                "x": []
            }

            # Add categorical variables to the JSON
            for x_var in x_categorical_variables:
                variable_data["x"].append({
                    "variable": x_var,
                    "type": "categorical",
                    "file_name": all_columns[x_var]["file_name"],
                    "file_path": all_columns[x_var]["file_path"]
                })

            # Add continuous variables to the JSON
            for x_var in x_continuous_variables:
                variable_data["x"].append({
                    "variable": x_var,
                    "type": "continuous",
                    "file_name": all_columns[x_var]["file_name"],
                    "file_path": all_columns[x_var]["file_path"]
                })

            # Save to a JSON file
            output_file = "econometric_data/selected_variables.json"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w') as f:
                json.dump(variable_data, f, indent=4)

            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#FF69B4;'>Selections saved to <span style='color:#FFFFF0;'>'{output_file}'</span></b>"
                )
            )
    else:
        with variable_selection_output:
            clear_output()
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value="<b style='color:white;'>❌ Please ensure all selections are made and valid.</b>"
                )
            )

def display_widgets():
    """
    Main function to display the widgets for logistic regression variable selection.
    """
    selection_data = load_selection_data()
    datasets = selection_data["datasets"]

    all_columns = {}
    for dataset in datasets:
        file_path = dataset["path"]
        df = pd.read_csv(file_path)

        for column in df.columns:
            all_columns[column] = {
                "file_name": dataset["file_name"],
                "file_path": dataset["path"]
            }

    column_names = sorted(all_columns.keys())
    widgets_dict = create_widgets(column_names)

    confirm_button = widgets.Button(description="Submit Selections", button_style='success')
    confirm_button.on_click(lambda b: confirm_selections(b, widgets_dict, all_columns))

    display(
        widgets_dict["y_variable_text"],
        widgets_dict["dependent_variable_text"],
        widgets_dict["y_dropdown"],
        widgets_dict["x_categorical_text"],
        widgets_dict["x_categorical_select"],
        widgets_dict["x_continuous_text"],
        widgets_dict["x_continuous_select"],
        confirm_button,
        widgets_dict["variable_selection_output"]
    )
