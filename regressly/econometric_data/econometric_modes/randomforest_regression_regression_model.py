# econometric_data/econometric_modes/randomforest_regression_regression_model.py



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

def create_widgets(column_names, date_columns):
    """
    Create interactive widgets for selecting Y, Categorical X, and Continuous X variables for Random Forest Regression.
    """
    y_variable_text = widgets.HTML(value=f"<b>Selecting variables for <span style='color:#228B22'>Random Forest Regression</span> model:</b>")
    dependent_variable_text = widgets.HTML(value="<b>Please Select the '<span style='color:#1E90FF'>Dependent Variable</span>' for Regression:</b>")

    x_categorical_text = widgets.HTML(value="<b style='color:white;'>Please Select the '<span style='color:#FF6347'>Categorical</span> Independent Variables':</b>")
    x_continuous_text = widgets.HTML(value="<b style='color:white;'>Please Select the '<span style='color:#32CD32'>Continuous</span> Independent Variables':</b>")

    # Exclude date columns from dropdown options
    selectable_columns = [col for col in column_names if col not in date_columns]

    y_dropdown = widgets.Dropdown(
        options=selectable_columns,
        description='Y Variable:',
    )
    x_categorical_select = widgets.SelectMultiple(
        options=selectable_columns,
        description='Categorical X:',
    )
    x_continuous_select = widgets.SelectMultiple(
        options=selectable_columns,
        description='Continuous X:',
    )

    # Slider for selecting the number of trees
    n_estimators_slider = widgets.IntSlider(
        value=100,
        min=10,
        max=500,
        step=10,
        description='Trees (n_estimators):',
        continuous_update=False
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
        "n_estimators_slider": n_estimators_slider,
        "variable_selection_output": variable_selection_output
    }

def confirm_selections(b, widgets_dict, all_columns, date_columns):
    """
    Confirm the selected Y, Categorical X, and Continuous X variables, and save them to a JSON file.
    """
    y_variable = widgets_dict["y_dropdown"].value
    x_categorical_variables = list(widgets_dict["x_categorical_select"].value)
    x_continuous_variables = list(widgets_dict["x_continuous_select"].value)
    n_estimators = widgets_dict["n_estimators_slider"].value

    variable_selection_output = widgets_dict["variable_selection_output"]

    # Check if Y variable is also selected as an X variable
    if y_variable in x_categorical_variables or y_variable in x_continuous_variables:
        with variable_selection_output:
            clear_output()
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:red;'>❌ Error:</b> <b style='color:white;'>The Y variable '</b><b style='color:red;'>{y_variable}</b><b style='color:white;'>'' is also selected as an X variable. Please remove it from the X selection.</b>"
                )
            )
        return

    # Check if the same variable is selected for both categorical and continuous X
    common_variables = set(x_categorical_variables).intersection(set(x_continuous_variables))
    if common_variables:
        with variable_selection_output:
            clear_output()
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:red;'>❌ Error:</b> <b style='color:white;'>The variable(s) '</b><b style='color:red;'>{', '.join(common_variables)}</b><b style='color:white;'>'' is/are selected as both categorical and continuous. Please correct your selection.</b>"
                )
            )
        return

    if y_variable and (x_categorical_variables or x_continuous_variables):
        with variable_selection_output:
            clear_output()
            # Y Variable Confirmation
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#1E90FF;'>Y Variable:</b> <span style='color:white;'>{y_variable}</span>"
                )
            )
            # Categorical Variables Confirmation
            if x_categorical_variables:
                variable_selection_output.append_display_data(
                    widgets.HTML(
                        value=f"<b style='color:#FF8C00;'>Categorical X Variables:</b> <span style='color:white;'>{', '.join(x_categorical_variables)}</span>"
                    )
                )
            # Continuous Variables Confirmation
            if x_continuous_variables:
                variable_selection_output.append_display_data(
                    widgets.HTML(
                        value=f"<b style='color:#32CD32;'>Continuous X Variables:</b> <span style='color:white;'>{', '.join(x_continuous_variables)}</span>"
                    )
                )
            # n_estimators Confirmation
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#FFD700;'>Number of Trees (n_estimators):</b> <span style='color:white;'>{n_estimators}</span>"
                )
            )

            # Create the JSON structure
            variable_data = {
                "model": "Random Forest Regression",
                "parameters": {
                    "n_estimators": n_estimators
                },
                "y": {
                    "variable": y_variable,
                    "file_name": all_columns[y_variable]["file_name"],
                    "file_path": all_columns[y_variable]["file_path"]
                },
                "x": [
                    {
                        "variable": x_var,
                        "type": "categorical" if x_var in x_categorical_variables else "continuous",
                        "file_name": all_columns[x_var]["file_name"],
                        "file_path": all_columns[x_var]["file_path"]
                    }
                    for x_var in (x_categorical_variables + x_continuous_variables)
                ]
            }

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
    Main function to display the widgets for Random Forest Regression variable selection.
    """
    selection_data = load_selection_data()
    datasets = selection_data["datasets"]

    all_columns = {}
    date_columns = set()

    for dataset in datasets:
        file_path = dataset["path"]
        date_column = dataset["date_column"]
        date_columns.add(date_column)

        df = pd.read_csv(file_path)
        for column in df.columns:
            all_columns[column] = {
                "file_name": dataset["file_name"],
                "file_path": dataset["path"]
            }

    column_names = sorted(all_columns.keys())
    widgets_dict = create_widgets(column_names, date_columns)

    confirm_button = widgets.Button(description="Submit Selections", button_style='success')
    confirm_button.on_click(lambda b: confirm_selections(b, widgets_dict, all_columns, date_columns))

    display(
        widgets_dict["y_variable_text"],
        widgets_dict["dependent_variable_text"],
        widgets_dict["y_dropdown"],
        widgets_dict["x_categorical_text"],
        widgets_dict["x_categorical_select"],
        widgets_dict["x_continuous_text"],
        widgets_dict["x_continuous_select"],
        widgets_dict["n_estimators_slider"],
        confirm_button,
        widgets_dict["variable_selection_output"]
    )