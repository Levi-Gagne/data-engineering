# econometric_data/econometric_modes/linear_regression_model.py

# Old Name: regression_variables_selection.py

import os
import json
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
from datetime import datetime
from econometric_data.colorConfig import C

def load_selection_data(json_file="econometric_data/date_and_model_selection.json"):
    """
    Load the JSON file containing datasets, model type, and frequency information.
    """
    with open(json_file, 'r') as f:
        return json.load(f)

def calculate_date_ranges(datasets):
    """
    Calculate the minimum and maximum date ranges for each dataset.
    Returns a dictionary with date range information for each dataset.
    """
    date_ranges = {}
    for dataset in datasets:
        file_path = dataset["path"]
        date_column = dataset["date_column"]

        # Check if file exists
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            continue

        # Read the file and check for valid date column
        df = pd.read_csv(file_path)
        if date_column not in df.columns:
            print(f"❌ Date column '{date_column}' not found in file: {file_path}")
            continue

        # Convert date column to datetime and handle errors
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        df = df.dropna(subset=[date_column])

        # Skip if the dataframe is empty after dropping NaT values
        if df.empty:
            print(f"❌ No valid dates found in column '{date_column}' for file: {file_path}")
            continue

        # Sort and calculate min/max dates
        df = df.sort_values(by=date_column)
        min_date = df[date_column].min()
        max_date = df[date_column].max()

        date_ranges[dataset["file_name"]] = {
            "file_path": file_path,
            "date_column": date_column,
            "min_date": min_date,
            "max_date": max_date,
            "columns": [col for col in df.columns if col != date_column]
        }

    if not date_ranges:
        raise ValueError("❌ No valid datasets found. Please check your input files.")

    return date_ranges


def run_regression():
    """
    Load the selected variables and run the regression model.
    """
    with open("econometric_data/selected_variables.json", 'r') as f:
        selected_data = json.load(f)

    model_type = selected_data["model"]
    y_info = selected_data["y"]
    x_info = selected_data["x"]
    start_date = pd.to_datetime(selected_data["start_date"])
    end_date = pd.to_datetime(selected_data["end_date"])

    y_df = pd.read_csv(y_info["file_path"], parse_dates=[y_info["date_column"]])
    y_df.set_index(y_info["date_column"], inplace=True)
    y_df = y_df.loc[start_date:end_date]
    y_column = y_info["variable"]
    y_data = y_df[[y_column]]

    x_data = pd.DataFrame()
    for x_var in x_info:
        x_df = pd.read_csv(x_var["file_path"], parse_dates=[x_var["date_column"]])
        x_df.set_index(x_var["date_column"], inplace=True)
        x_df = x_df.loc[start_date:end_date]
        x_column = x_var["variable"]
        x_data[x_column] = x_df[x_column]

    if not y_data.index.equals(x_data.index):
        raise ValueError("❌ The indices for the dependent and independent variables are not aligned. Please check your date range.")

    if model_type.lower() == "linear regression":
        x_data = sm.add_constant(x_data)
        model = sm.OLS(y_data, x_data).fit()
        print(model.summary())
    else:
        print(f"❌ Unsupported model type: {model_type}")

def create_widgets(model_type, frequency, overall_min_date, overall_max_date, column_names):
    """
    Create interactive widgets for selecting date ranges and variables based on the given parameters.
    Returns a dictionary of widgets.
    """
    y_variable_text = widgets.HTML(value=f"<b>Selecting variables for <span style='color:#228B22'>{model_type}</span> model:</b>")
    dependent_variable_text = widgets.HTML(value="<b>Please Select the '<span style='color:#1E90FF'>Dependent Variable</span>' for the Regression:</b>")
    x_variable_text = widgets.HTML(value="<b>Please Select the '<span style='color:#FF8C00'>Independent Variables</span>' for the Regression:</b>")

    date_range_text = widgets.HTML(
        value=f"<b>The data exists in this range: </b>"
              f"<span style='color:#FF69B4;'>{overall_min_date.date()}</span> "
              f"<b>to</b> "
              f"<span style='color:#FF69B4;'>{overall_max_date.date()}</span>"
    )




    if frequency.lower() == "monthly":
        start_month_dropdown = widgets.Dropdown(
            options=[datetime(2000, month, 1).strftime('%b') for month in range(1, 13)],
            value=overall_min_date.strftime('%b'),
            description='Start Month: '
        )
        end_month_dropdown = widgets.Dropdown(
            options=[datetime(2000, month, 1).strftime('%b') for month in range(1, 13)],
            value=overall_max_date.strftime('%b'),
            description='End Month: '
        )
        start_year_dropdown = widgets.Dropdown(
            options=[str(year) for year in range(overall_min_date.year, overall_max_date.year + 1)],
            value=str(overall_min_date.year),
            description='Start Year: '
        )
        end_year_dropdown = widgets.Dropdown(
            options=[str(year) for year in range(overall_min_date.year, overall_max_date.year + 1)],
            value=str(overall_max_date.year),
            description='End Year: '
        )
    elif frequency.lower() == "annually":
        start_year_dropdown = widgets.Dropdown(
            options=[str(year) for year in range(overall_min_date.year, overall_max_date.year + 1)],
            value=str(overall_min_date.year),
            description='Start Year: '
        )
        end_year_dropdown = widgets.Dropdown(
            options=[str(year) for year in range(overall_min_date.year, overall_max_date.year + 1)],
            value=str(overall_max_date.year),
            description='End Year: '
        )

    y_dropdown = widgets.Dropdown(
        options=column_names,
        description='Y Variable:',
    )
    x_select = widgets.SelectMultiple(
        options=column_names,
        description='X Variables:',
    )

    variable_selection_output = widgets.Output()
    return {
        "y_variable_text": y_variable_text,
        "dependent_variable_text": dependent_variable_text,
        "x_variable_text": x_variable_text,
        "date_range_text": date_range_text,
        "start_month_dropdown": start_month_dropdown if frequency.lower() == "monthly" else None,
        "end_month_dropdown": end_month_dropdown if frequency.lower() == "monthly" else None,
        "start_year_dropdown": start_year_dropdown,
        "end_year_dropdown": end_year_dropdown,
        "y_dropdown": y_dropdown,
        "x_select": x_select,
        "variable_selection_output": variable_selection_output
    }

def confirm_selections(b, widgets_dict, frequency, model_type, all_columns, overall_min_date, overall_max_date):
    start_date = end_date = None
    variable_selection_output = widgets_dict["variable_selection_output"]

    if frequency.lower() == "monthly":
        start_month = widgets_dict["start_month_dropdown"].value
        start_year = widgets_dict["start_year_dropdown"].value
        end_month = widgets_dict["end_month_dropdown"].value
        end_year = widgets_dict["end_year_dropdown"].value

        if start_month and start_year and end_month and end_year:
            start_date = f"{start_year}-{datetime.strptime(start_month, '%b').month:02d}-01"
            end_date = f"{end_year}-{datetime.strptime(end_month, '%b').month:02d}-01"
        else:
            with variable_selection_output:
                clear_output()
                print("❌ Please select all start and end month/year values.")
            return
    elif frequency.lower() == "annually":
        start_year = widgets_dict["start_year_dropdown"].value
        end_year = widgets_dict["end_year_dropdown"].value

        if start_year and end_year:
            start_date = f"{start_year}-01-01"
            end_date = f"{end_year}-12-31"
        else:
            with variable_selection_output:
                clear_output()
                print("❌ Please select both start and end year values.")
            return

    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    if start_date < overall_min_date or end_date > overall_max_date:
        with variable_selection_output:
            clear_output()
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:red;'>❌ Chosen date out of range! Please select a date within this range: {overall_min_date.date()} to {overall_max_date.date()}</b>"
                )
            )
        return

    y_variable = widgets_dict["y_dropdown"].value
    x_variables = list(widgets_dict["x_select"].value)

    if start_date and end_date and y_variable and x_variables:
        with variable_selection_output:
            clear_output()
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#32CD32;'>✅ Selected Date Range: <span style='color:#FF69B4'>{start_date.date()} to {end_date.date()}</span></b>"
                )
            )

            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#FF4500;'>Y Variable: {y_variable}</b>"
                )
            )
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#1E90FF;'>X Variables: {', '.join(x_variables)}</b>"
                )
            )

            variable_data = {
                "model": model_type,
                "frequency": frequency,
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "y": {
                    "variable": y_variable,
                    "file_name": all_columns[y_variable]["file_name"],
                    "file_path": all_columns[y_variable]["file_path"],
                    "date_column": all_columns[y_variable]["date_column"]
                },
                "x": [
                    {
                        "variable": x,
                        "file_name": all_columns[x]["file_name"],
                        "file_path": all_columns[x]["file_path"],
                        "date_column": all_columns[x]["date_column"]
                    }
                    for x in x_variables
                ]
            }

            output_file = "econometric_data/selected_variables.json"
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w') as f:
                json.dump(variable_data, f, indent=4)

            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:#FF69B4;'>Selections saved to '{output_file}'</b>"
                )
            )
    else:
        with variable_selection_output:
            clear_output()
            variable_selection_output.append_display_data(
                widgets.HTML(
                    value=f"<b style='color:white;'>❌ Please ensure all selections are made and valid.</b>"
                )
            )

def display_widgets():
    selection_data = load_selection_data()
    datasets = selection_data["datasets"]
    model_type = selection_data["model"]
    frequency = selection_data["frequency"]

    date_ranges = calculate_date_ranges(datasets)

    min_dates = [info["min_date"] for info in date_ranges.values()]
    max_dates = [info["max_date"] for info in date_ranges.values()]

    overall_min_date = max(min_dates)
    overall_max_date = min(max_dates)

    all_columns = {}
    for file_name, info in date_ranges.items():
        for column in info["columns"]:
            all_columns[column] = {
                "file_name": file_name,
                "file_path": info["file_path"],
                "date_column": info["date_column"]
            }

    column_names = sorted(all_columns.keys())
    widgets_dict = create_widgets(model_type, frequency, overall_min_date, overall_max_date, column_names)

    confirm_button = widgets.Button(description="Submit Selections", button_style='success')
    confirm_button.on_click(lambda b: confirm_selections(b, widgets_dict, frequency, model_type, all_columns, overall_min_date, overall_max_date))

    if frequency.lower() == "monthly":
        display(
            widgets_dict["date_range_text"],
            widgets_dict["y_variable_text"],
            widgets_dict["start_month_dropdown"],
            widgets_dict["start_year_dropdown"],
            widgets_dict["end_month_dropdown"],
            widgets_dict["end_year_dropdown"],
            widgets_dict["dependent_variable_text"],
            widgets_dict["y_dropdown"],
            widgets_dict["x_variable_text"],
            widgets_dict["x_select"],
            confirm_button,
            widgets_dict["variable_selection_output"]
        )
    elif frequency.lower() == "annually":
        display(
            widgets_dict["date_range_text"],
            widgets_dict["y_variable_text"],
            widgets_dict["start_year_dropdown"],
            widgets_dict["end_year_dropdown"],
            widgets_dict["dependent_variable_text"],
            widgets_dict["y_dropdown"],
            widgets_dict["x_variable_text"],
            widgets_dict["x_select"],
            confirm_button,
            widgets_dict["variable_selection_output"]
        )
    else:
        display(
            widgets_dict["date_range_text"],
            widgets_dict["y_variable_text"],
            widgets_dict["dependent_variable_text"],
            widgets_dict["y_dropdown"],
            widgets_dict["x_variable_text"],
            widgets_dict["x_select"],
            confirm_button,
            widgets_dict["variable_selection_output"]
        )

if __name__ == "__main__":
    display_widgets()


