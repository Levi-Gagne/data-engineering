# econometric_data/model_date_slection.py



import os
import json
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
from econometric_data.colorConfig import C
from econometric_data.regression_info import regression_info

# ---- Function to Display the Model and Date Selection Widgets ----
def display_model_date_selection():
    # ---- Load the JSON file with datasets ----
    json_file = "econometric_data/ingested_files.json"
    with open(json_file, 'r') as f:
        datasets = json.load(f)

    # ---- Title and Explanations ----
    print(f"{C.b}{C.ivory}📘 Please select your desired {C.r}{C.b}{C.sky_blue}regression model{C.r}{C.b}{C.ivory} and {C.r}{C.b}{C.bright_pink}frequency{C.r}{C.b}{C.ivory} below.{C.r}")
    print(f"{C.ivory}The '{C.r}{C.b}{C.forest_green}Model{C.r}{C.b}{C.ivory}' defines the type of statistical analysis you want to perform (e.g., Linear Regression).{C.r}")
    print(f"{C.ivory}The '{C.r}{C.b}{C.sky_blue}Frequency{C.r}{C.b}{C.ivory}' defines the time intervals of your datasets - {C.i}All datasets should have the same frequency.{C.r}")

    # ---- Dropdown for Model Selection ----
    model_dropdown = widgets.Dropdown(
        options=regression_info.keys(),
        description='Model:',
    )

    # Display the model description and formula when a model is selected
    model_info_output = widgets.Output()

    def show_model_info(change):
        model = change['new']
        if model:
            with model_info_output:
                clear_output()
                info = regression_info[model]
                print(f"{C.b}{C.ivory}📘 Selected Model:{C.r}{C.b}{C.royal_blue} {model}{C.r}")
                print(f"{C.ivory}{info['description']}{C.r}")
                print(f"{C.ivory}Formula: {info['formula']}{C.r}")
                print(f"\n{C.b}{C.electric_purple}Assumptions:{C.r}")
                for assumption in info["assumptions"]:
                    print(f"- {assumption}")
                print(f"\n{C.b}{C.electric_purple}Practical Applications:{C.r}")
                for application in info["practical_applications"]:
                    print(f"- {application}")

            # Dynamically adjust UI based on model type
            if model == "Logistic Regression":
                frequency_dropdown.layout.display = 'none'
                date_selection_output.clear_output()
            else:
                frequency_dropdown.layout.display = 'block'

    model_dropdown.observe(show_model_info, names='value')

    # ---- Dropdown for Frequency Selection (Only for Linear Regression) ----
    frequency_dropdown = widgets.Dropdown(
        options=["Daily", "Weekly", "Monthly", "Quarterly", "Annually"],
        description='Frequency:',
    )

    # ---- Date Column Selection (Only for Linear Regression) ----
    dropdowns = {}  # Store dropdown widgets for each file
    date_selection_output = widgets.Output()

    def show_date_column_selection():
        with date_selection_output:
            clear_output()
            print(f"\n{C.b}{C.ivory}📅 Please select the '{C.r}{C.b}{C.soft_orange}date column{C.r}{C.b}{C.ivory}' associated with each dataset from the dropdowns below.{C.r}")
            for file_name, details in datasets.items():
                file_path = details["path"]
                df = pd.read_csv(file_path)
                column_dropdown = widgets.Dropdown(
                    options=df.columns.tolist(),
                    description=f'{file_name}:',
                    value=df.columns[0],  # Default to the first column
                )
                dropdowns[file_name] = column_dropdown
                display(column_dropdown)

    # Initially show the date column selection
    show_date_column_selection()

    # ---- Button to Confirm Selections and Save to JSON ----
    button_output = widgets.Output()

    def confirm_selections(b):
        output_file = "econometric_data/date_and_model_selection.json"

        # Prepare the selection data
        date_columns = []
        for file_name, dropdown in dropdowns.items():
            date_columns.append({
                "file_name": file_name,
                "path": datasets[file_name]["path"],
                "date_column": dropdown.value
            })

        selection_data = {
            "model": model_dropdown.value,
            "frequency": frequency_dropdown.value if frequency_dropdown.layout.display != 'none' else None,
            "datasets": date_columns
        }

        try:
            # Write the updated data to the JSON file
            with open(output_file, 'w') as f:
                json.dump(selection_data, f, indent=4)

            # Display confirmation
            with button_output:
                clear_output()
                print(f"{C.b}{C.blue}🔎 Confirmed Selections:{C.r}")
                print(json.dumps(selection_data, indent=4))

        except Exception as e:
            with button_output:
                clear_output()
                print(f"{C.red}❌ Error saving selections: {str(e)}{C.r}")

    confirm_button = widgets.Button(description="Submit Selections", button_style='success')
    confirm_button.on_click(confirm_selections)

    # ---- Display all widgets ----
    display(model_dropdown, model_info_output, frequency_dropdown, date_selection_output, confirm_button, button_output)
