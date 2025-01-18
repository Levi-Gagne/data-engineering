# econometrics_data/upload_data.py


import os
import json
import pandas as pd
import ipywidgets as widgets
from IPython.display import display, clear_output
from econometric_data.colorConfig import C

# ---- Define the function to display the file uploader widgets ----
def display_file_uploader():
    """Function to display the file uploader widgets in a Jupyter notebook."""
    # ---- Set the initial directory to the current notebook directory ----
    current_path = os.getcwd()
    json_file = "econometric_data/ingested_files.json"

    # ---- Clear JSON File at the Start ----
    with open(json_file, 'w') as f:
        json.dump({}, f)
    print(f"JSON file {json_file} has been cleared.")

    # ---- Create Widgets ----
    back_button = widgets.Button(description="⬅️ Back", button_style='warning')
    upload_button = widgets.Button(description="Upload File", button_style='success')
    finish_button = widgets.Button(description="Finished Uploading", button_style='primary')
    output = widgets.Output()

    selected_file = None

    # ---- Update Directory Contents ----
    def update_directory_contents():
        """Update the directory listing."""
        with output:
            clear_output()
            print(f"Current Directory: {current_path}")

            items = os.listdir(current_path)
            items.sort()

            buttons = []
            for item in items:
                full_path = os.path.join(current_path, item)
                if os.path.isdir(full_path):
                    button = widgets.Button(description=f"📁 {item}", button_style='info')
                    button.on_click(lambda b, p=full_path: navigate_to_directory(p))
                elif item.endswith('.csv'):
                    button = widgets.Button(description=f"📄 {item}", button_style='primary')
                    button.on_click(lambda b, p=full_path: select_csv_file(p))
                else:
                    continue
                buttons.append(button)

            display(back_button)
            for btn in buttons:
                display(btn)

    # ---- Navigate to Directory ----
    def navigate_to_directory(path):
        """Navigate to the selected directory."""
        nonlocal current_path
        current_path = path
        update_directory_contents()

    # ---- Go Back a Directory ----
    def go_back(b):
        """Go up one directory."""
        nonlocal current_path
        current_path = os.path.dirname(current_path)
        update_directory_contents()

    # ---- Select a CSV File ----
    def select_csv_file(path):
        """Select a CSV file to upload."""
        nonlocal selected_file
        selected_file = path
        with output:
            clear_output()
            print(f"Selected File: {selected_file}")
            display(back_button)
            print("Click 'Upload File' to add this file to ingested_files.json.")

    # ---- Upload the Selected File ----
    def upload_selected_file(b):
        """Add the selected file to the JSON file."""
        nonlocal selected_file

        if selected_file and selected_file.endswith('.csv'):
            df = pd.read_csv(selected_file, nrows=0)
            headers = df.columns.tolist()

            try:
                with open(json_file, 'r') as f:
                    files = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                files = {}

            file_name = os.path.basename(selected_file)
            files[file_name] = {
                "path": selected_file,
                "headers": headers
            }

            with open(json_file, 'w') as f:
                json.dump(files, f, indent=4)

            json_file_path = os.path.abspath(json_file)
            with output:
                clear_output()
                print(f"Uploaded {file_name} to {json_file_path} with path and headers:")
                print(json.dumps(files, indent=4))

            selected_file = None
            update_directory_contents()

    # ---- Handle Finish Button Click ----
    def finish_uploading(b):
        """Display and update the JSON file content after uploading."""
        try:
            with open(json_file, 'r') as f:
                files = json.load(f)
            with output:
                clear_output()
                print(f"{C.b}{C.forest_green}Finished uploading files.{C.r}")
                print(json.dumps(files, indent=4))
        except FileNotFoundError:
            with output:
                clear_output()
                print("JSON file not found.")

    # ---- Bind Buttons to Functions ----
    back_button.on_click(go_back)
    upload_button.on_click(upload_selected_file)
    finish_button.on_click(finish_uploading)

    # ---- Initialize and Display ----
    update_directory_contents()
    display(output, upload_button, finish_button)
