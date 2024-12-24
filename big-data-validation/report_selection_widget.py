"""
Report Selection Widget Module

This module provides a widget interface for selecting and saving configurations 
for various report sheets. It allows users to interactively choose specific 
sheets or select all sheets for generating configurations for daily sales and QA 
validation reports. 

Features:
- Load configurations for daily sales and QA reports from predefined dictionaries.
- Display an interactive dropdown widget for sheet selection.
- Save the selected configurations to JSON files.
- Clear configuration files as needed to reset the widget state.

Classes:
    - ReportSelectionWidget: Main class for handling the widget interface, 
      configurations, and interaction logic.

Usage:
    1. Import the module and initialize the `ReportSelectionWidget` class.
    2. Call `display_and_handle_widget()` to display the widget and handle 
       configuration interactions.
    3. Use the selected configuration files in downstream processing as needed.

Dependencies:
    - `os` for file path handling.
    - `json` for saving and loading configurations.
    - `ipywidgets` for interactive widgets.
    - `IPython.display` for displaying widgets in a Jupyter notebook environment.
    - `colorConfig` for styled terminal output.
    - `QAReportSelection.reportSelectionUtils` for file utility operations.

Example:
    ```python
    from ReportSelectionWidget import ReportSelectionWidget

    # Initialize and display the widget
    widget = ReportSelectionWidget(C)
    widget.display_and_handle_widget()
    ```
    
Author:
    - Levi Gagne
"""
import os
import json
import ipywidgets as widgets
from IPython.display import display, clear_output
from colorConfig import C
from QAReportSelection.reportSelectionUtils import ReportSelectionUtils

class ReportSelectionWidget:
    """
    A widget for selecting and saving configurations for various report sheets.

    Attributes:
        C: Color configuration object for styled output.
        daily_sales_config_file_path: Path to the Daily Sales configuration file.
        qa_config_file_path: Path to the QA configuration file.
        daily_sales_report_config: Configuration dictionary for Daily Sales reports.
        qa_report_config: Configuration dictionary for QA Validation reports.
    """
    def __init__(self, C):
        """
        Initializes the ReportSelectionWidget with paths and configurations.

        Args:
            C: The color configuration object.
        """
        self.C = C
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Paths to configuration files for daily sales and QA reports
        self.daily_sales_config_file_path = os.path.join(base_dir, 'reportSelectionWidget_DailySales.json')
        self.qa_config_file_path = os.path.join(base_dir, 'reportSelectionWidget_QA.json')

        # Load configurations from respective dictionaries
        self.daily_sales_report_config = self.load_dictionary('dictionary_DailySales')
        self.qa_report_config = self.load_dictionary('dictionary_QEValidation')

        # Set up the widgets for interaction
        self.setup_widgets()

    def load_dictionary(self, dictionary_name):
        """
        Loads a Python dictionary from a file.

        Args:
            dictionary_name: Name of the dictionary file (without extension).

        Returns:
            A dictionary object loaded from the file.
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_file_path = os.path.join(base_dir, f'../QADictionary/QADictionaries/{dictionary_name}.py')
        config = {}
        with open(config_file_path, 'r') as file:
            exec(file.read(), config)  # Dynamically execute file content into config
        return config

    def setup_widgets(self):
        """
        Sets up the widgets for report selection.
        """
        # Combine sheet names from both Daily Sales and QA configurations
        sheet_names = list({config['sheet_name'] for key, config in self.daily_sales_report_config['daily_sales_report_config'].items()} |
                           {config['sheet_name'] for key, config in self.qa_report_config['qe_validation_config'].items()})
        sheet_names.insert(0, 'All')  # Add "All" option at the beginning

        # Dropdown widget for selecting sheets
        self.sheet_dropdown = widgets.SelectMultiple(
            options=sheet_names,
            description='Select Sheets:',
            disabled=False,
        )

        # Button widget for submitting the selection
        self.submit_button = widgets.Button(
            description='Submit',
            disabled=False,
            button_style='',  # Default style
            tooltip='Click to select the report',
            icon='check'
        )

        # Output widget for displaying messages or results
        self.output = widgets.Output()

        # Attach click event handler to the submit button
        self.submit_button.on_click(self.on_submit_clicked)

    def display_widgets(self):
        """
        Displays the dropdown and button widgets.
        """
        display(self.sheet_dropdown, self.submit_button, self.output)

    def on_submit_clicked(self, b):
        """
        Handles the submit button click event.

        Args:
            b: Button click event.
        """
        with self.output:
            clear_output()  # Clear previous output for a fresh display

            selected_sheets = self.sheet_dropdown.value  # Get selected sheets
            if 'All' in selected_sheets:
                # Select all configurations if "All" is chosen
                self.selected_config_daily_sales = self.daily_sales_report_config['daily_sales_report_config']
                self.selected_config_qa = self.qa_report_config['qe_validation_config']
            else:
                # Filter configurations based on selected sheets
                self.selected_config_daily_sales = {key: val for key, val in self.daily_sales_report_config['daily_sales_report_config'].items() if val['sheet_name'] in selected_sheets}
                self.selected_config_qa = {key: val for key, val in self.qa_report_config['qe_validation_config'].items() if val['sheet_name'] in selected_sheets}

            # Save the selected configurations to respective files
            self.save_config_to_file(self.selected_config_daily_sales, self.daily_sales_config_file_path)
            self.save_config_to_file(self.selected_config_qa, self.qa_config_file_path)

            # Display confirmation message
            selected_sheets_str = ', '.join(selected_sheets)
            print(f"{self.C.b}{self.C.veryLightBlue}Configuration for '{self.C.oliveGreen}{selected_sheets_str}{self.C.r}{self.C.veryLightBlue}' selected. Ready to run the main function.{self.C.r}")

    def save_config_to_file(self, config, file_path):
        """
        Saves the configuration dictionary to a JSON file.

        Args:
            config: Configuration dictionary to save.
            file_path: Path to the JSON file.
        """
        # Clear the file before saving the new configuration
        ReportSelectionUtils.clear_config_file(file_path, print_message=False)
        with open(file_path, 'w') as file:
            json.dump(config, file, indent=4)

    def clear_config_file(self):
        """
        Clears the configuration files for Daily Sales and QA reports.
        """
        ReportSelectionUtils.clear_config_file(self.daily_sales_config_file_path, print_message=True)
        ReportSelectionUtils.clear_config_file(self.qa_config_file_path, print_message=True)

    def display_and_handle_widget(self):
        """
        Displays the widget and ensures configuration files are cleared at the start.
        """
        # Clear configuration files without printing messages
        ReportSelectionUtils.clear_config_file(self.daily_sales_config_file_path, print_message=False)
        ReportSelectionUtils.clear_config_file(self.qa_config_file_path, print_message=False)

        # Display the widget for user interaction
        self.display_widgets()

    @staticmethod
    def run_display_and_handle_widget():
        """
        Initializes and displays the widget for interaction.
        """
        widget = ReportSelectionWidget(C)
        widget.display_and_handle_widget()