# Big Data Validation Tool

This application is designed to efficiently validate two sets of dataframes simultaneously, making it ideal for large-scale validation tasks. It was specifically built to handle the validation of 96 reports daily, providing specialized reporting to highlight discrepancies and areas requiring attention. The tool is user-friendly and easily adaptable for user input, making it versatile for different data sources and workflows.

This repository contains three primary Python scripts that serve specific roles in data validation workflows. While not all files are currently assembled, this repository showcases key components of the application, with ongoing work to integrate the remaining elements and configurations.

---

### **1. dictionary_generator.py**

This script generates Python dictionaries from CSV files. It allows users to specify the output path, dictionary variable name, and associated metadata for the generated file. 

- **Missing Components**:
  - Configuration JSON files for dictionary generation parameters.
  - CSV files required for input.

---

### **2. qa_validation_report_reader.py**

This module reads and processes QA validation reports from Excel files. It uses a configuration dictionary to define the specific sheets, ranges, and DataFrames to extract and summarize data.

- **Missing Components**:
  - Configuration files defining the sheet names, data ranges, and headers.
  - Sample QA validation Excel files for testing and demonstration.

---

### **3. report_selection_widget.py**

This script provides an interactive widget for selecting and saving report configurations. It dynamically loads configurations for Daily Sales and QA Validation reports and allows users to save selections to JSON files.

- **Missing Components**:
  - Required JSON configuration files for both Daily Sales and QA Validation reports.
  - Dependencies for widget interaction, such as `colorConfig` and utility modules.

---

These files showcase modular design and functionality but depend on additional configuration and input files that are not included in the current repository.
