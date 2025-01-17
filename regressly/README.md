# Model Selection and Analysis App

This project implements a flexible model selection and analysis pipeline using Jupyter widgets and Python scripts. It allows users to interactively select datasets, configure variables, and run various regression models.

## Supported Models
- Logistic Regression
- Linear Regression
- Additional models can be added as needed

## Project Structure

The project is structured as follows:

- **Notebook**: The main entry point, containing Jupyter widgets for interactivity.
- **Scripts and Configurations**: Encapsulate logic for various steps of the pipeline.
- **JSON Configurations**: Store metadata about the datasets and variable selections.

### File Descriptions

#### Python Scripts
- **`colorConfig.py`**: Manages color themes for the widgets and visualizations.
- **`model_date_selection.py`**: Contains logic for selecting models and date ranges.
- **`regression_info.py`**: Handles calculations and statistics for the chosen models.
- **`validation_tests.py`**: Validates inputs and ensures data integrity.
- **`select_model_variables.py`**: Manages variable selection for the models.
- **`upload_data.py`**: Handles dataset uploads and ingestion into the pipeline.

#### JSON Files
- **`date_and_model_selection.json`**:
  - Defines the selected model and dataset.
  - Example dataset: `logistic_regression_dataset-Social_Network_Ads.csv`.
- **`selected_variables.json`**:
  - Specifies the dependent (`y`) and independent (`x`) variables.
  - Example:
    - `y`: `Purchased`
    - `x`: `Gender`, `Age`, `EstimatedSalary`.
- **`ingested_files.json`**:
  - Stores metadata about ingested datasets, such as file paths and headers.

## Features
- Interactive dataset upload and selection.
- Flexible model selection (e.g., Logistic, Linear).
- Variable configuration for dependent and independent variables.
- Regression result visualizations.
- Modular and extensible design for additional models and features.

## Getting Started

### Prerequisites
- Python 3.x
- Jupyter Notebook
- Required libraries installed (`pip install -r requirements.txt`).

### Usage
1. Launch the notebook in JupyterLab or Jupyter Notebook.
2. Use the widgets to upload datasets and configure models.
3. View results and export data as needed.

### Example Workflow
1. Upload the dataset: `logistic_regression_dataset-Social_Network_Ads.csv`.
2. Select a model (e.g., Logistic Regression).
3. Configure the dependent and independent variables:
   - `y`: `Purchased`
   - `x`: `Gender`, `Age`, `EstimatedSalary`.
4. Run the analysis and view the outputs.

## Future Enhancements
- Add support for additional regression models (e.g., Ridge, Lasso).
- Integrate advanced data preprocessing and visualization tools.
- Develop a standalone web app for enhanced accessibility.

---

This README is a placeholder and will be updated as the project evolves.