# **Regressly: Model Selection and Analysis App**

_Regressly_ is a flexible and interactive tool for model selection and analysis. It enables users to upload datasets, configure variables, and run various regression models through an intuitive interface built with Jupyter widgets and Python scripts. This modular design allows for easy customization and extension.

---

## **Supported Models**
- Linear Regression
- Logistic Regression
- Lasso Regression
- Random Forest (Classification & Regression)
- Tobit Model
- Probit Model
- Additional models can be added as needed.

---

## **Project Structure**

### **Main Components**
- **Notebook**: The main entry point, containing interactive Jupyter widgets for data uploads, model selection, and variable configuration.
- **Python Scripts**: Encapsulate logic for various steps of the pipeline.
- **JSON Files**: Store metadata about datasets and variable selections for consistent workflows.

### **File Descriptions**

#### **Python Scripts**
- **`colorConfig.py`**: Manages color themes and provides enhanced terminal feedback.
- **`mock_data_generator.py`**: Generates large synthetic datasets for testing and validation.
- **`model_date_selection.py`**: Handles regression model and date range selection workflows.
- **`regression_info.py`**: Provides descriptions, formulas, and assumptions for each regression model.
- **`validation_tests.py`**: Ensures data integrity and validates inputs for analysis.
- **`select_model_variables.py`**: Manages dependent (`y`) and independent (`x`) variable configuration.
- **`upload_data.py`**: Facilitates dataset upload and ingestion into the pipeline.

#### **JSON Files**
- **`ingested_files.json`**: Stores metadata about ingested datasets, including file paths and headers.
- **`date_and_model_selection.json`**: Records the selected model, dataset, and date ranges.
- **`selected_variables.json`**: Specifies variable configurations, such as:
  - Dependent (`y`) variable: e.g., `Purchased`
  - Independent (`x`) variables: e.g., `Gender`, `Age`, `EstimatedSalary`.

---

## **Features**
- 📂 **Interactive Dataset Management**: Easily upload and explore datasets.
- 🧹 **Smart Data Cleaning**: Automatically clean and aggregate data for analysis.
- 📊 **Flexible Model Selection**: Choose from a variety of regression models to suit your analysis goals.
- ⚙️ **Variable Configuration**: Seamlessly define dependent and independent variables.
- 🚀 **Instant Results**: Quickly execute models and visualize results, including key metrics and insights.
- 🛠️ **Modular Design**: Extend the tool with additional models and features.

---

## **Getting Started**

### **Prerequisites**
- Python 3.x
- Jupyter Notebook or JupyterLab
- Required libraries installed:
```bash
pip install pandas ipywidgets statsmodels matplotlib seaborn scikit-learn ipython

## Clone the Repository

```bash
git clone https://github.com/Levi-Gagne/data-engineering.git
cd regressly
```

## Usage

1. Launch the notebook in JupyterLab or Jupyter Notebook.
2. Use the provided widgets to upload datasets, configure models, and define variables.
3. Run the regression models and view results in real-time.

---

## Example Workflow

### 1. Data Upload

Upload datasets directly through the provided widget interface:

```python
from econometric_data.upload_data import display_file_uploader
display_file_uploader()
```

### 2. Model and Date Selection

Select the desired regression model and configure time-related variables:

```python
from econometric_data.model_date_selection import display_model_date_selection
display_model_date_selection()
```

### 3. Configure Variables and Run Regression

Define dependent and independent variables, then execute the model:

```python
from econometric_data.select_model_variables import display_model_variables, run_regression_model
display_model_variables()
run_regression_model()
```

### 4. Generate Synthetic Data

Create synthetic datasets for testing and validation:

```python
from econometric_data.mock_data_generator import generate_dataset, display_file_size

# Generate a synthetic dataset
generate_dataset(
    n_rows=1_000_000,
    start_date="2000-01-01",
    end_date="2022-12-31",
    file_name="synthetic_dataset.csv"
)

# Display the size of the generated file
display_file_size("synthetic_dataset.csv")
```

---

## Future Enhancements

- **Additional Models**: Include Ridge Regression, ElasticNet, and other advanced techniques.
- **Improved Visualizations**: Add support for interactive charts and model diagnostics.
- **Web Application**: Develop a standalone web app for a more accessible and user-friendly experience.
- **Advanced Preprocessing**: Integrate tools for handling missing values, outliers, and feature engineering.

---

## Contributing

Contributions are welcome! Here's how you can help:

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature-name`.
3. Commit your changes: `git commit -m 'Add feature'`.
4. Push to the branch: `git push origin feature-name`.
5. Submit a pull request.
