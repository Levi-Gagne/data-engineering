
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
