# econometric_data/econometric_modes/run_linear_regression.py


import pandas as pd
import json
import statsmodels.api as sm
import matplotlib.pyplot as plt
from econometric_data.colorConfig import C

def load_and_prepare_linear_data():
    """
    Load and prepare the linear regression data from the selected_variables.json file.
    """
    # Load the selected variables from the JSON file
    with open('econometric_data/selected_variables.json', 'r') as f:
        selected_data = json.load(f)

    # ✅ Load the dependent variable (Y)
    y_variable = selected_data["y"]["variable"]
    y_file_path = selected_data["y"]["file_path"]

    # Load the Y data
    y_df = pd.read_csv(y_file_path, parse_dates=[selected_data["y"]["date_column"]])
    y_df.set_index(selected_data["y"]["date_column"], inplace=True)
    y_data = y_df[y_variable]

    # ✅ Initialize a DataFrame for X variables
    x_data = pd.DataFrame(index=y_data.index)

    # ✅ Iterate over all X variables
    for x_var in selected_data["x"]:
        x_variable = x_var["variable"]
        x_file_path = x_var["file_path"]

        # Load the X data
        x_df = pd.read_csv(x_file_path, parse_dates=[x_var["date_column"]])
        x_df.set_index(x_var["date_column"], inplace=True)

        # Add the X variable to the main DataFrame
        x_data[x_variable] = x_df[x_variable]

    # ✅ Drop rows with missing values
    combined_data = pd.concat([y_data, x_data], axis=1).dropna()

    # ✅ Separate Y and X after alignment
    y_data = combined_data[y_variable]
    x_data = combined_data.drop(columns=[y_variable])

    return y_data, x_data, y_variable, [x["variable"] for x in selected_data["x"]]

def run_model():
    """
    Run the linear regression using the prepared data.
    """
    # ✅ Load and prepare the data
    y_data, x_data, y_variable, x_variable_names = load_and_prepare_linear_data()

    # ✅ Print the inputs being used for the regression
    print(f"\n🔄 {C.b}{C.light_seafoam}Running a Linear Regression based on provided inputs:")
    print(f"   ✅{C.b}{C.candy_red} Y Variable:{C.r}{C.b}{C.ivory} {y_variable}")
    print(f"   ✅{C.b}{C.sky_blue} X Variables: {C.r}{C.b}{C.ivory} {', '.join(x_variable_names)}{C.r}{C.b}{C.pastel_peach}\n")

    # ✅ Add a constant for the intercept
    x_data = sm.add_constant(x_data)

    # ✅ Run the linear regression
    linear_model = sm.OLS(y_data, x_data).fit()

    # ✅ Print the summary
    print(linear_model.summary())

    # ✅ Plot the actual vs predicted values
    plot_linear_regression_results(y_data, linear_model.predict(x_data))

def plot_linear_regression_results(y_data, y_pred):
    """
    Plot the actual vs predicted values for linear regression.
    """
    plt.figure(figsize=(10, 6))
    plt.scatter(y_data, y_pred, alpha=0.6, label="Predicted Values", color="#FFA07A")
    plt.plot(y_data, y_data, color="red", linewidth=2, label="Perfect Prediction Line")
    plt.title("Linear Regression: Actual vs Predicted Values")
    plt.xlabel("Actual Values")
    plt.ylabel("Predicted Values")
    plt.legend()
    plt.grid(True)
    plt.show()
