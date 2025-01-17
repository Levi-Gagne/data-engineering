# econometric_data/econometric_modes/run_logistic_regression.py


import pandas as pd
import json
import statsmodels.api as sm
import matplotlib.pyplot as plt
from econometric_data.colorConfig import C

def load_and_prepare_logistic_data():
    """
    Load and prepare the logistic regression data from the selected_variables.json file.
    Dynamically handle categorical variables and continuous variables from the same file.
    """
    # ✅ Load selected variables from the JSON file
    with open('econometric_data/selected_variables.json', 'r') as f:
        selected_data = json.load(f)

    # ✅ Load the CSV file once
    file_path = selected_data["y"]["file_path"]
    df = pd.read_csv(file_path)

    # ✅ Load the dependent variable (Y)
    y_variable = selected_data["y"]["variable"]
    y_data = df[y_variable].astype(int)  # Ensure binary target is numeric

    # ✅ Initialize a DataFrame for X variables
    x_data = pd.DataFrame()

    # ✅ Process independent variables (X)
    for x_var in selected_data["x"]:
        x_column = x_var["variable"]

        if x_var["type"] == "categorical":
            # ✅ Check the unique values in the categorical column
            unique_values = df[x_column].unique()

            if len(unique_values) == 2:
                # ✅ Binary encoding (0/1) for two unique values
                mapping = {unique_values[0]: 0, unique_values[1]: 1}
                x_data[x_column] = df[x_column].map(mapping)
            else:
                # ✅ One-hot encoding for more than two unique values
                x_encoded = pd.get_dummies(df[x_column], prefix=x_column, drop_first=True)
                x_data = pd.concat([x_data, x_encoded], axis=1)
        else:
            # ✅ For continuous variables, add directly to the DataFrame
            x_data[x_column] = df[x_column]

    # ✅ Drop rows with missing values
    combined_data = pd.concat([y_data, x_data], axis=1).dropna()

    # ✅ Separate Y and X after alignment
    y_data = combined_data[y_variable]
    x_data = combined_data.drop(columns=[y_variable])

    return y_data, x_data


def run_model():
    """
    Run the logistic regression using the prepared data.
    """
    # ✅ Load and prepare the data
    y_data, x_data = load_and_prepare_logistic_data()

    # ✅ Print the inputs being used for the regression
    print(f"\n🔄 {C.b}{C.pastel_peach}Running a Logistic Regression based on provided inputs:")
    print(f"   ✅{C.b}{C.candy_red} Y Variable:{C.r}{C.b}{C.ivory} {y_data.name}")
    print(f"   ✅{C.b}{C.sky_blue} X Variables:{C.r}{C.b}{C.ivory} {', '.join(x_data.columns)}{C.r}{C.b}{C.pastel_peach}\n")

    # ✅ Add a constant for the intercept
    x_data = sm.add_constant(x_data)

    # ✅ Run the logistic regression
    logit_model = sm.Logit(y_data, x_data).fit()

    # ✅ Print the summary
    print(logit_model.summary())

    # ✅ Plot the predicted probabilities
    plot_logistic_regression_results(y_data, logit_model.predict(x_data))


def plot_logistic_regression_results(y_data, y_pred_prob):
    """
    Plot the predicted probabilities for logistic regression.
    """
    plt.figure(figsize=(10, 6))
    plt.hist(y_pred_prob, bins=20, edgecolor='black', alpha=0.7)
    plt.axvline(0.5, color='red', linestyle='--', label="Threshold (0.5)")
    plt.title("Logistic Regression: Predicted Probabilities")
    plt.xlabel("Predicted Probability")
    plt.ylabel("Frequency")
    plt.legend()
    plt.grid(True)
    plt.show()