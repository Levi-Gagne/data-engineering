# econometric_data/regression_info.py


import ipywidgets as widgets
from IPython.display import display, HTML
from econometric_data.colorConfig import C

regression_info = {
    "Linear Regression": {
        "description": "Linear Regression is a statistical method for modeling the relationship between a dependent variable (Y) and one or more independent variables (X). It assumes a linear relationship between the variables.",
        "formula": "y = β0 + β1X1 + β2X2 + ... + ε",
        "input": "X values (features), Y value (continuous)",
        "output": "Predicted continuous value",
        "assumptions": [
            "Linear relationship between independent and dependent variables",
            "Homoscedasticity (constant variance of errors)",
            "Independence of errors",
            "Normally distributed errors"
        ],
        "practical_applications": [
            "Predicting house prices based on features like size, location, etc.",
            "Forecasting sales revenue based on advertising spend",
            "Modeling the effect of education on income"
        ],
        "key_concept": "Predicts a continuous outcome based on a linear combination of inputs."
    },
    "Logistic Regression": {
        "description": "Logistic Regression is used for binary classification problems where the outcome is either 0 or 1. It predicts the probability of a binary event occurring by modeling the log-odds of the outcome as a linear combination of the inputs.",
        "formula": "p = 1 / (1 + e^-(β0 + ∑βiXi))",
        "input": "X values (features), Y value (binary: 0/1)",
        "output": "Predicted probability between 0 and 1",
        "assumptions": [
            "The dependent variable is binary",
            "No multicollinearity among independent variables",
            "Linear relationship between independent variables and the log-odds of the dependent variable",
            "Large sample size is preferred"
        ],
        "practical_applications": [
            "Spam detection in emails (spam or not spam)",
            "Customer churn prediction (will the customer leave or stay)",
            "Disease diagnosis (positive or negative result)"
        ],
        "key_concept": "Predicts probability for binary classification."
    },
    "ARIMA": {
        "description": "ARIMA (AutoRegressive Integrated Moving Average) is a popular model for time series forecasting. It captures trends, seasonality, and noise in time-dependent data.",
        "formula": "Yt = c + ∑ϕiYt-i + ∑θjεt-j + εt",
        "input": "Time-indexed series of values, optional exogenous variables (X)",
        "output": "Predicted future values in the time series",
        "assumptions": [
            "The time series is stationary (mean, variance, and covariance are constant over time)",
            "No autocorrelation in residuals",
            "Appropriate differencing is applied to achieve stationarity"
        ],
        "practical_applications": [
            "Forecasting stock prices",
            "Predicting future sales based on historical data",
            "Modeling economic indicators such as GDP growth"
        ],
        "key_concept": "Time series forecasting."
    },
    "Probit Model": {
        "description": "The Probit model is a binary classification model that uses the normal cumulative distribution function (CDF) to model the probability of a binary outcome.",
        "formula": "P(y=1) = Φ(β0 + ∑βiXi)",
        "input": "X values (features), Y value (binary: 0/1)",
        "output": "Predicted probability between 0 and 1",
        "assumptions": [
            "The dependent variable is binary",
            "The error term follows a standard normal distribution"
        ],
        "practical_applications": [
            "Credit risk assessment (default or no default)",
            "Election outcome prediction (win or lose)",
            "Medical diagnosis (disease present or absent)"
        ],
        "key_concept": "Uses the normal distribution to model binary outcomes."
    },
    "Lasso Regression": {
        "description": "Lasso Regression (Least Absolute Shrinkage and Selection Operator) is a linear regression model that includes L1 regularization to reduce overfitting and perform automatic feature selection by shrinking some coefficients to zero.",
        "formula": "Loss = Σ(yᵢ - (β0 + ΣβⱼXⱼ))² + λΣ|βⱼ|",
        "input": "X values (features), Y value (continuous)",
        "output": "Predicted continuous value, with some coefficients potentially set to zero",
        "assumptions": [
            "Linear relationship between independent and dependent variables",
            "Features should not be highly correlated",
            "Errors should be normally distributed"
        ],
        "practical_applications": [
            "Predicting house prices with a large number of features",
            "Feature selection in high-dimensional datasets",
            "Modeling the effect of customer behavior on purchase amounts"
        ],
        "key_concept": "Reduces overfitting and performs feature selection by shrinking some coefficients to zero."
    },
    "Random Forest Classification": {
        "description": "Random Forest Classification is an ensemble learning method used for classifying categorical Y variables by building multiple decision trees and combining their predictions through majority vote.",
        "formula": "Prediction = Majority Vote (Classification Trees)",
        "input": "X values (features), Y value (categorical)",
        "output": "Predicted class label",
        "assumptions": [
            "Y variable must be categorical",
            "The trees are independent and diverse",
            "Handles both continuous and categorical features"
        ],
        "practical_applications": [
            "Spam detection",
            "Customer churn prediction",
            "Disease diagnosis",
            "Multi-class classification problems"
        ],
        "key_concept": "Predicts a class label by majority vote from decision trees.",
        "rules": widgets.HTML(
            value="""
            <b style='color:#228B22;'>Rules for Random Forest Classification:</b>
            <ul>
                <li><b>Y Variable (Target):</b> Must be <span style='color:#FF4500;'>categorical</span> (binary or multi-class).</li>
                <li><b>X Variables (Features):</b> Can be <span style='color:#1E90FF;'>continuous</span> or <span style='color:#FF8C00;'>categorical</span>.</li>
                <li><b>Number of Trees (n_estimators):</b> Set the number of trees, typically between <span style='color:#FFD700;'>10 to 500</span>.</li>
            </ul>
            """
        )
    },
    "Random Forest Regression": {
        "description": "Random Forest Regression is an ensemble learning method used to predict continuous Y variables by averaging the predictions of multiple decision trees.",
        "formula": "Prediction = Average(Regression Trees)",
        "input": "X values (features), Y value (continuous)",
        "output": "Predicted continuous value",
        "assumptions": [
            "Y variable must be continuous",
            "The trees are independent and diverse",
            "Handles both continuous and categorical features"
        ],
        "practical_applications": [
            "House price prediction",
            "Sales forecasting",
            "Stock price prediction",
            "Credit scoring and risk assessment"
        ],
        "key_concept": "Predicts a continuous value by averaging the predictions from multiple decision trees.",
        "rules": widgets.HTML(
            value="""
            <b style='color:#228B22;'>Rules for Random Forest Regression:</b>
            <ul>
                <li><b>Y Variable (Target):</b> Must be <span style='color:#32CD32;'>continuous</span>.</li>
                <li><b>X Variables (Features):</b> Can be <span style='color:#1E90FF;'>continuous</span> or <span style='color:#FF8C00;'>categorical</span>.</li>
                <li><b>Number of Trees (n_estimators):</b> Set the number of trees, typically between <span style='color:#FFD700;'>10 to 500</span>.</li>
            </ul>
            """
        )
    }
}


# ---- Function to Display Regression Info ----
def display_regression_info(model_name):
    """
    Displays detailed information about the selected regression model, using widgets.HTML for formatted output.
    """
    info = regression_info.get(model_name, {})
    if info:
        print(f"{C.b}{model_name}{C.r}")
        print(f"{C.blue}Description:{C.r} {info['description']}")
        print(f"{C.green}Formula:{C.r} {info['formula']}")
        print(f"{C.b}Input:{C.r} {info['input']}")
        print(f"{C.b}Output:{C.r} {info['output']}")
        print(f"{C.red}Key Concept:{C.r} {info['key_concept']}")
        print(f"{C.magenta}Assumptions:{C.r}")
        for assumption in info['assumptions']:
            print(f"  - {assumption}")
        print(f"{C.cyan}Practical Applications:{C.r}")
        for app in info['practical_applications']:
            print(f"  - {app}")

        # ✅ Display the HTML rules widget
        if "rules" in info:
            display(info["rules"])
    else:
        print(f"{C.red}Model not found.{C.r}")
