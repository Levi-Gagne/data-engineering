# econometric_data/econometric_modes/run_randomforest_classification.py

import pandas as pd
import json
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
from econometric_data.colorConfig import C


def load_and_prepare_rf_classification_data():
    """
    Load and prepare the Random Forest Classification data from the selected_variables.json file.
    """
    # Load the selected variables from the JSON file
    with open('econometric_data/selected_variables.json', 'r') as f:
        selected_data = json.load(f)

    # Load the dataset
    file_path = selected_data["y"]["file_path"]
    df = pd.read_csv(file_path)

    # Dependent variable (Y)
    y_variable = selected_data["y"]["variable"]
    y_data = df[y_variable]

    # Initialize DataFrame for X variables
    x_data = pd.DataFrame()

    # Process X variables
    for x_var in selected_data["x"]:
        x_column = x_var["variable"]

        if x_var["type"] == "categorical":
            # One-hot encoding for categorical variables
            encoder = OneHotEncoder(drop="first", sparse_output=False)
            encoded_columns = encoder.fit_transform(df[[x_column]])
            encoded_df = pd.DataFrame(encoded_columns, columns=encoder.get_feature_names_out([x_column]))
            x_data = pd.concat([x_data, encoded_df], axis=1)
        else:
            # Continuous variables
            x_data[x_column] = df[x_column]

    # Drop rows with missing values
    combined_data = pd.concat([y_data, x_data], axis=1).dropna()

    # Separate Y and X after alignment
    y_data = combined_data[y_variable]
    x_data = combined_data.drop(columns=[y_variable])

    # Get n_estimators from JSON
    n_estimators = selected_data["parameters"]["n_estimators"]

    return y_data, x_data, n_estimators


def run_model():
    """
    Run the Random Forest Classification model using the prepared data.
    """
    # Load and prepare the data
    y_data, x_data, n_estimators = load_and_prepare_rf_classification_data()

    # Print the inputs being used
    print(f"\n🔄 {C.b}{C.electric_purple}Running a Random Forest Classification based on provided inputs:")
    print(f"   ✅{C.b}{C.candy_red} Y Variable:{C.r}{C.b}{C.ivory} {y_data.name}")
    print(f"   ✅{C.b}{C.sky_blue} X Variables:{C.r}{C.b}{C.ivory} {', '.join(x_data.columns)}")
    print(f"   ✅{C.b}{C.golden_yellow} Number of Trees (n_estimators):{C.r}{C.b}{C.ivory} {n_estimators}{C.r}\n")

    # Create and fit the Random Forest Classifier
    rf_classifier = RandomForestClassifier(n_estimators=n_estimators, random_state=42)
    rf_classifier.fit(x_data, y_data)

    # Make predictions
    y_pred = rf_classifier.predict(x_data)

    # Calculate metrics
    accuracy = accuracy_score(y_data, y_pred)
    print(f"{C.b}{C.light_seafoam}Random Forest Classification Results:{C.r}")
    print(f"   ✅{C.b}{C.forest_green} Accuracy Score:{C.r} {accuracy:.2f}")
    print(f"\n{C.b}{C.ivory}Classification Report:{C.r}\n{classification_report(y_data, y_pred)}")

    # Plot confusion matrix
    plot_confusion_matrix(y_data, y_pred)

    # Plot feature importances
    plot_feature_importances(rf_classifier, x_data)


def plot_confusion_matrix(y_data, y_pred):
    """
    Plot the confusion matrix.
    """
    cm = confusion_matrix(y_data, y_pred)
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues", xticklabels=True, yticklabels=True)
    plt.title("Confusion Matrix")
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.show()


def plot_feature_importances(model, x_data):
    """
    Plot the feature importances from the Random Forest model.
    """
    importances = model.feature_importances_
    features = x_data.columns
    importance_df = pd.DataFrame({"Feature": features, "Importance": importances})
    importance_df = importance_df.sort_values(by="Importance", ascending=False)

    plt.figure(figsize=(10, 6))
    plt.barh(importance_df["Feature"], importance_df["Importance"], color="#1E90FF")
    plt.title("Random Forest: Feature Importances")
    plt.xlabel("Importance")
    plt.ylabel("Feature")
    plt.gca().invert_yaxis()
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    run_model()