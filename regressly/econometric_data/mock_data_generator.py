# econometric_data/mock_data_generator.py

import pandas as pd
import numpy as np
import os
from econometric_data.colorConfig import C  # Importing color configuration

def generate_dataset(n_rows, start_date, end_date, file_name):
    """
    Generate a synthetic dataset with specified parameters and save it to a CSV file.

    Parameters:
        n_rows (int): Number of rows in the dataset.
        start_date (str): Start date for the observation_date column (YYYY-MM-DD format).
        end_date (str): End date for the observation_date column (YYYY-MM-DD format).
        file_name (str): Name (and path) of the CSV file to save the dataset.

    Returns:
        None: The dataset is saved to the specified CSV file with confirmation.
    """
    try:
        # Seed for reproducibility
        np.random.seed(42)

        # Create a date range and repeat it to match the number of rows
        dates = pd.date_range(start=start_date, end=end_date, freq="D")
        repeated_dates = np.resize(dates, n_rows)

        # Create sample dataset with 10 columns
        data = {
            "observation_date": repeated_dates,
            "gender": np.random.choice(["Male", "Female"], size=n_rows),
            "age": np.random.randint(18, 94, size=n_rows),
            "income": np.random.normal(50_000, 15_000, size=n_rows).astype(int),
            "education_level": np.random.choice(["High School", "Bachelor's", "Master's", "PhD"], size=n_rows),
            "hours_worked_per_week": np.random.randint(20, 60, size=n_rows),
            "credit_score": np.random.normal(700, 50, size=n_rows).astype(int),
            "loan_amount": np.random.normal(25_000, 10_000, size=n_rows).astype(int),  # Target variable (Y)
            "house_price_index": np.random.normal(200, 50, size=n_rows).round(2),
            "car_price_index": np.random.normal(30, 10, size=n_rows).round(2),
        }

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Save dataset to CSV
        df.to_csv(file_name, index=False)
        print(f"{C.b}{C.green}✅ Dataset successfully generated and saved to:{C.r} {C.blue}{file_name}{C.r}")

    except Exception as e:
        print(f"{C.b}{C.red}❌ Error occurred while generating the dataset:{C.r} {str(e)}")


def display_file_size(file_path="lasso_dataset.csv"):
    """
    Display the file size of the specified file in MB and GB.

    Parameters:
        file_path (str): Path to the file for which the size needs to be displayed. Default is 'lasso_dataset.csv'.

    Returns:
        None: Prints the file size in MB and GB with color-coded output.
    """
    try:
        # Get the file size in bytes
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)  # Convert bytes to MB
        file_size_gb = file_size_bytes / (1024 * 1024 * 1024)  # Convert bytes to GB

        print(f"{C.b}{C.green}📁 File size of '{file_path}':{C.r}")
        print(f"  {C.b}{C.yellow}- {file_size_mb:.2f} MB{C.r}")
        print(f"  {C.b}{C.cyan}- {file_size_gb:.2f} GB{C.r}")

    except FileNotFoundError:
        print(f"{C.b}{C.red}❌ File not found:{C.r} {C.blue}{file_path}{C.r}")
    except Exception as e:
        print(f"{C.b}{C.red}❌ Error occurred while checking the file size:{C.r} {str(e)}")