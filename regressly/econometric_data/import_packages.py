# econometric_data/import_packages.py

def check_and_install_packages(required_packages=None):
    """
    Check for required Python packages, install missing ones, and declare imports globally.
    """
    import importlib
    import subprocess
    import sys
    import pkg_resources  # For checking installed packages

    # Default required packages
    DEFAULT_REQUIRED_PACKAGES = [
        "statsmodels",
        "ipywidgets",
        "scikit-learn",
        "matplotlib",
        "pandas",
        "seaborn"
    ]

    # Use default list if none provided
    if required_packages is None:
        required_packages = DEFAULT_REQUIRED_PACKAGES

    # Get a list of currently installed packages
    installed_distributions = {pkg.key for pkg in pkg_resources.working_set}

    # Track installation status
    installed_packages = []
    already_installed_packages = []

    # Function to install a package
    def install_package(package):
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        installed_packages.append(package)

    # Check and install each package in the list
    for package in required_packages:
        if package.lower() in installed_distributions:
            already_installed_packages.append(package)
        else:
            install_package(package)

    # Print installation summary
    print("\nPackage Installation Summary:")
    if installed_packages:
        print(f"Installed: {', '.join(installed_packages)}")
    else:
        print("No new packages were installed. All required packages are already installed.")
    print(f"Already Installed: {', '.join(already_installed_packages)}")

    # ---- Declare Imports as Globals ----
    global os, json, pd, sm, plt, sns, display, clear_output
    global display_file_uploader, display_model_date_selection

    # Standard library and third-party modules
    import os
    import json
    import pandas as pd
    import statsmodels.api as sm
    import matplotlib.pyplot as plt
    import seaborn as sns
    from IPython.display import display, clear_output

    # Custom modules
    from econometric_data.upload_data import display_file_uploader
    from econometric_data.model_date_selection import display_model_date_selection

    # Global imports are now accessible in the notebook
