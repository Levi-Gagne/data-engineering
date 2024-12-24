"""
Script: Extract Data from Embedded Power BI Reports

This script demonstrates how to:
1. Authenticate to Power BI using `DeviceCodeLoginAuthentication`.
2. Embed a Power BI report in a notebook environment using the `powerbiclient` library.
3. List pages within the embedded report.
4. Retrieve visuals from the active page of the report.
5. Export summarized data from a specific visual.
6. Convert the exported data into a Pandas DataFrame for analysis.
7. Visualize the data extracted from Power BI.

The script is structured with a `main()` function for ease of use and modularity.

Author: Levi Gagne
"""

# Import necessary libraries
from powerbiclient import Report, models
from powerbiclient.authentication import DeviceCodeLoginAuthentication
from IPython.display import display
from io import StringIO
import pandas as pd
import matplotlib.pyplot as plt
import time


def authenticate_power_bi():
    """
    Authenticate to Power BI using DeviceCodeLoginAuthentication.
    Returns the authentication object.
    """
    print("Authenticating to Power BI...")
    return DeviceCodeLoginAuthentication()


def embed_power_bi_report(auth, group_id, report_id):
    """
    Embed the Power BI report using the powerbiclient library.
    Displays the embedded report.
    """
    print("Embedding the Power BI report...")
    report = Report(group_id=group_id, report_id=report_id, auth=auth, mode=models.EmbedMode.EDIT)
    display(report)
    time.sleep(10)  # Wait for the report to embed
    if not report._embedded:
        raise Exception("Power BI report is not embedded. Ensure the report is displayed above.")
    return report


def list_pages(report):
    """
    List all pages in the embedded Power BI report.
    """
    print("\nPages in the report:")
    pages = report.get_pages()
    for page in pages:
        print(f"Page name: {page['name']}, Display name: {page['displayName']}")
    return pages


def get_active_page(report, pages):
    """
    Retrieve the active page from the report.
    """
    active_page = next((page for page in pages if page['isActive']), None)
    if not active_page:
        raise Exception("No active page found in the report.")
    print(f"\nActive page: {active_page['name']}")
    return active_page


def list_visuals(report, active_page_name):
    """
    List all visuals on the active page of the report.
    """
    visuals = report.visuals_on_page(active_page_name)
    print("\nVisuals on the active page:")
    for visual in visuals:
        print(f"Visual name: {visual['name']}, Visual type: {visual['type']}")
    return visuals


def export_visual_data(report, active_page_name, visuals):
    """
    Export summarized data from a specific visual on the active page.
    Adjust the visual type or name to target the desired visual.
    """
    try:
        visual = next(filter(lambda v: v['type'] == 'clusteredColumnChart', visuals))
    except StopIteration:
        raise Exception("No clusteredColumnChart visual found on the active page.")
    visual_name = visual['name']
    summarized_data = report.export_visual_data(active_page_name, visual_name, rows=20)
    print("\nSummarized data from the visual:")
    print(summarized_data)
    return summarized_data


def convert_to_dataframe(summarized_data):
    """
    Convert the exported data (in CSV format) into a Pandas DataFrame for analysis.
    """
    data = StringIO(summarized_data)
    df = pd.read_csv(data, sep=",")
    print("\nDataFrame head:")
    print(df.head())
    return df


def visualize_data(df):
    """
    Visualize the extracted data using Matplotlib.
    In this example, a pie chart is generated based on the data.
    """
    print("\nVisualizing the data...")
    df.plot.pie(y="Total Units", labels=df.loc[:, 'isVanArsdel'].values.tolist(), figsize=(10, 15))
    plt.show()


def main():
    """
    Main function to execute the steps for embedding and extracting data from Power BI.
    """
    # Replace these placeholders with your actual Group ID and Report ID
    group_id = "YOUR_GROUP_ID"
    report_id = "YOUR_REPORT_ID"

    # Step 1: Authenticate
    auth = authenticate_power_bi()

    # Step 2: Embed Report
    report = embed_power_bi_report(auth, group_id, report_id)

    # Step 3: List Pages
    pages = list_pages(report)

    # Step 4: Get Active Page
    active_page = get_active_page(report, pages)
    active_page_name = active_page['name']

    # Step 5: List Visuals
    visuals = list_visuals(report, active_page_name)

    # Step 6: Export Visual Data
    summarized_data = export_visual_data(report, active_page_name, visuals)

    # Step 7: Convert Data to DataFrame
    df = convert_to_dataframe(summarized_data)

    # Step 8: Visualize Data
    visualize_data(df)


if __name__ == "__main__":
    main()
