"""
Module: transformations.py
Description: This module provides a collection of utility functions for common DataFrame transformations 
in PySpark. These functions simplify the process of manipulating DataFrames, making it easier to perform 
data cleaning, preparation, and exploratory analysis in a scalable manner. The functions include adding 
and dropping columns, filtering rows, renaming columns, selecting specific columns, sorting, and grouping.

Purpose: To serve as a reusable toolkit for PySpark DataFrame transformations, enhancing code readability 
and reducing redundancy in data manipulation tasks across various projects.

Created by: Levi Gagne
Date: 2024-07-01
"""

from pyspark.sql import DataFrame

def with_column(df: DataFrame, col_name: str, expression: str) -> DataFrame:
    """
    Adds a new column or replaces an existing column in the DataFrame based on the given expression.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        col_name (str): The name of the new or existing column to create or replace.
        expression (str): The expression to compute the new column.

    Returns:
        DataFrame: A DataFrame with the modified column.
    """
    return df.withColumn(col_name, expression)

def filter_rows(df: DataFrame, condition: str) -> DataFrame:
    """
    Filters rows using the given condition.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        condition (str): The condition to filter rows by.

    Returns:
        DataFrame: A DataFrame containing only rows that meet the condition.
    """
    return df.filter(condition)

def drop_column(df: DataFrame, col_name: str) -> DataFrame:
    """
    Drops a column from the DataFrame.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        col_name (str): The name of the column to drop.

    Returns:
        DataFrame: A DataFrame without the dropped column.
    """
    return df.drop(col_name)

def rename_column(df: DataFrame, old_name: str, new_name: str) -> DataFrame:
    """
    Renames a column in the DataFrame.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        old_name (str): The current name of the column.
        new_name (str): The new name for the column.

    Returns:
        DataFrame: A DataFrame with the renamed column.
    """
    return df.withColumnRenamed(old_name, new_name)

def select_columns(df: DataFrame, columns: str) -> DataFrame:
    """
    Selects a set of columns from the DataFrame.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        columns (str): A comma-separated string of column names to select.

    Returns:
        DataFrame: A DataFrame containing only the specified columns.
    """
    cols = [col.strip() for col in columns.split(",")]
    return df.select(*cols)

def sort_by(df: DataFrame, columns: str) -> DataFrame:
    """
    Sorts the DataFrame by the specified columns.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        columns (str): A comma-separated string of column names to sort by.

    Returns:
        DataFrame: A DataFrame sorted by the specified columns.
    """
    cols = [col.strip() for col in columns.split(",")]
    return df.sort(*cols)

def group_by(df: DataFrame, columns: str) -> DataFrame:
    """
    Groups the DataFrame by the specified columns for aggregation.
    
    Parameters:
        df (DataFrame): The input DataFrame.
        columns (str): A comma-separated string of column names to group by.

    Returns:
        DataFrame: A grouped DataFrame ready for aggregation.
    """
    cols = [col.strip() for col in columns.split(",")]
    return df.groupBy(*cols)

# Add more transformation functions as needed
