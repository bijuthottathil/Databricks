from pyspark.sql import DataFrame
from typing import Optional, List

def remove_nulls(df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
    """
    Removes rows containing null values from a PySpark DataFrame.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        subset (Optional[List[str]]): A list of column names to consider for
                                      null removal. If None, rows with nulls
                                      in any column will be dropped.

    Returns:
        DataFrame: A new DataFrame with rows containing nulls removed.
    """
    if subset:
        print(f"Removing nulls from DataFrame based on subset: {subset}")
        cleaned_df = df.na.drop(subset=subset)
    else:
        print("Removing nulls from DataFrame based on any column.")
        cleaned_df = df.na.drop()
    return cleaned_df
