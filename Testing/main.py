# main.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from data_cleaner import remove_nulls

def main():
    """
    Main function to demonstrate the usage of remove_nulls.
    """
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("NullRemovalDemo") \
        .getOrCreate()

    print("SparkSession created.")

    # Define schema for the DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])

    # Sample data with nulls
    data = [
        (1, "Alice", 30, "New York"),
        (2, "Bob", None, "Los Angeles"),
        (3, "Charlie", 25, None),
        (4, None, 35, "Chicago"),
        (5, "David", 40, "Houston"),
        (6, "Eve", None, None),
        (7, "Frank", 50, "Miami"),
        (8, None, None, "Dallas")
    ]

    # Create a DataFrame
    df = spark.createDataFrame(data, schema)
    print("\nOriginal DataFrame:")
    df.show()
    print(f"Original DataFrame count: {df.count()}")

    # --- Case 1: Remove rows with any null ---
    print("\n--- Case 1: Removing rows with any null ---")
    cleaned_df_any = remove_nulls(df)
    print("DataFrame after removing rows with any null:")
    cleaned_df_any.show()
    print(f"Cleaned DataFrame (any null) count: {cleaned_df_any.count()}")

    # --- Case 2: Remove rows with nulls in specific columns (e.g., 'age' or 'city') ---
    print("\n--- Case 2: Removing rows with nulls in 'age' or 'city' columns ---")
    cleaned_df_subset = remove_nulls(df, subset=["age", "city"])
    print("DataFrame after removing rows with nulls in 'age' or 'city':")
    cleaned_df_subset.show()
    print(f"Cleaned DataFrame (subset) count: {cleaned_df_subset.count()}")

    # Stop the SparkSession
    spark.stop()
    print("\nSparkSession stopped.")

if __name__ == "__main__":
    main()