# test_data_cleaner.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from data_cleaner import remove_nulls

# Define a fixture for SparkSession to be used across tests
@pytest.fixture(scope="session")
def spark_session():
    """
    Provides a SparkSession for testing.
    """
    spark = SparkSession.builder \
        .appName("PySparkNullRemovalTests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

# Define a common schema for test DataFrames
@pytest.fixture
def sample_schema():
    """
    Provides a common schema for test DataFrames.
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("city", StringType(), True)
    ])

def test_remove_nulls_any(spark_session, sample_schema):
    """
    Test remove_nulls when no subset is specified (drops rows with any null).
    """
    print("\nRunning test_remove_nulls_any...")
    data = [
        (1, "Alice", 30, "New York"),
        (2, "Bob", None, "Los Angeles"),
        (3, "Charlie", 25, None),
        (4, None, 35, "Chicago"),
        (5, "David", 40, "Houston")
    ]
    df = spark_session.createDataFrame(data, sample_schema)

    cleaned_df = remove_nulls(df)

    # Expected data: only rows with no nulls
    expected_data = [
        (1, "Alice", 30, "New York"),
        (5, "David", 40, "Houston")
    ]
    expected_df = spark_session.createDataFrame(expected_data, sample_schema)

    # Collect and sort to compare DataFrames reliably
    assert sorted(cleaned_df.collect()) == sorted(expected_df.collect())
    print("test_remove_nulls_any passed.")


def test_remove_nulls_subset(spark_session, sample_schema):
    """
    Test remove_nulls when a subset of columns is specified.
    """
    print("\nRunning test_remove_nulls_subset...")
    data = [
        (1, "Alice", 30, "New York"),
        (2, "Bob", None, "Los Angeles"), # Null in 'age'
        (3, "Charlie", 25, None),        # Null in 'city'
        (4, "Diana", 35, "Chicago"),
        (5, "Eve", None, None)          # Null in 'age' and 'city'
    ]
    df = spark_session.createDataFrame(data, sample_schema)

    # Remove nulls only if 'age' or 'city' is null
    cleaned_df = remove_nulls(df, subset=["age", "city"])

    # Expected data: rows where 'age' and 'city' are both non-null
    expected_data = [
        (1, "Alice", 30, "New York"),
        (4, "Diana", 35, "Chicago")
    ]
    expected_df = spark_session.createDataFrame(expected_data, sample_schema)

    assert sorted(cleaned_df.collect()) == sorted(expected_df.collect())
    print("test_remove_nulls_subset passed.")


def test_remove_nulls_empty_df(spark_session, sample_schema):
    """
    Test remove_nulls with an empty DataFrame.
    """
    print("\nRunning test_remove_nulls_empty_df...")
    data = []
    df = spark_session.createDataFrame(data, sample_schema)

    cleaned_df = remove_nulls(df)

    assert cleaned_df.count() == 0
    print("test_remove_nulls_empty_df passed.")


def test_remove_nulls_no_nulls(spark_session, sample_schema):
    """
    Test remove_nulls with a DataFrame that has no nulls.
    """
    print("\nRunning test_remove_nulls_no_nulls...")
    data = [
        (1, "Alice", 30, "New York"),
        (2, "Bob", 28, "Los Angeles")
    ]
    df = spark_session.createDataFrame(data, sample_schema)

    cleaned_df = remove_nulls(df)

    # The cleaned DataFrame should be identical to the original
    assert sorted(cleaned_df.collect()) == sorted(df.collect())
    print("test_remove_nulls_no_nulls passed.")

def test_remove_nulls_subset_no_nulls_in_subset(spark_session, sample_schema):
    """
    Test remove_nulls with a subset where no nulls exist in those columns.
    """
    print("\nRunning test_remove_nulls_subset_no_nulls_in_subset...")
    data = [
        (1, "Alice", 30, "New York"),
        (2, "Bob", None, "Los Angeles"), # Null in 'age', but 'name' and 'city' are fine
        (3, "Charlie", 25, None)         # Null in 'city', but 'name' and 'age' are fine
    ]
    df = spark_session.createDataFrame(data, sample_schema)

    # Remove nulls only if 'name' is null (which it isn't in this data)
    cleaned_df = remove_nulls(df, subset=["name"])

   # The cleaned DataFrame should be identical to the original as 'name' has no nulls
    assert sorted(cleaned_df.collect()) == sorted(df.collect())
    print("test_remove_nulls_subset_no_nulls_in_subset passed.")