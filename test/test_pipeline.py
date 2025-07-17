import pytest
from pyspark.sql import SparkSession
from src.pipeline import transform_data

@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a Spark session for tests."""
    return SparkSession.builder.appName("UnitTests").master("local[2]").getOrCreate()

def test_transform_data(spark_session):
    """Test the data transformation logic."""
    # 1. Create dummy raw data
    raw_data = [("1004", "C003", None, "300.75", "Canada"),
                ("1002", "C002", "2", "25.00", "")]
    columns = ["order_id", "customer_id", "quantity", "price", "country"]
    raw_df = spark_session.createDataFrame(raw_data, columns)

    # 2. Apply the transformation
    transformed_df = transform_data(raw_df)
    result = {row['order_id']: row for row in transformed_df.collect()}

    # 3. Assert the results are correct
    assert result["1004"]['quantity'] == 0
    assert result["1002"]['country'] == "Unknown"
    assert "total_sale" in transformed_df.columns
    assert result["1004"]['total_sale'] is None # 0 * 300.75 = NULL in Spark