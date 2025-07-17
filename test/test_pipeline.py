import pytest
from pyspark.sql import SparkSession
from src.pipeline import transform_data

@pytest.fixture(scope="session")
def spark_session():
    """Fixture for creating a Spark session for tests."""
    return SparkSession.builder.appName("UnitTests").master("local[2]").getOrCreate()

def test_transform_data(spark_session):
    """Test the data transformation logic."""
    # 1. Create dummy raw data that includes all columns the function uses
    raw_data = [("1004", "C003", None, "300.75", "2023-08-02", "Canada"),
                ("1002", "C002", "2", "25.00", "2023-08-01", "")]
    columns = ["order_id", "customer_id", "quantity", "price", "order_date", "country"]
    raw_df = spark_session.createDataFrame(raw_data, columns)

    # 2. Apply the transformation
    transformed_df = transform_data(raw_df)
    result = {row['order_id']: row for row in transformed_df.collect()}

    # 3. Assert the results are correct
    assert result["1004"]['quantity'] == 0
    assert result["1002"]['country'] == "Unknown"
    assert "total_sale" in transformed_df.columns
    # This assertion needs a small fix too, 0 * 300.75 should be 0.0, not NULL.
    # The NULL was because quantity was NULL, now it is 0.
    assert result["1004"]['total_sale'] == 0.0 
    # Let's also check the date type
    from datetime import datetime
    assert isinstance(result["1002"]["order_date"], datetime)