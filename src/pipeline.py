from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

# Define paths using S3 instead of DBFS 
RAW_CSV_PATH = "s3://my-databricks-delta-lake-bucket/raw/orders_data.csv"  # For demo, still using a dummy file on DBFS
DELTA_TABLE_PATH = "s3://my-databricks-delta-lake-bucket/orders_cleaned" # IMPORTANT: S3 path

def get_spark_session(app_name: str) -> SparkSession:
    """Gets or creates a SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def extract_from_csv(path: str, spark: SparkSession) -> DataFrame:
    # (Identical to previous answer's function)
    print(f"EXTRACT: Reading from {path}")
    return spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(path)

def transform_data(df: DataFrame) -> DataFrame:
    # (Identical to previous answer's function)
    print("TRANSFORM: Cleaning data")
    transformed_df = df.withColumn("quantity", col("quantity").cast(IntegerType())) \
                       .withColumn("price", col("price").cast(DoubleType())) \
                       .withColumn("order_date", col("order_date").cast(TimestampType()))
    transformed_df = transformed_df.fillna(0, subset=["quantity"]) \
                                   .withColumn("country", when(col("country").isNull() | (col("country") == ""), "Unknown").otherwise(col("country")))
    transformed_df = transformed_df.withColumn("total_sale", col("quantity") * col("price"))
    return transformed_df

def load_to_delta(df: DataFrame, path: str) -> None:
    """Loads the DataFrame to a Delta table at the specified path."""
    print(f"LOAD: Writing to Delta table at {path}")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    print("LOAD: Completed.")

def main():
    """Main pipeline orchestration function."""
    spark = get_spark_session("DataCleaningPipeline")
    # In a real pipeline, the EXTRACT step would read from a production S3 location.
    # For this example, we assume the dummy CSV from the notebook is still in DBFS.
    raw_df = extract_from_csv(RAW_CSV_PATH, spark)
    cleaned_df = transform_data(raw_df)
    load_to_delta(cleaned_df, DELTA_TABLE_PATH)
    #spark.stop() This causes workload to fail It is automatically taken care of by Databricks
    print("Pipeline finished successfully.")

if __name__ == "__main__":
    main()