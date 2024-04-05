import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession

from main import main


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName("etl_pipeline_integration_testing").getOrCreate()
    yield spark


def test_main(spark):
    """This is testing main function of our app. We are end-to-end testing if our app is writing the expected results
    for the input"""

    # Create Spark DataFrame
    source = "./tests/test_data/sample_file.csv"
    destination = "./tests/test_data/test_out.parquet"

    main(spark, None, None, source=source, destination=destination)
    df = spark.read.parquet(destination)
    actual_df = df.select(df.customer_id.cast("long"), df.favourite_product, df.longest_streak.cast("long"))
    expected_data = [{"customer_id": 23938, "favourite_product": "PURA250", "longest_streak": 6},
                     {"customer_id": 23975, "favourite_product": "PURA100", "longest_streak": 1},
                     {"customer_id": 24019, "favourite_product": "PURA200", "longest_streak": 1}]

    expected_df = spark.createDataFrame(expected_data)
    assertDataFrameEqual(actual_df, expected_df)
