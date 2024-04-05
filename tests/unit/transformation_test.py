import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession

from main import join_results, get_favorite_product, get_longest_streak


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName("etl_pipeline_testing").getOrCreate()
    yield spark

# we can these unit tests from directory where main file is present by command - "pytest tests"

def test_join_results(spark):
    df1 = [{"customer_id": 1, "age": 30},
           {"customer_id": 2, "age": 25},
           {"customer_id": 3, "age": 35}]
    df2 = [{"customer_id": 1, "favourite_product": "A"},
           {"customer_id": 2, "favourite_product": "B"},
           {"customer_id": 3, "favourite_product": "C"},
           {"customer_id": 5, "favourite_product": "D"}]

    # Create Spark DataFrame
    original_df1 = spark.createDataFrame(df1)
    original_df2 = spark.createDataFrame(df2)

    final_df = join_results(original_df1, original_df2).select("age", "customer_id", "favourite_product")

    expected_data = [{"customer_id": 1, "age": 30, "favourite_product": "A"},
                     {"customer_id": 2, "age": 25, "favourite_product": "B"},
                     {"customer_id": 3, "age": 35, "favourite_product": "C"}]

    expected_df = spark.createDataFrame(expected_data)
    assertDataFrameEqual(final_df, expected_df)


def test_get_favorite_product(spark):
    cust_data = [{"custId": 1, "productSold": "A", "unitsSold": 5},
          {"custId": 1, "productSold": "B", "unitsSold": 4},
          {"custId": 2, "productSold": "C", "unitsSold": 2},
          {"custId": 2, "productSold": "D", "unitsSold": 1},
          {"custId": 2, "productSold": "D", "unitsSold": 4},
          {"custId": 2, "productSold": "C", "unitsSold": 6},
          {"custId": 3, "productSold": "A", "unitsSold": 1}
          ]

    # Create Spark DataFrame
    df = spark.createDataFrame(cust_data)

    actual_df = get_favorite_product(df)

    expected_data = [{"customer_id": 1, "favourite_product": "A"},
                     {"customer_id": 2, "favourite_product": "C"},
                     {"customer_id": 3, "favourite_product": "A"}]

    expected_df = spark.createDataFrame(expected_data)
    assertDataFrameEqual(actual_df, expected_df)


def test_get_longest_streak(spark):
    cust_data = [{"custId": 1, "transactionDate": "2012-03-02"},
                 {"custId": 1, "transactionDate": "2012-03-02"},
                 {"custId": 1, "transactionDate": "2012-03-15"},
                 {"custId": 1, "transactionDate": "2012-07-24"},
                 {"custId": 1, "transactionDate": "2012-07-25"},
                 {"custId": 1, "transactionDate": "2012-07-25"},
                 {"custId": 1, "transactionDate": "2012-07-26"},
                 {"custId": 1, "transactionDate": "2012-07-27"},
                 {"custId": 1, "transactionDate": "2012-07-28"},
                 {"custId": 1, "transactionDate": "2012-07-29"},
                 {"custId": 1, "transactionDate": "2012-09-03"},
                 {"custId": 2, "transactionDate": "2012-03-28"},
                 {"custId": 2, "transactionDate": "2012-04-01"},
                 {"custId": 2, "transactionDate": "2012-04-01"}]

    # Create Spark DataFrame
    df = spark.createDataFrame(cust_data)

    actual_df = get_longest_streak(df)

    expected_data = [{"customer_id": 1, "longest_streak": 6},
                     {"customer_id": 2, "longest_streak": 1}]

    expected_df = spark.createDataFrame(expected_data)
    assertDataFrameEqual(actual_df, expected_df)
