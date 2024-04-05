from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, row_number, date_sub, count, max
from pyspark.sql.window import Window

import argparse


def main(spark, database, table, source, destination):
    # reading csv data from source path which we can pass from arguments
    # df = spark.read.csv(args.source, sep='|', header=True, inferSchema=True)
    df = read_data(spark, source)
    df.cache()

    """With spark DSL"""
    favourite_prod_df = get_favorite_product(df)
    longest_streak_df = get_longest_streak(df)

    # finally joining both dataframes
    final_df = join_results(favourite_prod_df, longest_streak_df)

    # storing the results in parquet format if destination provided
    if destination:
        write_to_file(final_df, destination)

    # bonus assignment solution
    # storing results to postgresql database
    if database:
        write_to_db(final_df, database, table)

    ################################################
    """With Spark SQL
    We need to uncomment last line if we want to run
    using Spark SQL. Uncomment - # final_df.show() 
    """
    df.createOrReplaceTempView('transactions')
    df1 = get_favourite_product_by_spark_sql(spark)
    df2 = get_longest_streak_by_spark_sql(spark)

    final_df = join_results(df1, df2)
    # final_df.show()


def read_data(spark, source):
    return spark.read.csv(source, sep='|', header=True, inferSchema=True)


def get_favorite_product(df):
    cust_prod_df = df.groupBy("custId", "productSold").agg(sum("unitsSold").alias("total_units_sold"))
    # find max units sold for each customer
    max_sold_df = cust_prod_df.groupBy("custId").agg(max("total_units_sold").alias("max_sold"))
    max_sold_df = max_sold_df.select(max_sold_df.custId.alias("customer_id"), max_sold_df.max_sold)

    # condition to join customer_product and max_sold dataframes
    cond = [cust_prod_df.custId == max_sold_df.customer_id, cust_prod_df.total_units_sold == max_sold_df.max_sold]

    # finally joining to get only product for which total_units_sold equals to max units sold
    joined_df = cust_prod_df.join(max_sold_df, cond)
    favourite_prod_df = joined_df.select(joined_df.customer_id, joined_df.productSold.alias("favourite_product"))
    return favourite_prod_df


def get_longest_streak(df):
    # getting distinct transaction days as customer has purchase multiple products in a day and
    # to us only consecutive days matter
    cust_purchase_df = df.select("custId", "transactionDate").distinct()
    # creating window to find row number basis on transaction days
    window = Window.partitionBy("custId").orderBy("transactionDate")
    purchase_df_with_rn = cust_purchase_df.withColumn("row_num", row_number().over(window))
    # we need to find groups of consecutive days so we can subtract row_nums from transactiondate to get same date
    purchase_df_with_grp = purchase_df_with_rn.withColumn("grp", date_sub(purchase_df_with_rn.transactionDate,
                                                                          purchase_df_with_rn.row_num))
    # now we can group by grp; which is the same date we have got by transactiondate -row_num
    streak_df = purchase_df_with_grp.groupBy("custId", "grp").agg(count("transactionDate").alias("streak"))
    # find max of streak for each group
    longest_streak_df = streak_df.groupBy("custId").agg(max("streak").alias("longest_streak"))
    longest_streak_df = longest_streak_df.select(longest_streak_df.custId.alias("customer_id"), longest_streak_df.longest_streak)
    return longest_streak_df


def join_results(favourite_prod_df, longest_streak_df):
    final_df = favourite_prod_df.join(longest_streak_df, 'customer_id', how='left')
    return final_df


def write_to_file(df, destination):
    df.write.options(header=True, inferSchema=True).mode("overwrite").parquet(destination)


def write_to_db(df, database, table):
    # writing results into postgreDB
    df.write.mode("overwrite").format("jdbc")\
        .option("url", "jdbc:postgresql://db:5432/{database}".format(database=database))\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", table)\
        .option("user", "postgres")\
        .option("password", "admin123").save()


#################################################################
# functions for spark SQL
#################################################################

def get_favourite_product_by_spark_sql(spark):
    with open("./sql_scripts/customer_favourite_product.sql") as f:
        customer_favourite_sql = f.read()
    df = spark.sql(customer_favourite_sql)
    return df


def get_longest_streak_by_spark_sql(spark):
    with open("./sql_scripts/customer_longest_streak.sql") as f:
        customer_longest_streak_sql = f.read()
    df = spark.sql(customer_longest_streak_sql)
    return df


if __name__ == "__main__":
    # Create the parser and add arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', help="To define source path from arguments", required=True)
    parser.add_argument('--destination', help="To define destination path from arguments", required=False)
    parser.add_argument('--database', help="To define database name where data is stored", required=False)
    parser.add_argument('--table', help="To define table where data is stored", required=False)

    args = parser.parse_args()

    # parsing the arguments
    database = args.database
    table = args.table
    source = args.source
    destination = args.destination

    if destination:
        spark: SparkSession = SparkSession.builder.appName("etl-pipeline").getOrCreate()
    else:
        """Pyspark is not supporting deployment_mode=cluster in standalone mode so we are submitting spark job with client 
                mode from a separate container. We can also setup another cluster manager like yarn if we want to submit as cluster 
                mode"""
        spark: SparkSession = (SparkSession.builder.master('spark://spark:7077')
                               .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3").getOrCreate())

    main(spark, database, table, source, destination)
