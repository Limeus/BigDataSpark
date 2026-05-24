import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    corr,
    count,
    countDistinct,
    dense_rank,
    lag,
    lit,
    max,
    min,
    round,
    sum,
)


POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/lab2")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/lab2")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "lab2")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lab2")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/lab2")

PG_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def spark_session():
    return (
        SparkSession.builder.appName("lab2-build-nosql-reports")
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .getOrCreate()
    )


def read_pg(spark, table):
    return spark.read.jdbc(POSTGRES_URL, table, properties=PG_PROPS)


def write_clickhouse(df, table):
    fill_values = {}
    for name, dtype in df.dtypes:
        if dtype == "string":
            fill_values[name] = ""
        elif dtype.startswith(("int", "bigint", "double", "float", "decimal", "long")):
            fill_values[name] = 0
    if fill_values:
        df = df.na.fill(fill_values)

    (
        df.write.format("jdbc")
        .option("url", CLICKHOUSE_URL)
        .option("dbtable", table)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
        .mode("overwrite")
        .save()
    )


def write_mongo(df, collection):
    (
        df.write.format("mongodb")
        .option("database", "lab2")
        .option("collection", collection)
        .mode("overwrite")
        .save()
    )


def publish(df, name):
    write_clickhouse(df, name)
    write_mongo(df, name)
    print(f"Published {name}: {df.count()} rows")


def main():
    spark = spark_session()

    fact = read_pg(spark, "dw.fact_sales")
    product = read_pg(spark, "dw.dim_product")
    category = read_pg(spark, "dw.dim_product_category")
    customer = read_pg(spark, "dw.dim_customer")
    store = read_pg(spark, "dw.dim_store")
    supplier = read_pg(spark, "dw.dim_supplier")
    date = read_pg(spark, "dw.dim_date")
    geo = read_pg(spark, "dw.dim_geo_location")
    country = read_pg(spark, "dw.dim_country")

    product_sales = (
        fact.join(product, "product_key")
        .join(category, "category_key")
        .groupBy("product_key", "product_name", "category_name")
        .agg(
            count("*").alias("sales_count"),
            sum("sale_quantity").alias("total_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            round(avg("product_unit_price"), 2).alias("avg_unit_price"),
            round(avg("product_rating"), 2).alias("avg_rating"),
            max("product_reviews").alias("reviews_count"),
        )
        .withColumn(
            "product_quantity_rank",
            dense_rank().over(Window.orderBy(col("total_quantity").desc())),
        )
        .withColumn(
            "product_revenue_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())),
        )
        .withColumn(
            "category_total_revenue",
            round(sum("total_revenue").over(Window.partitionBy("category_name")), 2),
        )
        .withColumn(
            "category_total_quantity",
            sum("total_quantity").over(Window.partitionBy("category_name")),
        )
        .withColumn(
            "category_products_count",
            count("*").over(Window.partitionBy("category_name")),
        )
        .orderBy(col("total_revenue").desc())
    )

    customer_sales = (
        fact.join(customer, "customer_key")
        .join(geo, "location_key")
        .join(country, "country_key")
        .withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))
        .groupBy("customer_key", "customer_name", "email", "country_name")
        .agg(
            count("*").alias("orders_count"),
            sum("sale_quantity").alias("total_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            round(avg("sale_total_price"), 2).alias("avg_order_value"),
        )
        .withColumn(
            "customer_revenue_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())),
        )
        .withColumn(
            "country_customers_count",
            count("*").over(Window.partitionBy("country_name")),
        )
        .withColumn(
            "country_total_revenue",
            round(sum("total_revenue").over(Window.partitionBy("country_name")), 2),
        )
        .withColumn(
            "country_orders_count",
            sum("orders_count").over(Window.partitionBy("country_name")),
        )
        .orderBy(col("total_revenue").desc())
    )

    month_window = Window.orderBy("year_number", "month_number")
    time_sales = (
        fact.join(date, "date_key")
        .groupBy("year_number", "month_number", "month_name")
        .agg(
            count("*").alias("orders_count"),
            sum("sale_quantity").alias("total_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            round(avg("sale_total_price"), 2).alias("avg_order_value"),
        )
        .withColumn(
            "year_total_revenue",
            round(sum("total_revenue").over(Window.partitionBy("year_number")), 2),
        )
        .withColumn("prev_month_revenue", lag("total_revenue").over(month_window))
        .withColumn(
            "revenue_diff_to_prev_month",
            round(col("total_revenue") - col("prev_month_revenue"), 2),
        )
        .withColumn(
            "month_revenue_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())),
        )
        .orderBy("year_number", "month_number")
    )

    store_sales = (
        fact.join(store, "store_key")
        .join(geo, "location_key")
        .join(country, "country_key")
        .groupBy("store_key", "store_name", "city_name", "state_name", "country_name")
        .agg(
            count("*").alias("orders_count"),
            sum("sale_quantity").alias("total_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            round(avg("sale_total_price"), 2).alias("avg_order_value"),
        )
        .withColumn(
            "store_revenue_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())),
        )
        .withColumn(
            "city_total_revenue",
            round(sum("total_revenue").over(Window.partitionBy("city_name")), 2),
        )
        .withColumn(
            "country_total_revenue",
            round(sum("total_revenue").over(Window.partitionBy("country_name")), 2),
        )
        .withColumn(
            "country_stores_count",
            count("*").over(Window.partitionBy("country_name")),
        )
        .orderBy(col("total_revenue").desc())
    )

    supplier_sales = (
        fact.join(supplier, "supplier_key")
        .join(product, "product_key")
        .join(geo, "location_key")
        .join(country, "country_key")
        .groupBy("supplier_key", "supplier_name", "country_name")
        .agg(
            countDistinct("product_key").alias("products_count"),
            count("*").alias("sales_count"),
            sum("sale_quantity").alias("total_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            round(avg("product_price"), 2).alias("avg_product_price"),
        )
        .withColumn(
            "supplier_revenue_rank",
            dense_rank().over(Window.orderBy(col("total_revenue").desc())),
        )
        .withColumn(
            "country_suppliers_count",
            count("*").over(Window.partitionBy("country_name")),
        )
        .withColumn(
            "country_total_revenue",
            round(sum("total_revenue").over(Window.partitionBy("country_name")), 2),
        )
        .orderBy(col("total_revenue").desc())
    )

    product_quality_base = (
        fact.join(product, "product_key")
        .join(category, "category_key")
        .groupBy("product_key", "product_name", "category_name")
        .agg(
            round(avg("product_rating"), 2).alias("avg_rating"),
            max("product_reviews").alias("reviews_count"),
            sum("sale_quantity").alias("sold_quantity"),
            round(sum("sale_total_price"), 2).alias("total_revenue"),
            min("product_price").alias("min_price"),
            max("product_price").alias("max_price"),
        )
    )
    rating_sales_correlation = product_quality_base.select(
        round(corr("avg_rating", "sold_quantity"), 4).alias("rating_sales_correlation")
    )
    product_quality = (
        product_quality_base.crossJoin(rating_sales_correlation)
        .withColumn(
            "highest_rating_rank",
            dense_rank().over(Window.orderBy(col("avg_rating").desc())),
        )
        .withColumn(
            "lowest_rating_rank",
            dense_rank().over(Window.orderBy(col("avg_rating").asc())),
        )
        .withColumn(
            "reviews_count_rank",
            dense_rank().over(Window.orderBy(col("reviews_count").desc())),
        )
        .withColumn(
            "sold_quantity_rank",
            dense_rank().over(Window.orderBy(col("sold_quantity").desc())),
        )
        .orderBy(col("avg_rating").desc(), col("reviews_count").desc())
    )

    publish(product_sales, "report_product_sales")
    publish(customer_sales, "report_customer_sales")
    publish(time_sales, "report_time_sales")
    publish(store_sales, "report_store_sales")
    publish(supplier_sales, "report_supplier_sales")
    publish(product_quality, "report_product_quality")

    spark.stop()


if __name__ == "__main__":
    main()
