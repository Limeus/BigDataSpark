import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
    md5,
    month,
    quarter,
    row_number,
    to_date,
    trim,
    when,
    year,
)


POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/lab2")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

PG_PROPS = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def spark_session():
    return (
        SparkSession.builder.appName("lab2-build-postgres-dw")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def execute_sql(spark, statements):
    jvm = spark.sparkContext._gateway.jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    driver_class = jvm.java.lang.Class.forName("org.postgresql.Driver", True, loader)
    driver = driver_class.newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", POSTGRES_USER)
    props.setProperty("password", POSTGRES_PASSWORD)
    conn = driver.connect(POSTGRES_URL, props)
    try:
        stmt = conn.createStatement()
        try:
            for sql in statements:
                if sql.strip():
                    stmt.execute(sql)
        finally:
            stmt.close()
    finally:
        conn.close()


def clean(name):
    value = trim(col(name).cast("string"))
    return when(value == "", None).otherwise(value)


def nk(*cols):
    return md5(concat_ws("|", *[coalesce(c.cast("string"), lit("")) for c in cols]))


def with_key(df, key_name, order_cols):
    window = Window.orderBy(*[col(c).asc_nulls_last() for c in order_cols])
    return df.withColumn(key_name, row_number().over(window).cast("long"))


def write_pg(df, table):
    (
        df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def create_dw_schema(spark):
    execute_sql(
        spark,
        [
            "DROP SCHEMA IF EXISTS dw CASCADE",
            "CREATE SCHEMA dw",
            """
            CREATE TABLE dw.dim_country (
                country_key bigint PRIMARY KEY,
                country_name text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_geo_location (
                location_key bigint PRIMARY KEY,
                country_key bigint,
                state_name text,
                city_name text,
                postal_code text,
                address_line text,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_pet (
                pet_key bigint PRIMARY KEY,
                pet_type text,
                pet_name text,
                pet_breed text,
                pet_category text,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_customer (
                customer_key bigint PRIMARY KEY,
                source_customer_id integer,
                first_name text,
                last_name text,
                age integer,
                email text,
                location_key bigint,
                pet_key bigint,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_seller (
                seller_key bigint PRIMARY KEY,
                source_seller_id integer,
                first_name text,
                last_name text,
                email text,
                location_key bigint,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_supplier (
                supplier_key bigint PRIMARY KEY,
                supplier_name text,
                contact_name text,
                email text,
                phone text,
                location_key bigint,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_store (
                store_key bigint PRIMARY KEY,
                store_name text,
                phone text,
                email text,
                location_key bigint,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_product_category (
                category_key bigint PRIMARY KEY,
                category_name text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_product_attribute (
                attribute_key bigint PRIMARY KEY,
                pet_category text,
                color_name text,
                size_name text,
                brand_name text,
                material_name text,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_product (
                product_key bigint PRIMARY KEY,
                source_product_id integer,
                product_name text,
                category_key bigint,
                attribute_key bigint,
                product_description text,
                product_price numeric(12, 2),
                available_quantity integer,
                product_weight numeric(12, 2),
                product_rating numeric(3, 1),
                product_reviews integer,
                release_date date,
                expiry_date date,
                natural_key text NOT NULL UNIQUE
            )
            """,
            """
            CREATE TABLE dw.dim_date (
                date_key integer PRIMARY KEY,
                full_date date NOT NULL UNIQUE,
                day_of_month integer NOT NULL,
                month_number integer NOT NULL,
                quarter_number integer NOT NULL,
                year_number integer NOT NULL,
                day_of_week integer NOT NULL,
                day_name text NOT NULL,
                month_name text NOT NULL
            )
            """,
            """
            CREATE TABLE dw.fact_sales (
                sale_key bigint PRIMARY KEY,
                source_row_id bigint NOT NULL UNIQUE,
                date_key integer NOT NULL,
                customer_key bigint NOT NULL,
                seller_key bigint NOT NULL,
                product_key bigint NOT NULL,
                store_key bigint NOT NULL,
                supplier_key bigint NOT NULL,
                source_sale_customer_id integer,
                source_sale_seller_id integer,
                source_sale_product_id integer,
                sale_quantity integer NOT NULL,
                sale_total_price numeric(12, 2) NOT NULL,
                product_unit_price numeric(12, 2)
            )
            """,
        ],
    )


def main():
    spark = spark_session()
    create_dw_schema(spark)

    raw = spark.read.jdbc(POSTGRES_URL, "staging.mock_data", properties=PG_PROPS).cache()

    countries = with_key(
        raw.select(clean("customer_country").alias("country_name"))
        .union(raw.select(clean("seller_country")))
        .union(raw.select(clean("store_country")))
        .union(raw.select(clean("supplier_country")))
        .where(col("country_name").isNotNull())
        .distinct(),
        "country_key",
        ["country_name"],
    ).cache()

    customer_geo = raw.select(
        clean("customer_country").alias("country_name"),
        lit(None).cast("string").alias("state_name"),
        lit(None).cast("string").alias("city_name"),
        clean("customer_postal_code").alias("postal_code"),
        lit(None).cast("string").alias("address_line"),
    )
    seller_geo = raw.select(
        clean("seller_country"),
        lit(None).cast("string"),
        lit(None).cast("string"),
        clean("seller_postal_code"),
        lit(None).cast("string"),
    )
    store_geo = raw.select(
        clean("store_country"),
        clean("store_state"),
        clean("store_city"),
        lit(None).cast("string"),
        clean("store_location"),
    )
    supplier_geo = raw.select(
        clean("supplier_country"),
        lit(None).cast("string"),
        clean("supplier_city"),
        lit(None).cast("string"),
        clean("supplier_address"),
    )
    geo = (
        customer_geo.union(seller_geo)
        .union(store_geo)
        .union(supplier_geo)
        .toDF("country_name", "state_name", "city_name", "postal_code", "address_line")
        .where(
            col("country_name").isNotNull()
            | col("state_name").isNotNull()
            | col("city_name").isNotNull()
            | col("postal_code").isNotNull()
            | col("address_line").isNotNull()
        )
        .distinct()
        .withColumn(
            "natural_key",
            nk(
                col("country_name"),
                col("state_name"),
                col("city_name"),
                col("postal_code"),
                col("address_line"),
            ),
        )
        .join(countries, "country_name", "left")
    )
    geo = with_key(
        geo.select(
            "country_key",
            "state_name",
            "city_name",
            "postal_code",
            "address_line",
            "natural_key",
        ),
        "location_key",
        ["natural_key"],
    ).cache()

    pets = with_key(
        raw.select(
            clean("customer_pet_type").alias("pet_type"),
            clean("customer_pet_name").alias("pet_name"),
            clean("customer_pet_breed").alias("pet_breed"),
            clean("pet_category").alias("pet_category"),
        )
        .distinct()
        .withColumn(
            "natural_key",
            nk(col("pet_type"), col("pet_name"), col("pet_breed"), col("pet_category")),
        ),
        "pet_key",
        ["natural_key"],
    ).cache()

    categories = with_key(
        raw.select(clean("product_category").alias("category_name"))
        .where(col("category_name").isNotNull())
        .distinct(),
        "category_key",
        ["category_name"],
    ).cache()

    attributes = with_key(
        raw.select(
            clean("pet_category").alias("pet_category"),
            clean("product_color").alias("color_name"),
            clean("product_size").alias("size_name"),
            clean("product_brand").alias("brand_name"),
            clean("product_material").alias("material_name"),
        )
        .distinct()
        .withColumn(
            "natural_key",
            nk(
                col("pet_category"),
                col("color_name"),
                col("size_name"),
                col("brand_name"),
                col("material_name"),
            ),
        ),
        "attribute_key",
        ["natural_key"],
    ).cache()

    customer_geo_nk = nk(clean("customer_country"), lit(None), lit(None), clean("customer_postal_code"), lit(None))
    seller_geo_nk = nk(clean("seller_country"), lit(None), lit(None), clean("seller_postal_code"), lit(None))
    store_geo_nk = nk(clean("store_country"), clean("store_state"), clean("store_city"), lit(None), clean("store_location"))
    supplier_geo_nk = nk(clean("supplier_country"), lit(None), clean("supplier_city"), lit(None), clean("supplier_address"))
    pet_nk = nk(clean("customer_pet_type"), clean("customer_pet_name"), clean("customer_pet_breed"), clean("pet_category"))
    attr_nk = nk(clean("pet_category"), clean("product_color"), clean("product_size"), clean("product_brand"), clean("product_material"))

    customers = (
        raw.withColumn("location_nk", customer_geo_nk)
        .withColumn("pet_nk", pet_nk)
        .join(geo.select("location_key", col("natural_key").alias("location_nk")), "location_nk")
        .join(pets.select("pet_key", col("natural_key").alias("pet_nk")), "pet_nk")
        .select(
            col("sale_customer_id").alias("source_customer_id"),
            clean("customer_first_name").alias("first_name"),
            clean("customer_last_name").alias("last_name"),
            col("customer_age").cast("integer").alias("age"),
            clean("customer_email").alias("email"),
            "location_key",
            "pet_key",
        )
        .distinct()
        .withColumn(
            "natural_key",
            nk(
                col("source_customer_id"),
                col("first_name"),
                col("last_name"),
                col("age"),
                col("email"),
                col("location_key"),
                col("pet_key"),
            ),
        )
    )
    customers = with_key(customers, "customer_key", ["natural_key"]).cache()

    sellers = (
        raw.withColumn("location_nk", seller_geo_nk)
        .join(geo.select("location_key", col("natural_key").alias("location_nk")), "location_nk")
        .select(
            col("sale_seller_id").alias("source_seller_id"),
            clean("seller_first_name").alias("first_name"),
            clean("seller_last_name").alias("last_name"),
            clean("seller_email").alias("email"),
            "location_key",
        )
        .distinct()
        .withColumn(
            "natural_key",
            nk(
                col("source_seller_id"),
                col("first_name"),
                col("last_name"),
                col("email"),
                col("location_key"),
            ),
        )
    )
    sellers = with_key(sellers, "seller_key", ["natural_key"]).cache()

    suppliers = (
        raw.withColumn("location_nk", supplier_geo_nk)
        .join(geo.select("location_key", col("natural_key").alias("location_nk")), "location_nk")
        .select(
            clean("supplier_name").alias("supplier_name"),
            clean("supplier_contact").alias("contact_name"),
            clean("supplier_email").alias("email"),
            clean("supplier_phone").alias("phone"),
            "location_key",
        )
        .distinct()
        .withColumn(
            "natural_key",
            nk(col("supplier_name"), col("contact_name"), col("email"), col("phone"), col("location_key")),
        )
    )
    suppliers = with_key(suppliers, "supplier_key", ["natural_key"]).cache()

    stores = (
        raw.withColumn("location_nk", store_geo_nk)
        .join(geo.select("location_key", col("natural_key").alias("location_nk")), "location_nk")
        .select(
            clean("store_name").alias("store_name"),
            clean("store_phone").alias("phone"),
            clean("store_email").alias("email"),
            "location_key",
        )
        .distinct()
        .withColumn("natural_key", nk(col("store_name"), col("phone"), col("email"), col("location_key")))
    )
    stores = with_key(stores, "store_key", ["natural_key"]).cache()

    product_base = (
        raw.withColumn("attr_nk", attr_nk)
        .join(categories, clean("product_category") == categories.category_name)
        .join(attributes.select("attribute_key", col("natural_key").alias("attr_nk")), "attr_nk")
        .select(
            col("sale_product_id").alias("source_product_id"),
            clean("product_name").alias("product_name"),
            "category_key",
            "attribute_key",
            clean("product_description").alias("product_description"),
            col("product_price").alias("product_price"),
            col("product_quantity").cast("integer").alias("available_quantity"),
            col("product_weight").alias("product_weight"),
            col("product_rating").alias("product_rating"),
            col("product_reviews").cast("integer").alias("product_reviews"),
            to_date(col("product_release_date"), "M/d/yyyy").alias("release_date"),
            to_date(col("product_expiry_date"), "M/d/yyyy").alias("expiry_date"),
        )
        .distinct()
        .withColumn(
            "natural_key",
            nk(
                col("source_product_id"),
                col("product_name"),
                col("category_key"),
                col("attribute_key"),
                col("product_description"),
                col("product_price"),
                col("available_quantity"),
                col("product_weight"),
                col("product_rating"),
                col("product_reviews"),
                col("release_date"),
                col("expiry_date"),
            ),
        )
    )
    products = with_key(product_base, "product_key", ["natural_key"]).cache()

    dates = with_key(
        raw.select(to_date(col("sale_date"), "M/d/yyyy").alias("full_date"))
        .where(col("full_date").isNotNull())
        .distinct()
        .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("integer"))
        .withColumn("day_of_month", dayofmonth(col("full_date")))
        .withColumn("month_number", month(col("full_date")))
        .withColumn("quarter_number", quarter(col("full_date")))
        .withColumn("year_number", year(col("full_date")))
        .withColumn("day_of_week", dayofweek(col("full_date")))
        .withColumn("day_name", date_format(col("full_date"), "EEEE"))
        .withColumn("month_name", date_format(col("full_date"), "MMMM")),
        "unused_key",
        ["full_date"],
    ).drop("unused_key")

    fact_seed = (
        raw.withColumn("sale_dt", to_date(col("sale_date"), "M/d/yyyy"))
        .withColumn("customer_location_nk", customer_geo_nk)
        .withColumn("seller_location_nk", seller_geo_nk)
        .withColumn("store_location_nk", store_geo_nk)
        .withColumn("supplier_location_nk", supplier_geo_nk)
        .withColumn("pet_nk", pet_nk)
        .withColumn("attr_nk", attr_nk)
    )
    fact_seed = (
        fact_seed.join(geo.select(col("location_key").alias("customer_location_key"), col("natural_key").alias("customer_location_nk")), "customer_location_nk")
        .join(geo.select(col("location_key").alias("seller_location_key"), col("natural_key").alias("seller_location_nk")), "seller_location_nk")
        .join(geo.select(col("location_key").alias("store_location_key"), col("natural_key").alias("store_location_nk")), "store_location_nk")
        .join(geo.select(col("location_key").alias("supplier_location_key"), col("natural_key").alias("supplier_location_nk")), "supplier_location_nk")
        .join(pets.select("pet_key", col("natural_key").alias("pet_nk")), "pet_nk")
        .join(categories, clean("product_category") == categories.category_name)
        .join(attributes.select("attribute_key", col("natural_key").alias("attr_nk")), "attr_nk")
        .withColumn("customer_nk", nk(col("sale_customer_id"), clean("customer_first_name"), clean("customer_last_name"), col("customer_age"), clean("customer_email"), col("customer_location_key"), col("pet_key")))
        .withColumn("seller_nk", nk(col("sale_seller_id"), clean("seller_first_name"), clean("seller_last_name"), clean("seller_email"), col("seller_location_key")))
        .withColumn("store_nk", nk(clean("store_name"), clean("store_phone"), clean("store_email"), col("store_location_key")))
        .withColumn("supplier_nk", nk(clean("supplier_name"), clean("supplier_contact"), clean("supplier_email"), clean("supplier_phone"), col("supplier_location_key")))
        .withColumn("product_nk", nk(col("sale_product_id"), clean("product_name"), col("category_key"), col("attribute_key"), clean("product_description"), col("product_price"), col("product_quantity"), col("product_weight"), col("product_rating"), col("product_reviews"), to_date(col("product_release_date"), "M/d/yyyy"), to_date(col("product_expiry_date"), "M/d/yyyy")))
    )
    facts = (
        fact_seed.join(dates.select("date_key", "full_date"), fact_seed.sale_dt == dates.full_date)
        .join(customers.select("customer_key", col("natural_key").alias("customer_nk")), "customer_nk")
        .join(sellers.select("seller_key", col("natural_key").alias("seller_nk")), "seller_nk")
        .join(stores.select("store_key", col("natural_key").alias("store_nk")), "store_nk")
        .join(suppliers.select("supplier_key", col("natural_key").alias("supplier_nk")), "supplier_nk")
        .join(products.select("product_key", col("natural_key").alias("product_nk")), "product_nk")
        .select(
            row_number().over(Window.orderBy("raw_source_id")).cast("long").alias("sale_key"),
            col("raw_source_id").cast("long").alias("source_row_id"),
            "date_key",
            "customer_key",
            "seller_key",
            "product_key",
            "store_key",
            "supplier_key",
            col("sale_customer_id").cast("integer").alias("source_sale_customer_id"),
            col("sale_seller_id").cast("integer").alias("source_sale_seller_id"),
            col("sale_product_id").cast("integer").alias("source_sale_product_id"),
            col("sale_quantity").cast("integer").alias("sale_quantity"),
            col("sale_total_price").alias("sale_total_price"),
            col("product_price").alias("product_unit_price"),
        )
    )

    write_pg(countries.select("country_key", "country_name"), "dw.dim_country")
    write_pg(geo.select("location_key", "country_key", "state_name", "city_name", "postal_code", "address_line", "natural_key"), "dw.dim_geo_location")
    write_pg(pets.select("pet_key", "pet_type", "pet_name", "pet_breed", "pet_category", "natural_key"), "dw.dim_pet")
    write_pg(customers.select("customer_key", "source_customer_id", "first_name", "last_name", "age", "email", "location_key", "pet_key", "natural_key"), "dw.dim_customer")
    write_pg(sellers.select("seller_key", "source_seller_id", "first_name", "last_name", "email", "location_key", "natural_key"), "dw.dim_seller")
    write_pg(suppliers.select("supplier_key", "supplier_name", "contact_name", "email", "phone", "location_key", "natural_key"), "dw.dim_supplier")
    write_pg(stores.select("store_key", "store_name", "phone", "email", "location_key", "natural_key"), "dw.dim_store")
    write_pg(categories.select("category_key", "category_name"), "dw.dim_product_category")
    write_pg(attributes.select("attribute_key", "pet_category", "color_name", "size_name", "brand_name", "material_name", "natural_key"), "dw.dim_product_attribute")
    write_pg(products.select("product_key", "source_product_id", "product_name", "category_key", "attribute_key", "product_description", "product_price", "available_quantity", "product_weight", "product_rating", "product_reviews", "release_date", "expiry_date", "natural_key"), "dw.dim_product")
    write_pg(dates.select("date_key", "full_date", "day_of_month", "month_number", "quarter_number", "year_number", "day_of_week", "day_name", "month_name"), "dw.dim_date")
    write_pg(facts, "dw.fact_sales")

    execute_sql(
        spark,
        [
            "CREATE INDEX idx_fact_sales_date_key ON dw.fact_sales(date_key)",
            "CREATE INDEX idx_fact_sales_customer_key ON dw.fact_sales(customer_key)",
            "CREATE INDEX idx_fact_sales_product_key ON dw.fact_sales(product_key)",
            "CREATE INDEX idx_fact_sales_store_key ON dw.fact_sales(store_key)",
        ],
    )

    print("PostgreSQL DW build finished")
    spark.stop()


if __name__ == "__main__":
    main()
