from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transform").getOrCreate()

def transform(product_dimension, sales_dimension, store_dimension, time_dimension, sales_fact):
    # ------------------------
    # product_dimension Transformations
    # ------------------------
    product_dimension = spark.createDataFrame(product_dimension)
    sales_dimension = spark.createDataFrame(sales_dimension)
    store_dimension = spark.createDataFrame(store_dimension)
    time_dimension = spark.createDataFrame(time_dimension)
    sales_fact = spark.createDataFrame(sales_fact)
    product_dimension = product_dimension.withColumn(
        "price", F.round(F.col("price"), 2)
    )

    # ------------------------
    # sales_dimension Transformations
    # ------------------------
    sales_dimension = (
        sales_dimension
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
    )

    # ------------------------
    # sales_fact Transformations
    # ------------------------
    sales_fact = (
        sales_fact
        .withColumn("discount_applied", F.round(F.col("discount_applied"), 2))
        .withColumn("tax_amount", F.round(F.col("tax_amount"), 2))
        .withColumn("net_amount", F.round(F.col("net_amount"), 2))
        .withColumn("total_amount", F.round(F.col("total_amount"), 2))
        # Uncomment if needed
        # .withColumn("customer_id", F.col("customer_id").cast("string"))
        # .withColumn("payment_method", F.col("payment_method").cast("string"))
        # .withColumn("transaction_type", F.col("transaction_type").cast("string"))
    )

    # ------------------------
    # store_dimension
    # ------------------------
    store_dimension = store_dimension.withColumn(
        "opening_date", F.to_date("opening_date")
    )

    # ------------------------
    # time_dimension
    # ------------------------
    time_dimension = time_dimension.withColumn(
        "date", F.to_date("date")
    )

    product_dimension = product_dimension.toPandas()
    sales_dimension = sales_dimension.toPandas()
    store_dimension = store_dimension.toPandas()
    time_dimension = time_dimension.toPandas()
    sales_fact = sales_fact.toPandas()
    return [product_dimension, sales_dimension, store_dimension, time_dimension, sales_fact]
