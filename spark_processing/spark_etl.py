from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, first, date_format
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[1]") \
    .appName("CS649-Final-Project") \
    .config("spark.jars", "../jars/postgresql-42.3.5.jar") \
    .getOrCreate()


def clean_data(spdf):
    df = spdf
    print("cleaning data")

    print("Removing rows with null timestamp")
    df = df.na.drop(subset=['unix', 'date', 'just_date'])

    print("Replacing null ""symbol"" values with first symbol or NA")
    f = df.select(first("symbol")).collect()
    f = f[0][0]
    if f is NullType:
        df = df.fillna("NA", subset=['symbol'])
    else:
        df = df.fillna(str(f), subset=['symbol'])

    print("Replacing null values for rest of the dataframe with 0")
    df = df.fillna(0)

    currency_volume_column = df.columns[-3]
    currency_volume_column_usd = df.columns[-2]

    print("Replacing negative values with 0")
    df = df.withColumn(
        "open",
        F.when(df["open"] < 0, 0).when(F.col("open").isNull(), 0).otherwise(F.col("open")), )

    df = df.withColumn(
        "high",
        F.when(df["high"] < 0, 0).when(F.col("high").isNull(), 0).otherwise(F.col("high")), )

    df = df.withColumn(
        "low",
        F.when(df["low"] < 0, 0).when(F.col("low").isNull(), 0).otherwise(F.col("low")), )

    df = df.withColumn(
        "close",
        F.when(df["close"] < 0, 0).when(F.col("close").isNull(), 0).otherwise(F.col("close")), )

    df = df.withColumn(
        str(currency_volume_column),
        F.when(df[str(currency_volume_column)] < 0, 0).when(F.col(str(currency_volume_column)).isNull(), 0).otherwise(
            F.col(str(currency_volume_column))), )

    df = df.withColumn(
        str(currency_volume_column_usd),
        F.when(df[str(currency_volume_column_usd)] < 0, 0).when(F.col(str(currency_volume_column_usd)).isNull(),
                                                                0).otherwise(F.col(str(currency_volume_column_usd))), )

    print("Changing into correct data type")
    df = df.withColumn("unix", df.unix.cast(LongType()))
    df = df.withColumn("open", df.open.cast(FloatType()))
    df = df.withColumn("high", df.high.cast(FloatType()))
    df = df.withColumn("low", df.low.cast(FloatType()))
    df = df.withColumn("close", df.close.cast(FloatType()))
    df = df.withColumn(str(currency_volume_column), df[str(currency_volume_column)].cast(FloatType()))
    df = df.withColumn(str(currency_volume_column_usd), df[str(currency_volume_column_usd)].cast(FloatType()))
    df = df.withColumn('just_date', F.to_date('just_date', 'yyyy-MM-dd'))
    df = df.withColumnRenamed(str(currency_volume_column), "volume-coin") \
        .withColumnRenamed(str(currency_volume_column_usd), "volume-usd")
    df.show(100)
    print(df.dtypes)

    return df


def split_timestamp(cleaned_df):
    # Splits timestamp to date and time
    df = cleaned_df
    split_col = F.split(df['date'], ' ')
    df = df.withColumn('just_time', F.concat(split_col.getItem(1)))
    df = df.withColumn('just_time', date_format('just_time', 'HH:mm:ss'))
    df.show(100)
    return df


def aggregate_date(data):
    df = data
    df = df.orderBy(col('unix'))
    df_agg = df.groupBy(col('just_date'), col('symbol')) \
        .agg(F.first('open').alias("Open"),

             F.avg('high').alias("Avg High"),
             F.percentile_approx("high", 0.5).alias("Median High"),

             F.avg('low').alias("Avg Low"),
             F.percentile_approx("low", 0.5).alias("Median Low"),

             F.last('close').alias("Close"),

             F.sum('volume-coin').alias("Total Volume-Coin"),

             F.sum('volume-usd').alias("Total Volume-USD")
             )

    return df_agg


def write_to_warehouse(data, historical, DB_NAME, file_type):
    df = data
    if historical:
        # Writes Historical Data
        table_name = file_type + "_historical_coin_data"

        df.select("unix", "date", "symbol", "open", "high", "low", "close", "volume-coin", "volume-usd", "just_date",
                  "just_time"). \
            write.format("jdbc") \
            .mode("append") \
            .option("url",
                    "jdbc:postgresql://crypto-warehouse.cdvabacdldda.us-west-1.rds.amazonaws.com:5432/" + str(DB_NAME)) \
            .option("driver", "org.postgresql.Driver").option("dbtable", str(table_name)) \
            .option("user", "postgres").option("password", "postgres").save()

    else:
        # Writes Aggregated Data
        TABLE_NAME = "daily_coin_data"
        df.select("just_date", "symbol", "Open", "Avg High", "Median High",
                  "Avg Low", "Median Low", "Close",
                  "Total Volume-Coin",
                  "Total Volume-USD") \
            .write.format("jdbc") \
            .mode("append") \
            .option("url",
                    "jdbc:postgresql://crypto-warehouse.cdvabacdldda.us-west-1.rds.amazonaws.com:5432/" + str(DB_NAME)) \
            .option("driver", "org.postgresql.Driver").option("dbtable", str(TABLE_NAME)) \
            .option("user", "postgres").option("password", "postgres").save()


def process_files(files, file_type):
    spark_df = spark.read.option("header", True) \
        .csv(files)
    cleaned_df = clean_data(spark_df)

    df_with_time = split_timestamp(cleaned_df)
    print(df_with_time.schema)

    agg_data = aggregate_date(df_with_time)
    print("Aggregated data")
    agg_data.show(100)

    print("Writing historical data to warehouse")
    write_to_warehouse(df_with_time, True, "historical", file_type)

    print("Writing aggregated data to warehouse")
    write_to_warehouse(agg_data, False, "Aggregated_Data", file_type)

    # spark_df.show(100)
    return


def helloworld():
    print(" hello world ")