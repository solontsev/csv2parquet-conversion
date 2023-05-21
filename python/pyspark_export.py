from pyspark.sql import SparkSession
from pyspark.sql.functions import StructType, count, desc
from pyspark.sql.types import LongType, StringType
import time

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName("Test")\
        .getOrCreate()

    schema = StructType()\
        .add("id", LongType(), False)\
        .add("name", StringType(), False)\
        .add("created_utc", LongType(), False)\
        .add("updated_on", LongType(), False)\
        .add("comment_karma", LongType(), False)\
        .add("link_karma", LongType(), False)

    df = spark\
        .read\
        .option("header", True)\
        .option("delimiter", ',')\
        .schema(schema)\
        .csv("69M_reddit_accounts.csv")

    start_time = time.time()
    df\
        .coalesce(1)\
        .write\
        .parquet(path='69M_reddit_accounts_pyspark_snappy.parquet', mode='overwrite', compression='snappy')
    print("Elapsed time: %s seconds" % (time.time() - start_time))
