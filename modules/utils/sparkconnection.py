from pyspark import SQLContext
from pyspark.sql import SparkSession


class sparkconnection:
        spark = SparkSession.builder \
            .appName("spark connnection") \
            .config("spark.num.executors", "24") \
            .config("spark.driver.memory", "88g") \
            .config("spark.executor.memory", "88g") \
            .config("spark.driver.cores", "12") \
            .config("spark.executor.cores", "12") \
            .config("spark.scheduler.mode", "FAIR") \
            .getOrCreate()
        sc = spark.sparkContext
        sqlContext = SQLContext(sc)

        # need to get the app name to the above somehow .shouldn't be a static string
        def test(self):
            pass