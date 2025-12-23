from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random


spark = SparkSession.builder.appName('app_name').getOrCreate()
    

df = spark.range(0, 10)
df.show()
