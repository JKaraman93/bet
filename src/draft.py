from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random
import utils.config as config
import os

config_ = config.DataGenConfig()
os.environ["SPARK_LOCAL_IP"] = "192.168.182.129"  # replace with your VM IP if needed

# Set logging level to reduce console warnings
spark = SparkSession.builder.appName('app_name').getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 
   

df = spark.range(0, 10)
df = df.withColumn("player_id", F.concat(F.lit("P"), F.col("id")))
df = df.withColumn(
            "registration_date",
            F.expr(f"date_add('{config_.start_date}', cast(rand({config_.seed}) * 120 as int))")
)
df.show()
