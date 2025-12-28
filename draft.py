from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random
import src.utils.config as config
import os
from src.ingestion.generate_players import generate_player_profiles
from src.ingestion.player_lifecycle import assign_lifecycle
from src.ingestion.generate_sessions import generate_gameplay_sessions

config_ = config.DataGenConfig()
os.environ["SPARK_LOCAL_IP"] = "192.168.182.129"  # replace with your VM IP if needed

# Set logging level to reduce console warnings
spark = SparkSession.builder.appName('app_name').getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 

df = generate_player_profiles(spark, config_)
df = assign_lifecycle(df)
df_sessions = generate_gameplay_sessions(df, spark, config_)

print (df_sessions)