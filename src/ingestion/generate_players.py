from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random

def generate_player_profiles(spark, config):
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    random.seed(config.seed)

    df = spark.range(0, config.num_players).withColumnRenamed("id", "player_idx")


    player_id: int 
    registration_date: str 
    country: str 

