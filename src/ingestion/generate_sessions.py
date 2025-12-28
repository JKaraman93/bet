from pyspark.sql import functions as F

def generate_gameplay_sessions(players_df, spark, config): 
    date_range = spark.sql(
        f"SELECT explode(sequence(to_date('{config.start_date}'), to_date('{config.end_date}'), interval 1 day)) AS event_date"
    )

    print (date_range)

    df = players_df.crossJoin(date_range)
    '''
    df = df.withColumn(
        "daily_sessions",
        F.when(
            F.col("lifecycle_stage") == "engaged",
            F.floor(-F.log(1 - F.rand(seed=42)) * F.col("active_lambda"))
        ).when(
            F.col("lifecycle_stage") == "at_risk",
            F.floor(-F.log(1 - F.rand(seed=42)) * F.col("at_risk_lambda"))
        ).otherwise(0)
    )
    '''
                
    return df
    