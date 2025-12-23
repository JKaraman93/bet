# src/ingestion/generate_players.py

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random

def generate_player_profiles(spark, config):
    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    random.seed(config.seed)

    df = spark.range(0, config.num_players).withColumnRenamed("id", "player_idx")

    df = (
        df
        .withColumn("player_id", F.concat(F.lit("P"), F.col("player_idx")))
        .withColumn(
            "registration_date",
            F.expr(f"date_add('{config.start_date}', cast(rand({config.seed}) * 120 as int))")
        )
        .withColumn(
            "country",
            F.expr("""
                CASE
                    WHEN rand() < 0.4 THEN 'GR'
                    WHEN rand() < 0.7 THEN 'UK'
                    WHEN rand() < 0.85 THEN 'DE'
                    ELSE 'OTHER'
                END
            """)
        )
        .withColumn(
            "age_bucket",
            F.expr("""
                CASE
                    WHEN rand() < 0.25 THEN '18-24'
                    WHEN rand() < 0.55 THEN '25-34'
                    WHEN rand() < 0.8 THEN '35-44'
                    ELSE '45+'
                END
            """)
        )
        .withColumn(
            "device_type",
            F.expr("CASE WHEN rand() < 0.7 THEN 'mobile' ELSE 'desktop' END")
        )
        .withColumn(
            "acquisition_channel",
            F.expr("""
                CASE
                    WHEN rand() < 0.5 THEN 'organic'
                    WHEN rand() < 0.8 THEN 'paid'
                    ELSE 'affiliate'
                END
            """)
        )
        .withColumn(
            "risk_segment",
            F.expr("""
                CASE
                    WHEN rand() < 0.6 THEN 'low'
                    WHEN rand() < 0.9 THEN 'medium'
                    ELSE 'high'
                END
            """)
        )
        .drop("player_idx")
    )

    return df


# src/ingestion/player_lifecycle.py

from pyspark.sql import functions as F

def assign_lifecycle(df_players):
    return (
        df_players
        .withColumn(
            "lifecycle_stage",
            F.expr("""
                CASE
                    WHEN rand() < 0.6 THEN 'engaged'
                    WHEN rand() < 0.85 THEN 'new'
                    ELSE 'at_risk'
                END
            """)
        )
    )



# src/ingestion/generate_sessions.py

from pyspark.sql import functions as F

def generate_gameplay_sessions(players_df, config):
    date_range = spark.sql(
        f"SELECT explode(sequence(to_date('{config.start_date}'), to_date('{config.end_date}'), interval 1 day)) AS event_date"
    )

    df = players_df.crossJoin(date_range)

    df = (
        df
        .withColumn(
            "daily_sessions",
            F.expr(f"""
                CASE lifecycle_stage
                    WHEN 'engaged' THEN poisson({config.active_lambda})
                    WHEN 'at_risk' THEN poisson({config.at_risk_lambda})
                    ELSE 0
                END
            """)
        )
        .filter(F.col("daily_sessions") > 0)
        .withColumn("session_id", F.expr("uuid()"))
        .withColumn("game_id", F.concat(F.lit("G"), (F.rand() * 200).cast("int")))
        .withColumn("session_duration_sec", (F.rand() * 3600).cast("int"))
        .withColumn("bet_count", (F.rand() * 20).cast("int"))
        .withColumn("total_bet_amount", F.round(F.rand() * 100, 2))
        .withColumn("total_win_amount", F.round(F.rand() * 120, 2))
        .select(
            "session_id",
            "player_id",
            "game_id",
            F.col("event_date").alias("session_date"),
            "session_duration_sec",
            "bet_count",
            "total_bet_amount",
            "total_win_amount",
            "device_type"
        )
    )

    return df


# src/ingestion/generate_transactions.py

from pyspark.sql import functions as F

def generate_transactions(players_df):
    df = (
        players_df
        .sample(fraction=0.4)
        .withColumn("transaction_id", F.expr("uuid()"))
        .withColumn(
            "transaction_type",
            F.expr("CASE WHEN rand() < 0.7 THEN 'deposit' ELSE 'withdrawal' END")
        )
        .withColumn("amount", F.round(F.rand() * 200, 2))
        .withColumn(
            "success_flag",
            F.expr("CASE WHEN rand() < 0.95 THEN true ELSE false END")
        )
        .withColumn("transaction_ts", F.current_timestamp())
        .select(
            "transaction_id",
            "player_id",
            "transaction_ts",
            "transaction_type",
            "amount",
            "success_flag"
        )
    )
    return df


# src/ingestion/generate_churn_labels.py

from pyspark.sql import functions as F

def generate_churn_labels(sessions_df, config):
    last_activity = (
        sessions_df
        .groupBy("player_id")
        .agg(F.max("session_date").alias("last_session_date"))
    )

    labels = (
        last_activity
        .withColumn(
            "churn_7d",
            F.expr(
                f"datediff('{config.end_date}', last_session_date) >= {config.churn_inactivity_days}"
            )
        )
        .withColumn("reference_date", F.lit(config.end_date))
    )

    return labels


def write_bronze(df, path, table_name):
    (
        df
        .write
        .mode("overwrite")
        .partitionBy("session_date" if "session_date" in df.columns else None)
        .parquet(f"{path}/{table_name}")
    )
