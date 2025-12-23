from pyspark.sql import SparkSession

def get_spark(app_name: str = 'SyntheticDataGenerator'):
    return ( SparkSession.appName(app_name).getOrCreate())
