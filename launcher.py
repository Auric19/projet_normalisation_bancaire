from pyspark.sql import SparkSession
from normalisation import run
from rules import applyRules


def start():
    db_properties = {'user': "postgres", 'password': "Darkauric",
                     'url': "jdbc:postgresql://localhost:5433/postgres",
                     'driver': "org.postgresql.Driver"}
    spark = SparkSession.builder.master("local[*]").appName("Test_Spark").config("spark.jars",
                                                                                 "postgresql-42.2.20.jar").getOrCreate()
    df = spark.read.option("header", "true").csv("data.csv")
    normalized_payment = run(df)
    applyRules(normalized_payment, db_properties)
    # normalized_payment.write
    normalized_payment.write.jdbc(url=db_properties['url'], table="normalized_payment", mode='overwrite',
                                  properties=db_properties)


if __name__ == '__main__':
    start()
