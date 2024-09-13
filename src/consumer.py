# import json
import logging
import os
from datetime import datetime

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Initialiser le logging et rediriger les logs vers un fichier
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s",
    filename="./app.log",  # Nom du fichier de log
    filemode="a",
)

# Créer un logger
logger = logging.getLogger("spark_structured_streaming")


findspark.init()


scala_version = "2.12"
spark_version = "3.1.2"
# I use the following conf, solved the problem, and works perfectly
packages = [
    f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}",
    "org.apache.kafka:kafka-clients:3.2.1",
]

try:
    spark = (
        SparkSession.builder.master("local")
        .appName("kafka-example")
        .config("spark.jars.packages", ",".join(packages))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session initialized successfully")

except Exception as e:
    logger.error(f"Spark session initialization failed. Error: {e}")


class Run_consumer:
    def __init__(self, server: str, topic: str, output_folder: str) -> None:
        self.server = server
        # "localhost:9092,"
        self.topic = topic
        # "test_madjid"
        self.output_folder = output_folder

    def save_data(self, df, batch_id: int):
        print(batch_id)
        desired_value = df.select("value").collect()[0]["value"]
        print(desired_value.id_flight)
        result_dict = {
            "id_flight": desired_value.id_flight,
            "airline_icao": desired_value.airline_icao,
            "aeroport_origin_iata": desired_value.aeroport_origin_iata,
            "aeroport_origin_icao": desired_value.aeroport_origin_icao,
            "aeroport_destination_iata": desired_value.aeroport_destination_iata,
            "aeroport_destination_icao": desired_value.aeroport_destination_icao,
            "live": desired_value.live,
            "departure": desired_value.departure,
            "arrival": desired_value.arrival,
            "offsetHours": desired_value.offsetHours,
            "offset": desired_value.offset,
        }
        # df = desired_value
        # df = df.drop("value")
        # for key, value in result_dict.items():
        #     df = df.withColumn(key, lit(value))

        # Get today's date
        today = datetime.today()

        # Create directory path

        directory = "Flights/year={year}/month={month}/day={day}".format(
            year=today.year, month=today.month, day=today.day
        )
        full_path = os.path.join(self.output_folder, directory)
        os.makedirs(full_path, exist_ok=True)

        # df.write.parquet(
        #     os.path.join(
        #         full_path, "flight_" + str(desired_value.id_flight) + ".parquet"
        #     )
        # )

        # Show the resulting DataFrame
        import pandas as pd

        df = pd.DataFrame(result_dict, index=[0])
        df.to_parquet(
            os.path.join(
                full_path, "flight_" + str(desired_value.id_flight) + ".parquet"
            )
        )

        # with open(
        #     os.path.join(full_path, "flight_" + str(desired_value.id_flight) + ".json"),
        #     "a",
        # ) as json_file:
        #     json.dump(result_dict, json_file)

    def transform_streaming_data(self):
        try:
            df = (
                spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", self.server)
                .option("subscribe", self.topic)
                .option("startingOffsets", "earliest")
                .load()
            )

            flight_schema = StructType(
                [
                    StructField("id_flight", StringType(), True),
                    StructField("airline_icao", StringType(), True),
                    StructField("aeroport_origin_iata", StringType(), True),
                    StructField("aeroport_origin_icao", StringType(), True),
                    StructField("aeroport_destination_iata", StringType(), True),
                    StructField("aeroport_destination_icao", StringType(), True),
                    StructField(
                        "live", BooleanType(), True
                    ),  # Corrected: live is a boolean
                    StructField(
                        "departure", IntegerType(), True
                    ),  # Corrected: departure is an int
                    StructField(
                        "arrival", IntegerType(), True
                    ),  # Corrected: arrival is an int
                    StructField(
                        "offsetHours", StringType(), True
                    ),  # Corrected: offsetHours is a float
                    StructField(
                        "offset", IntegerType(), True
                    ),  # Corrected: offset is an int
                ]
            )

            parsed_df = df.withColumn("value", expr("cast(value as string)"))
            parsed_df = parsed_df.select(
                from_json(col("value"), flight_schema).alias("value")
            )

            parsed_df.printSchema()
            query = (
                parsed_df.writeStream.foreachBatch(self.save_data)
                .start()
                .awaitTermination()
            )
            logger.info("Streaming dataframe fetched successfully")

        except Exception as e:
            logger.warning(f"Failed to fetch and save streaming dataframe. Error: {e}")
