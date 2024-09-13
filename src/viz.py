from datetime import datetime, timezone
from typing import List

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, floor

spark = SparkSession.builder.appName("FlightData").getOrCreate()


def get_comp_most_fly(file_path_Airlines: str, file_path_Flights: List[str]):
    """return a dataframe for the airline with the most ongoing flights


    Args:
        file_path_Airlines (str): path to airlines data
        file_path_Flights (List[str]): path to flights data of two last days

    Returns:
        _type_: sprak df
    """
    liste = []
    # Read data from the files
    for file_path in file_path_Flights:
        flights_df = spark.read.parquet(file_path)
        liste.append(flights_df)
    airlines_df_sc = spark.read.csv(file_path_Airlines, sep=",", header=True)

    if len(liste) == 2:
        flights_df_sc = liste[0].union(liste[1])
    else:
        flights_df_sc = liste[0]
    # flights_df_sc.filter(flights_df_sc["day"] == 31).show()

    ##FIlter by live flight when i save data
    df_1 = flights_df_sc.filter(flights_df_sc["live"] == True)
    time_00 = datetime.now(timezone.utc)
    df_2 = df_1.filter((time_00.timestamp() + col("offset")) < col("arrival"))
    df_3 = df_2.groupBy("airline_icao")
    df_4 = df_3.agg(F.count("*").alias("Nombre de vol"))
    result_df = df_4.join(
        airlines_df_sc, df_4.airline_icao == airlines_df_sc.ICAO, "left"
    ).select(col("Name"), col("airline_icao"), col("Nombre de vol"))
    return result_df.orderBy(col("Nombre de vol").desc())


def get_comp_most_flight_in_same_continent(
    file_path_Airports: str, file_path_Flights: List[str], file_path_Airlines: str
):
    """return a dataframe that contain each continent
    with the airline that has the most active regional
    flights (origin continent == destination continent)

    Args:
        file_path_Airlines (str): path to airlines data
        file_path_Flights (List[str]): path to flights data of two last days


    Returns:
        _type_: sprak df
    """
    liste = []
    # Read data from the files
    for file_path in file_path_Flights:
        flights_df = spark.read.parquet(file_path)
        liste.append(flights_df)

    if len(liste) == 2:
        flights_df_sc = liste[0].union(liste[1])
    else:
        flights_df_sc = liste[0]

    airlines_df_sc = spark.read.csv(file_path_Airlines, sep=",", header=True)
    airports_df_sc = spark.read.csv(file_path_Airports, sep=",", header=True)

    # we should get continent for airport origine and destination in two df
    df_org = (
        flights_df_sc.join(
            airports_df_sc,
            (flights_df_sc.aeroport_origin_icao == airports_df_sc.icao),
            "left",
        )
        .withColumnRenamed("continent", "continent_original")
        .select(
            col("id_flight"),
            col("airline_icao"),
            col("Name"),
            col("continent_original"),
        )
    )
    df_dest = (
        flights_df_sc.join(
            airports_df_sc,
            (flights_df_sc.aeroport_destination_icao == airports_df_sc.icao),
            "left",
        )
        .withColumnRenamed("continent", "continent_destination")
        .select(col("id_flight"), col("continent_destination"))
    )

    # Get the flight with same continent destination
    df_1 = df_org.join(df_dest, df_org.id_flight == df_dest.id_flight, "inner")
    df_2 = df_1.filter(col("continent_original") == col("continent_destination"))
    df_3 = df_2.groupBy("continent_original", "airline_icao")

    df_4 = df_3.agg(F.count("*").alias("Nombre de vol"))
    df_5 = df_4.join(
        airlines_df_sc, df_4.airline_icao == airlines_df_sc.ICAO, "left"
    ).select(col("Name"), col("airline_icao"), col("Nombre de vol"))
    result_df = df_5.withColumnRenamed("continent_original", "continent")
    return result_df.orderBy(col("Nombre de vol").desc())


def get_fly_longest_dist(file_path_Flights: List[str]):
    """return a dataframe that contain the ongoing flight with the longest route

    Returns:
        _type_: spark df
    """
    liste = []
    # Read data from the files
    for file_path in file_path_Flights:
        flights_df = spark.read.parquet(file_path)
        liste.append(flights_df)
    if len(liste) == 2:
        flights_df_sc = liste[0].union(liste[1])
    else:
        flights_df_sc = liste[0]

    ##FIlter by live flight when i save data
    df_1 = flights_df_sc.filter(flights_df_sc["live"] == True)
    # get current time and filter flight that not yet arrived now with offset < arrival
    current_time = datetime.now(timezone.utc)
    df_2 = df_1.filter((current_time.timestamp() + col("offset")) < col("arrival"))
    df_3 = (
        df_2.withColumn("total duration in seconds", col("arrival") - col("departure"))
        .withColumn("duration in hours", floor(col("total duration in seconds") / 3600))
        .withColumn(
            "duration in minutes", floor(col("total duration in seconds") % 3600 / 60)
        )
        .withColumn(
            "duration in secondes", floor(col("total duration in seconds") % 3600 % 60)
        )
    )
    result_df = df_3.orderBy(df_3["total duration in seconds"].desc())

    return result_df.select(
        "id_flight", "duration in hours", "duration in minutes", "duration in secondes"
    )


def get_continent_mean_flights(file_path_Airports: str, file_path_Flights: List[str]):
    """return a dataframe that contain each continent with the average flight length

    Args:
        file_path_Airports (str): _description_
        file_path_Flights (List[str]): _description_

    Returns:
        _type_: spark df
    """
    ###Airoport DF

    airports_df_sc = spark.read.csv(file_path_Airports, sep=",", header=True)

    ## Flights DF

    liste = []
    # Read data from the files
    for file_path in file_path_Flights:
        flights_df = spark.read.parquet(file_path)
        liste.append(flights_df)
    if len(liste) == 2:
        flights_df_sc = liste[0].union(liste[1])
    else:
        flights_df_sc = liste[0]
    # Effectuez la jointure
    df_org = (
        flights_df_sc.join(
            airports_df_sc,
            (flights_df_sc.aeroport_origin_icao == airports_df_sc.icao),
            "left",
        )
        .withColumn("delay", col("arrival") - col("departure"))
        # .withColumnRenamed("continent", "continent_original")
        .select(col("id_flight"), col("continent"), col("delay"))
    )
    df_dest = (
        flights_df_sc.join(
            airports_df_sc,
            (flights_df_sc.aeroport_destination_icao == airports_df_sc.icao),
            "left",
        )
        .withColumn("delay", col("arrival") - col("departure"))
        # .withColumnRenamed("continent", "continent_destination")
        .select(col("id_flight"), col("continent"), col("delay"))
    )

    df_2 = df_org.join(df_dest, on=["id_flight", "continent", "delay"], how="outer")

    # # Supprimez les doublons de la colonne 'continent'
    df_3 = df_2.distinct()
    df_4 = df_3.groupBy("continent").agg(avg("delay").alias("mean_delay_flight_hours"))
    df_5 = df_4.withColumn(
        "mean_delay_flight_hours", col("mean_delay_flight_hours") / 3600
    )
    result_df = df_5.orderBy(df_5["mean_delay_flight_hours"].desc())
    return result_df
