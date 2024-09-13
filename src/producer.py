import json
import logging
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import List

from FlightRadar24 import FlightRadar24API
from kafka import KafkaProducer

fr_api = FlightRadar24API()

# Initialiser le logging et rediriger les logs vers un fichier
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s",
    filename="./app.log",  # Nom du fichier de log
    filemode="a",
)

# Créer un logger
logger = logging.getLogger("spark_structured_streaming")


class Run_producer:
    def __init__(self, server: List, topic: str) -> None:
        self.server = server
        self.topic = topic

    def run(self):
        producer = KafkaProducer(
            bootstrap_servers=self.server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(1, 10, 2),
        )
        id_flight = None
        airline_icao = None
        aeroport_origin_iata = None
        aeroport_destination_iata = None
        arrival = None
        live = None
        departure = None

        for flight in fr_api.get_flights():
            flight_dict = dict()

            det = fr_api.get_flight_details(flight)
            id_flight = flight.id

            if "airline" in det:
                if det["airline"] and det["airline"]["code"]:
                    airline_icao = det["airline"]["code"]["icao"]

            if "airport" in det:
                if "origin" in det["airport"]:
                    if det["airport"]["origin"]:
                        aeroport_origin_iata = det["airport"]["origin"]["code"]["iata"]
                        aeroport_origin_iata_icao = det["airport"]["origin"]["code"][
                            "icao"
                        ]
                        offsetHours = det["airport"]["origin"]["timezone"][
                            "offsetHours"
                        ]
                        offset = det["airport"]["origin"]["timezone"]["offset"]

                    if det["airport"]["destination"]:
                        aeroport_destination_iata = det["airport"]["destination"][
                            "code"
                        ]["iata"]
                        aeroport_destination_icao = det["airport"]["destination"][
                            "code"
                        ]["icao"]
            if "time" in det:
                if det["time"]:
                    time = det["time"]
                    if time["real"]["departure"] and time["real"]["arrival"]:
                        departure = time["real"]["departure"]

                        arrival = time["real"]["arrival"]

            if arrival and offset:
                time_00 = datetime.now(timezone.utc)
                time_in = time_00 + timedelta(seconds=offset)
                live = True if time_in.timestamp() < arrival else False

            if (
                id_flight
                and aeroport_origin_iata
                and aeroport_destination_iata
                and airline_icao
            ):
                flight_dict["id_flight"] = id_flight
                flight_dict["airline_icao"] = airline_icao
                flight_dict["aeroport_origin_iata"] = aeroport_origin_iata
                flight_dict["aeroport_origin_icao"] = aeroport_origin_iata_icao
                flight_dict["aeroport_destination_iata"] = aeroport_destination_iata
                flight_dict["aeroport_destination_icao"] = aeroport_destination_icao
                flight_dict["live"] = live
                flight_dict["departure"] = departure
                flight_dict["arrival"] = arrival
                flight_dict["offsetHours"] = offsetHours
                flight_dict["offset"] = offset

            try:
                if flight_dict:
                    print(flight_dict)
                    producer.send(self.topic, flight_dict)
                    producer.flush()
                    sleep(5)
                    logger.info("Data sended successfully")
            except Exception as e:
                logger.error(f"Failed to send data. Error: {e}")
