import csv
import os
from time import sleep

from FlightRadar24 import FlightRadar24API

fr_api = FlightRadar24API()


class Airport:
    def __init__(self) -> None:
        pass

    def check(self) -> bool:
        pass

    def update_airport_data(self):
        airport_dict = dict()
        airports = fr_api.get_airports()
        max_retries = 3  # Set the maximum number of retries
        directory = r"./src/data/Airports"
        os.makedirs(directory, exist_ok=True)
        file_path = os.path.join(directory, "Airports.csv")
        with open(file_path, mode="w", newline="") as csvfile:
            fieldnames = [
                "iata",
                "icao",
                "name",
                "country",
                "latitude",
                "longitude",
                "altitude",
                "continent",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for airport, index in zip(airports[:], range(len(airports))):
                retries = 0
                while retries < max_retries:
                    try:
                        airport_dict["iata"] = airport.iata
                        airport_dict["icao"] = airport.icao
                        airport_dict["name"] = airport.name
                        airport_dict["country"] = airport.country
                        airport_dict["latitude"] = airport.latitude
                        airport_dict["longitude"] = airport.longitude
                        airport_dict["altitude"] = airport.altitude

                        details = fr_api.get_airport_details(code=airport.iata)
                        airport_dict["continent"] = details["airport"]["pluginData"][
                            "details"
                        ]["timezone"]["name"].split("/")[0]

                        writer.writerow(airport_dict)

                        sleep(1)
                        break  # Exit the retry loop if successful
                    except Exception as e:
                        print(f"Error: {e}")
                        retries += 1
                        sleep(30)  # Wait for 30 seconds before retrying
