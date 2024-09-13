import csv
import os

from FlightRadar24 import FlightRadar24API

fr_api = FlightRadar24API()


class Airline:
    def __init__(self) -> None:
        pass

    def check(self) -> bool:
        pass

    def update_airline_data(self):
        # Créez le chemin du répertoire
        directory = "./src/data/Airlines"
        os.makedirs(directory, exist_ok=True)
        airlines = fr_api.get_airlines()
        file_path = os.path.join(directory, "Airlines.csv")
        # Write to CSV
        with open(file_path, "w", newline="") as csvfile:
            fieldnames = ["Name", "Code", "ICAO"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(airlines)
