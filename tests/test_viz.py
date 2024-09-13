import os
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession

# sys.path.append(os.path.abspath("../src"))
from src.viz import *

spark = SparkSession.builder.appName("MySparkApp").getOrCreate()


class Testviz(unittest.TestCase):
    def test_get_continent_mean_flights(self):
        file_path_Airports = os.path.join(
            r".\\tests\\data_test\data\\Airports", "Airports.csv"
        )
        file_path_Flights = os.path.join(
            ".\\tests\\data_test\\data\\Flights\\year=2024\\month=8\\day=20",
            "*.parquet",
        )
        path_df_expected = ".\\tests\\data_test\\get_continent_mean_flights.csv"

        df_res = get_continent_mean_flights(
            file_path_Airports=file_path_Airports, file_path_Flights=[file_path_Flights]
        )
        pandasDF_res = df_res.toPandas()
        df_expected = pd.read_csv(path_df_expected)
        assert_frame_equal(pandasDF_res, df_expected)


if __name__ == "__main__":
    unittest.main()
