from dask.distributed import LocalCluster
from src.components.WeatherDataProcess import WeatherDataProcessor
from src.logger import logging
import time


START_YEAR = 2016
END_YEAR = 2022
AREA_OF_INTEREST=[44.547420,6.795044,46.176027,12.084961]

if __name__ == "__main__":
    # Step 1: Create a Dask Cluster
    logging.info("Step 1: Create Dask Cluster")
    cluster = LocalCluster(timeout='90s')
    client = cluster.get_client()
    time.sleep(3)

    for year in range(START_YEAR, END_YEAR+1):
        OUTPUT = WeatherDataProcessor(year=year, area=AREA_OF_INTEREST).Transformer()
        time.sleep(5)
    
    # Step 2: Close the Dask Cluster
    cluster.close()
