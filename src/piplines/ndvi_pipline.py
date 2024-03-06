from dask.distributed import LocalCluster
import dask
from src.components.NDVIDataProcess import NDVIDataTransformer,NDVIDataProcessor
from src.logger import logging
import time


START_YEAR = 2016
END_YEAR = 2023
AREA_OF_INTEREST=[8.6812211557677017,45.2248134643484434,
                  9.5784069699739973,45.6593335936165374]

if __name__ == "__main__":
    # Step 1: Create a Dask Cluster
    logging.info("Create Dask Cluster")
    # Create a local Dask Cluster with a timeout of 90 seconds
    cluster = LocalCluster(timeout='60s') #silence_logs=False
    # Get a Dask client from the created cluster
    client = cluster.get_client()
    # Allow a short pause for the cluster to stabilize
    time.sleep(5)

    # Iterate over the specified range of years
    for year in range(START_YEAR, END_YEAR+1):
        # Process weather data for the current year and area of interest
        # Assuming WeatherDataProcessor has a Transformer method
        OUTPUT = NDVIDataTransformer(year=year, bbox=AREA_OF_INTEREST)
        # Introduce a pause to manage processing or system load
        time.sleep(5)

    logging.info(f"Downloaded to -> {OUTPUT}")

    # Allow a short pause for the cluster to stabilize
    time.sleep(5)
    # Close the Dask cluster, releasing resources
    cluster.close()
