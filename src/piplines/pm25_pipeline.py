from dask.distributed import LocalCluster
from src.components.pm25Process import pm25processor
from src.logger import logging
import time, glob


FILES = sorted(glob.glob("data/pm25_cams/*.nc"))

AREA_OF_INTEREST=[45.2248134643484434,8.6812211557677017,
                  45.6593335936165374,9.5784069699739973]

if __name__ == "__main__":
    # Step 1: Create a Dask Cluster
    logging.info("Create Dask Cluster")
    # Create a local Dask Cluster with a timeout of 90 seconds
    cluster = LocalCluster(timeout='60s') #silence_logs=False
    # Get a Dask client from the created cluster
    client = cluster.get_client()
    # Allow a short pause for the cluster to stabilize
    time.sleep(5)

    for f in FILES:
        OUTPUT = pm25processor(filesPath=f, bbox=AREA_OF_INTEREST)
        logging.info(f"File Saved to -> {OUTPUT}")


    time.sleep(3)
    # # Close the Dask cluster, releasing resources
    cluster.close()
    logging.info(f"Process Done & Cluster Closed!")


# Unzip files -->>>> find . -type f -name "*.zip" -exec unzip {} \;
