import os
import sys
import time
from typing import List

import xarray as xr
from dask.distributed import LocalCluster
from dataclasses import dataclass, field

from src.exception import CustomException
from src.logger import logging
from src.utils.climatology import download_ERA_Land



@dataclass
class WeatherDataConfig:
    year: int 
    month: int
    area: List[float] = field(default_factory=list)
    
    filename: str = field(init=False)
    weather_data_path: str = field(init=False)
    variable: List[str] = field(default_factory=lambda: ["2m_temperature"])

    def __post_init__(self):
        self.filename = f"era5-land-{self.variable[0]}-{self.year:04d}-{self.month:02d}.nc"
        self.weather_data_path = os.path.join("weather-data", self.filename)

class WeatherDataDownloader:
    def __init__(self,config:WeatherDataConfig):
        self.config = config

    def download(self) -> str:
        try:
            logging.info(f"Start to download Variable -> {self.config.variable[0]}; Date ->{self.config.year}-{self.config.month}; AOI -> North:{self.config.area[0]}, West:{self.config.area[1]}, South:{self.config.area[2]}, East:{self.config.area[3]}")
            self.data_path = download_ERA_Land(self.config.variable, 
                            self.config.year,
                            self.config.month,
                            self.config.area,
                            self.config.weather_data_path)
            return self.data_path
        except Exception as e:
            raise CustomException(e,sys)
        
        
class WeatherTransformer:
    def __init__(self,files: List[str],variable:str) -> None:
        self.files = files
        self.variable = variable

    def convertor(self):
        try:


            # Step 1: Create a Dask Cluster
            logging.info("Step 1: Create Dask Cluster")
            cluster = LocalCluster(n_workers=8, threads_per_worker=3)
            client = cluster.get_client()
            time.sleep(3)

            # Step 2: Resample hourly data to daily data using xarray
            logging.info("Step 2: Resample hourly data to daily data")
            ds = xr.open_mfdataset(self.files, parallel=False)
            daily_ds = ds.resample(time='1D').mean().rename({'time': 'date'})

            # Step 3: Create a directory to save independent variables
            logging.info("Step 3: Make a directory to save independent variables")
            output_dir = os.path.join(os.getcwd(), "independent-variables",self.variable)
            os.makedirs(output_dir, exist_ok=True)

            # Step 4: Convert the resampled dataset to a Dask DataFrame
            logging.info("Step 4: Convert Dataset to DataFrame")
            df = daily_ds.to_dask_dataframe().repartition(npartitions=10)

            # Step 5: Save the DataFrame as a parquet file in the output directory
            name_function = lambda x: f"{self.variable}-{x}.parquet"
            df.to_parquet(output_dir,name_function=name_function)
            logging.info("Step 5: Data Saved to independent-variables/")

            # Step 6: Close the Dask Cluster
            return cluster.close()

        except Exception as e:
            # If any exception occurs, raise a CustomException
            raise CustomException(e, sys)