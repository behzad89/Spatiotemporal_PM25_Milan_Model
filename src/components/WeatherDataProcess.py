import os, sys
from typing import List

import xarray as xr
import pystac_client
import planetary_computer
from src.utils.climatology import h3_idx

from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging


@dataclass
class WeatherDataConfig:
    client = "https://planetarycomputer.microsoft.com/api/stac/v1/"
    collection = ["era5-pds"]

class WeatherDataProcessor:
    def __init__(self,year:int, area: List[float]):
        self.config = WeatherDataConfig
        self.year = year
        self.area = area

    def Transformer(self) -> str:
        try:
            logging.info(f"Start to download weather; "
                        f"Year -> {self.year}; "
                        f"AOI -> North:{self.area[0]}, "
                        f"West:{self.area[1]}, "
                        f"South:{self.area[2]}, "
                        f"East:{self.area[3]}")
            
            time_range = f"{self.year}-01/{self.year}-12"
            catalog = pystac_client.Client.open(self.config.client)

            search = catalog.search(collections=[self.config.collection], 
                                    datetime=time_range)
            
            items = search.item_collection()

            logging.info(f" The number of the items -> {len(items)}")

            datasets=[]
            for item in items:
                signed_item = planetary_computer.sign(item)
                for asset in signed_item.assets.values():
                    datasets.append(xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"]))

            ds = xr.combine_by_coords(datasets, join="exact")
            logging.info(f" The DASK object -> {ds.dims}")

            logging.info("Filter based on BBOX")
            ds_filter = ds.sel(lat=slice(self.area[2], self.area[0]), # Start with high lat
                                lon=slice(self.area[1],self.area[3]))

            # Create a directory to save independent variables
            logging.info("Make a directory to save independent variables")
            output_dir = os.path.join(os.getcwd(), "independent-variables", "weather_data")
            os.makedirs(output_dir, exist_ok=True)

            # Resample hourly data to daily data using xarray
            logging.info("Resample hourly data to daily data")
            daily_ds = ds_filter.resample(time='1D').mean().rename({'time': 'date'})

            # Convert the resampled dataset to a Dask DataFrame
            logging.info("Convert Dataset to DataFrame")
            df = daily_ds.to_dask_dataframe().repartition(npartitions=10)


            # Apply the function to each row in the DataFrame
            df = df.apply(h3_idx, axis=1,meta=dict(df.dtypes))

            # Drop Lat and Long to save space
            df = df.drop(["lat","lon"], axis=1)

            # Save the DataFrame as a parquet file in the output directory
            name_function = lambda x: f"weather_data{self.year}-{x}.parquet"
            df.to_parquet(output_dir,name_function=name_function,write_index=False)
            logging.info(f"Step 5: {self.year} Data Saved to {output_dir}")

            return output_dir
        
        except Exception as e:
            raise CustomException(e,sys)
