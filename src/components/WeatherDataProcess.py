import os, sys
from typing import List

import xarray as xr
import pystac_client
import planetary_computer
import h3.api.numpy_int as h3
import numpy as np

from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging

def h3_idx(row):
    lat, lon = row['lat'], row['lon']
    h3_index = h3.geo_to_h3(lat, lon, resolution=7)  # You can adjust the resolution based on your needs
    row['h3_index'] = h3_index
    return row

#TODO: Convert to the function 
@dataclass
class WeatherDataConfig:
    client = "https://planetarycomputer.microsoft.com/api/stac/v1/"
    collection = ["era5-pds"]

class WeatherDataProcessor:
    def __init__(self,year:int, bbox: List[float]):
        self.config = WeatherDataConfig
        self.year = year
        self.bbox = bbox

    def Transformer(self) -> str:
        try:
            logging.info(f"Start to download weather; "
                        f"Year -> {self.year}; "
                        f"AOI -> North:{self.bbox[0]}, "
                        f"West:{self.bbox[1]}, "
                        f"South:{self.bbox[2]}, "
                        f"East:{self.bbox[3]}")
            
            time_range = f"{self.year}-01/{self.year}-12"
            catalog = pystac_client.Client.open(self.config.client)

            search = catalog.search(collections=[self.config.collection], 
                                    datetime=time_range)
            
            items = search.item_collection()

            if len(items) == 0:
                logging.info(f"There is no data for {self.year}")
                sys.exit(0)
                
            logging.info(f" The number of the items -> {len(items)}")

            datasets=[]
            for item in items:
                signed_item = planetary_computer.sign(item)
                for asset in signed_item.assets.values():
                    datasets.append(xr.open_dataset(asset.href, **asset.extra_fields["xarray:open_kwargs"]))

            ds = xr.combine_by_coords(datasets, join="exact")
            logging.info(f" The DASK object -> {ds.dims}")

            logging.info("Filter based on BBOX")
            ds_filter = ds.sel(lat=slice(self.bbox[2], self.bbox[0]), # Start with high lat
                                lon=slice(self.bbox[1],self.bbox[3]))

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
            df = df.apply(h3_idx, axis=1,meta={**df.dtypes.to_dict(),**{"h3_index":np.int64}})

            # Save the DataFrame as a parquet file in the output directory
            name_function = lambda x: f"weather_data{self.year}-{x}.parquet"
            df.to_parquet(output_dir,name_function=name_function,write_index=False)
            logging.info(f"{self.year} Data Saved to {output_dir}")

            return output_dir
        
        except Exception as e:
            raise CustomException(e,sys)
