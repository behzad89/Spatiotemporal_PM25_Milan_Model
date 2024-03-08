import os, sys
from typing import List

import xarray as xr
import h3.api.numpy_int as h3
import numpy as np

from src.exception import CustomException
from src.logger import logging


def h3_idx(row):
    lat, lon = row['lat'], row['lon']
    h3_index = h3.geo_to_h3(lat, lon, resolution=5)  # You can adjust the resolution based on your needs
    row['h3_index'] = h3_index
    return row


def pm25processor(filesPath:str, bbox:List[float]) -> str:

    da = xr.open_dataarray(f'{filesPath}',chunks={'lat': 25, 'lon': 25, 'time': 25})
    logging.info(f" The DASK object -> {da.dims}")

    logging.info("Filter based on BBOX")
    ds_filter = da.sel(lat=slice(bbox[0], bbox[2]),
                        lon=slice(bbox[1],bbox[3]))
    
    
    # Create a directory to save independent variables
    logging.info("Make a directory to save independent variables")
    output_dir = os.path.join(os.getcwd(), "independent-variables", "pm25_cams_data")
    os.makedirs(output_dir, exist_ok=True)
    
    
    # Resample hourly data to daily data using xarray
    logging.info("Resample hourly data to daily data")
    daily_ds = ds_filter.resample(time='1D').mean().rename({'time': 'date'})

    logging.info("Convert Dataset to DataFrame")
    df = daily_ds.to_dask_dataframe().repartition(npartitions=10)


    # Apply the function to each row in the DataFrame
    df = df.apply(h3_idx, axis=1,meta={**df.dtypes.to_dict(),**{"h3_index":np.int64}})


    # Save the DataFrame as a parquet file in the output directory
    date = filesPath.split('.')[-2]
    name_function = lambda x: f"pm25_cams_{date}_{x}.parquet"
    df.to_parquet(output_dir,name_function=name_function,write_index=False)

    return f'{output_dir}/pm25_cams_{date}'