import os, sys
from typing import List

import odc.stac
import pystac_client
import planetary_computer
import dask_geopandas
import numpy as np
import h3.api.numpy_int as h3

import dask.dataframe as dd
from dask.delayed import delayed
from fusets.whittaker import whittaker_f
import pandas as pd

from dataclasses import dataclass
from src.exception import CustomException
from src.logger import logging


def h3_idx(row):
    lat, lon = row['lat'], row['lon']
    h3_index = h3.geo_to_h3(lat, lon, resolution=9)  # You can adjust the resolution based on your needs
    row['h3_index'] = h3_index
    return row

def decimal_to_binary(x):
    return format(x, '016b')


CLIENT = "https://planetarycomputer.microsoft.com/api/stac/v1/"
COLLECTION = ["modis-13Q1-061"]

def NDVIDataTransformer(year:int, bbox: List[float]) -> str:
    try:
        logging.info(f"Start to download ndvi; "
                    f"Year -> {year}; "
                    f"AOI -> North:{bbox[0]}, "
                    f"West:{bbox[1]}, "
                    f"South:{bbox[2]}, "
                    f"East:{bbox[3]}")
        
        time_range = f"{year}-01/{year}-12"
        catalog = pystac_client.Client.open(CLIENT,
                                            modifier=planetary_computer.sign_inplace)

        search = catalog.search(collections=[COLLECTION],
                                bbox=bbox,
                                datetime=time_range)
        
        items = search.item_collection()

        if len(items) == 0:
            logging.info(f"There is no data for {year}")
            sys.exit(0)
            
        logging.info(f" The number of the items -> {len(items)}")

        ds = odc.stac.load(
            items,
            chunks={"x": 300, "y": 300,"time":25},
            crs="EPSG:3857",
            bands=["250m_16_days_NDVI",
                    '250m_16_days_pixel_reliability',
                    '250m_16_days_VI_Quality'],
            resolution=250,
            bbox=bbox,
        )

        logging.info(f" The DASK object -> {ds.dims}")

        logging.info("Filter based on Date")
        ds_filter = ds.sel(time=slice(f'{year}-01-01',f'{year}-12-31')).rename({'time': 'date'})

        # Create a directory to save independent variables
        logging.info("Make a directory to save independent variables")
        output_dir = os.path.join(os.getcwd(), "independent-variables", "ndvi_data","download")
        os.makedirs(output_dir, exist_ok=True)

        # Convert the resampled dataset to a Dask DataFrame
        logging.info("Convert Dataset to DataFrame")
        df = ds_filter.to_dask_dataframe()

        logging.info("Decimal to Binary")
        df['250m_16_days_VI_Quality'] = df['250m_16_days_VI_Quality'].apply(
            decimal_to_binary, meta=('x', 'str'))

        logging.info("Convert DataFrame to GeoDataFrame")
        ddf = df.set_geometry(
            dask_geopandas.points_from_xy(df, 'x', 'y')).set_crs(3857)
        
        logging.info("Projection")
        ddf = ddf.to_crs(epsg=4326)

        # Drop X and Y to save space
        ddf = ddf.drop(["x","y"], axis=1)

        logging.info("Calculate h3 index")
        # Apply the function to each row in the DataFrame
        ddf['lat'] = ddf.geometry.y
        ddf['lon'] = ddf.geometry.x
        ddf_h3 = ddf.apply(h3_idx, axis=1,meta={**ddf.dtypes.to_dict(),**{"h3_index":np.int64}})

        # Drop Lat and Long to save space
        ddf_h3 = ddf_h3.drop(["lat","lon","geometry","spatial_ref"], axis=1)

        # Save the DataFrame as a parquet file in the output directory
        name_function = lambda x: f"ndvi_data{year}-{x}.parquet"
        ddf_h3.to_parquet(output_dir,name_function=name_function,write_index=False)
        logging.info(f"{year} Data Saved to {output_dir}")

        return output_dir
    
    except Exception as e:
        raise CustomException(e,sys)



def NDVIDataProcessor(files:str) -> str:
    # Define the delayed function
    @delayed
    def WhittakerTransformer(idx,output_dir:str):
        date = df[df['h3_index']==idx]['date'].compute().tolist()
        ndvi = df[df['h3_index']==idx]['250m_16_days_NDVI'].values.compute()
        lmbd = 6000
        d = 10
        Z, D, Zd, Dd = whittaker_f(date, ndvi, lmbd, d)
        dfs = pd.DataFrame({'h3_index': idx, 'date': D, 'NDVI': Z})
        dfs.to_parquet(f'{output_dir}/{idx}-ndvi.parquet')
        return output_dir

    try:
        logging.info("Read PARQUET files")
        # Read the parquet files
        df = dd.read_parquet(files)

        logging.info("Filter based on Pixel Reliability")

        # Filter based on condition
        df['250m_16_days_pixel_reliability'] = df['250m_16_days_pixel_reliability'].where(df['250m_16_days_pixel_reliability'] == 0)

        # Get unique indices
        h3_idxs = df['h3_index'].unique()

        logging.info(f"Number of Points -> {len(h3_idxs)} (after filter)")

        output_dir = os.path.join(os.getcwd(), "independent-variables", "ndvi_data","smoothed")
        os.makedirs(output_dir, exist_ok=True)

        logging.info("Generate daily data based on Whittaker")
        # Use dask.delayed to parallelize the computations
        delayed_results = [WhittakerTransformer(idx,output_dir) for idx in h3_idxs]

        return delayed_results

    except Exception as e:
        raise CustomException(e,sys)