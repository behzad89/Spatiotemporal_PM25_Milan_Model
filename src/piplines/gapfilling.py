import os, sys,time
from typing import List

import pandas as pd

import dask
from dask.delayed import delayed
from dask_cuda import LocalCUDACluster
from dask.distributed import Client
dask.config.set({"dataframe.backend": "cudf"})
import dask.dataframe as dd

from fusets.whittaker import whittaker_f

from src.exception import CustomException
from src.logger import logging


def NDVIDataProcessor(file:str) -> str:
    # Define the delayed function
    @delayed
    def WhittakerTransformer(output_dir,idx):
        # Read the parquet files and Filter the index
        dff = df[df['h3_index']==idx]
        date = dff['date'].compute().to_arrow().to_pylist()
        ndvi = dff['250m_16_days_NDVI'].values.compute().get()
        lmbd = 6000
        d = 10
        Z, D, Zd, Dd = whittaker_f(date, ndvi, lmbd, d)
        dfs = pd.DataFrame({'h3_index': idx, 'date': D, 'NDVI': Z})
        dfs.to_parquet(f'{output_dir}/{idx}.parquet')
        return output_dir

    try:
        logging.info("Read PARQUET file")
        df = dd.read_parquet(file)

        df = df.sort_values(by="date")

        # Get unique indices
        h3_idxs = df['h3_index'].unique().compute().to_arrow().to_pylist()

        logging.info(f"Number of Points -> {len(h3_idxs)}")

        output_dir = os.path.join(os.getcwd(), "independent-variables", "ndvi_data","smoothed")
        os.makedirs(output_dir, exist_ok=True)

        logging.info("Generate daily data based on Whittaker")
        # Use dask.delayed to parallelize the computations
        delayed_results = [WhittakerTransformer(output_dir, idx) for idx in h3_idxs]

        return delayed_results

    except Exception as e:
        raise CustomException(e,sys)
    

if __name__ == "__main__":
    cluster = LocalCUDACluster(timeout='90s')
    client = Client(cluster)
    time.sleep(5)

    
    file = './independent-variables/ndvi_data/ndvi_download_2016_2023.parquet'
    logging.info(f"Read file -> {file}")
    delayed_files = NDVIDataProcessor(file)

    output_dir = dask.compute(*delayed_files)[0]

    logging.info(f"File saved to -> {output_dir}")
    cluster.close()