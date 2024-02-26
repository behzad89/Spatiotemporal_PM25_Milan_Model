import threading
import concurrent.futures

from src.utils.climatology import create_cdsapirc_file
from src.components.WeatherDataProcess import (WeatherDataConfig, 
                                               WeatherDataDownloader, 
                                               WeatherTransformer)


CDSAPI_KEY = "1004:751df99f-695b-494a-bb42-11652a943996"
START_YEAR = 2016
END_YEAR = 2022
AREA_OF_INTEREST=[44.547420,6.795044,46.176027,12.084961]
VARIABLES = ["2m_temperature","2m_dewpoint_temperature",
             "10m_u_component_of_wind","10m_v_component_of_wind",
             "total_precipitation","surface_net_solar_radiation"]
MAX_CONCURRENT_DOWNLOADS = 10 # CDS does not let to download more than 10 simultaneously

# Assign Key
create_cdsapirc_file(key=CDSAPI_KEY)

# Semaphore to control the number of concurrent downloads
download_semaphore = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

def download_weather_data(variable, year, month):
    config = WeatherDataConfig(year=year, month=month, area=AREA_OF_INTEREST, variable=[variable])
    obj = WeatherDataDownloader(config)
    
    # Acquire semaphore before downloading
    with download_semaphore:
        file_path = obj.download()
    
    return file_path

def parallel_download(variable):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for year in range(START_YEAR, END_YEAR):
            for month in range(1, 13, MAX_CONCURRENT_DOWNLOADS):
                # Create a batch of months (up to MAX_CONCURRENT_DOWNLOADS)
                batch = [(year, m) for m in range(month, min(month + MAX_CONCURRENT_DOWNLOADS, 13))]
                futures.extend(executor.submit(download_weather_data, variable, year, m) for _, m in batch)

        # Wait for all tasks to complete
        results = [future.result() for future in futures]

    return results

if __name__ == "__main__":
    for var in VARIABLES:
        file_paths = parallel_download(var)
        WeatherTransformer(file_paths,var).convertor()
