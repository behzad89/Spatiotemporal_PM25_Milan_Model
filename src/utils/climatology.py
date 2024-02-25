import cdsapi
from calendar import monthrange
from typing import List
from src.exception import CustomException
import sys,os


def download_ERA_Land(variable:List[str], year:int, month:int, area:List[float], file_path:str):
    '''
    This function downloads ERA5-Land reanalysis data for specified parameters and saves it to a NetCDF file.

    Parameters:
    - variable (List[str]): A list of variables to download, e.g., ['temperature', 'precipitation'].
    - year (int): The year for which to download the data.
    - month (int): The month for which to download the data (1 to 12).
    - area (List[float]): A list representing the geographical bounding box for the desired area.
                         The order of values is [North, West, South, East].
    - file_path (str): The path where the downloaded NetCDF file will be saved.

    Returns:
    - None: The function does not return a value directly, but it downloads the data and saves it to the specified file.

    The function initiates a request to the Copernicus Climate Data Store (CDS) to retrieve ERA5-Land reanalysis data.
    It specifies the desired variable, year, month, geographical area, and the desired output format as NetCDF.

    The request is then submitted using the cdsapi.Client() and the retrieved data is saved to the specified file path.
    The function does not return anything directly but saves the downloaded data to the specified file.

    Example Usage:
    ```
    variables_to_download = ['temperature', 'precipitation']
    target_year = 2022
    target_month = 5
    target_area = [60, -10, 40, 20]  # North, West, South, East
    output_file_path = "path/to/output.nc"

    download_ERA_Land(variables_to_download, target_year, target_month, target_area, output_file_path)
    ```
    '''
    try:
        # Generate a list of days in the specified month
        days = [1]#range(1, monthrange(year, month)[1] + 1)

        # Construct the request parameters
        request = {
                    'variable':variable,
                    'year': "%04d" % year,
                    'month': "%02d" % month ,
                    'day': ["%02d" % d for d in days],
                    'time': ["%02d:00" % h for h in range(0, 1)],
                    'area': area, # North, West, South, East
                    'format':'netcdf'
        }

        # Create Directory to save files
        os.makedirs(os.path.dirname(file_path),exist_ok=True)

        # Initiate a client to interact with the Copernicus Climate Data Store (CDS)
        c = cdsapi.Client()

        # Retrieve data based on the constructed request
        r = c.retrieve(
                'reanalysis-era5-land',
                request
                )

        # Download the retrieved data and save it to the specified file path
        r.download(file_path)
        return file_path
    except Exception as e:
            raise CustomException(e,sys)


def create_cdsapirc_file(key:str, url:str = "https://cds.climate.copernicus.eu/api/v2"):
    # Get the home directory
    home_directory = os.path.expanduser('~')

    # Specify the file path
    file_path = os.path.join(home_directory, '.cdsapirc')

    # Write content to the file
    with open(file_path, 'w') as file:
        file.write(f"""url:{url}\nkey:{key} """)