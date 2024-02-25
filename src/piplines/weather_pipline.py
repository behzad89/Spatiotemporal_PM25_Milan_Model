from src.components import WeatherDataProcess
from src.utils.climatology import create_cdsapirc_file
from src.components.WeatherDataProcess import WeatherDataConfig,WeatherDataProcessor


CDSAPI_KEY = "1004:751df99f-695b-494a-bb42-11652a943996"
START_YEAR = 2005
END_YEAR = 2007
AREA_OF_Interest=[47.200388,8.235421,47.218580,8.272156]

create_cdsapirc_file(key=CDSAPI_KEY)

for YEAR in range(START_YEAR, END_YEAR):
    for MONTH in range(1, 13):
        config = WeatherDataConfig(year=YEAR,month=MONTH,area=AREA_OF_Interest)
        obj=WeatherDataProcessor(config)