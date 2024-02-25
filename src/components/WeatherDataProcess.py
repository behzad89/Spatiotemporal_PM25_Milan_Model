from src.utils.climatology import download_ERA_Land, create_cdsapirc_file
from dataclasses import dataclass,field
from src.exception import CustomException
from src.logger import logging
import os, sys
from typing import List


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

@dataclass
class WeatherDataProcessor:
    def __init__(self,config:WeatherDataConfig):
        self.config = config

        logging.info(f"Start to download Variable -> {self.config.variable[0]}; Date ->{self.config.year}-{self.config.month}; AOI -> North:{self.config.area[0]}, West:{self.config.area[1]}, South:{self.config.area[2]}, East:{self.config.area[3]}")
        download_ERA_Land(self.config.variable, 
                          self.config.year,
                          self.config.month,
                          self.config.area,
                          self.config.weather_data_path)
        

# TEST CODE
if __name__=="__main__":
    create_cdsapirc_file(key="1004:751df99f-695b-494a-bb42-11652a943996")
    config = WeatherDataConfig(year=2010,month=5,area=[47.200388,8.235421,47.218580,8.272156])
    obj=WeatherDataProcessor(config)
    print(obj)