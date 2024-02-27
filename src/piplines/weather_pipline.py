from src.components.WeatherDataProcess import WeatherDataProcessor


START_YEAR = 2016
END_YEAR = 2022
AREA_OF_INTEREST=[44.547420,6.795044,46.176027,12.084961]

if __name__ == "__main__":
    for year in range(START_YEAR, END_YEAR+1):
        OUTPUT = WeatherDataProcessor(year=year, area=AREA_OF_INTEREST).Transformer()
