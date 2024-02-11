# A Spatiotemporal Model for PM2.5 Monitoring and Analysis

## Model Performance Overview
The table presents a comprehensive overview of the performance metrics for different models in a predictive analysis. The models evaluated include Linear Regression, Random Forest, and a Tuned Random Forest. The performance is assessed based on Root Mean Squared Error (RMSE) and R-squared (R²) scores across various evaluation stages, including training, testing, spatial cross-validation, and temporal cross-validation.

| Model                   | Train RMSE/R²    | Test RMSE/R²     | Spatial CV RMSE/R²  | Temporal CV RMSE/R²   |
|-------------------------|------------------|------------------|----------------------|----------------------|
| Linear regression       | 4.01/0.93        | 3.99/0.93        | 4.04/0.93            | 4.03/0.93            |
| Random Forest           | 0.38/1.00        | 1.10/0.99        | 1.19/0.99            | 4.35/0.92            |
| Tuned Random Forest     | 4.19/0.92        | 4.13/0.92        | 4.15/0.92            | 4.43/0.91            |

## Data Source
GeoAI Challenge for Air Pollution Susceptibility Mapping by ITU: [Link](https://zindi.africa/competitions/geoai-challenge-for-air-pollution-susceptibility-mapping/data)
- Meteorological timeseries data (2016-2022) which includes temperature, precipitation, relative humidity, solar radiation, wind speed and direction at a daily temporal resolution.
- Air pollution timeseries data (2016-2021) which includes NOx, SO2, CO, O3, PM2.5, PM10, and benzene at a daily temporal resolution.
- Digital terrain model, land cover, geology, plan curvature, hill shade, slope, SPI, TRI, TWI are provided at the stations’ points and as a continuous representation at 100-m resolution.