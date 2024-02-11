# A Spatiotemporal Model for PM2.5 Monitoring and Analysis

## Model Performance Overview
The table presents a comprehensive overview of the performance metrics for different models in a predictive analysis. The models evaluated include Linear Regression, Random Forest, and a Tuned Random Forest. The performance is assessed based on Root Mean Squared Error (RMSE) and R-squared (R²) scores across various evaluation stages, including training, testing, spatial cross-validation, and temporal cross-validation.

| Model                   | Train RMSE/R²    | Test RMSE/R²     | Spatial CV RMSE/R²  | Temporal CV RMSE/R²   |
|-------------------------|------------------|------------------|----------------------|----------------------|
| Linear regression       | 4.01/0.93        | 3.99/0.93        | 4.04/0.93            | 4.03/0.93            |
| Random Forest           | 0.38/1.00        | 1.10/0.99        | 1.19/0.99            | 4.35/0.92            |
| Tuned Random Forest     | 4.19/0.92        | 4.13/0.92        | 4.15/0.92            | 4.43/0.91            |