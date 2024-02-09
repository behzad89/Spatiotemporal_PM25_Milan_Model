# A Spatiotemporal Model for PM2.5 Monitoring and Analysis

## Model Performance Overview
The table presents a comprehensive overview of the performance metrics for different models in a predictive analysis. The models evaluated include Linear Regression, Random Forest, and a Tuned Random Forest. The performance is assessed based on Root Mean Squared Error (RMSE) and R-squared (R²) scores across various evaluation stages, including training, testing, spatial cross-validation, and temporal cross-validation.

| Model                   | Train RMSE/R²    | Test RMSE/R²     | Spatial CV RMSE/R²  | Temporal CV RMSE/R²   |
|-------------------------|------------------|------------------|----------------------|----------------------|
| Linear regression       | 4.14/0.92        | 4.15/0.92        | 4.22/0.92            | 4.16/0.92            |
| Random Forest           | 0.48/1.00        | 1.5/0.99         | 1.67/0.99            | 3.32/0.95            |
| Tuned Random Forest     | 3.52/0.95        | 3.47/0.95        | 3.56/0.94            | 3.56/0.94            |



