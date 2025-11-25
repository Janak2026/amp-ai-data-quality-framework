# Week 5 Model Documentation

## Model: Customer Lifetime Value (CLV) Predictor  
Type: DecisionTreeRegressor  
Framework: Scikit-learn  
Tracking: MLflow

### ✔ Inputs
- total_spend (float)
- total_orders (int)
- avg_order_value (float)
- first_purchase_date (string)
- last_purchase_date (string)

### ✔ Output
- Predicted CLV (float)

### ✔ Performance
- Accuracy (classification proxy): 1.0
- F1 Score: 1.0  
*Dataset is synthetic → performance expectedly perfect.*

### ✔ Artifact
Stored at:
