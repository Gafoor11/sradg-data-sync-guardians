import pandas as pd
from sklearn.ensemble import IsolationForest

# Load historical and current data
history_df = pd.read_excel("C:\\Users\\Admin\\Desktop\\Datsets\\history_data.xlsx")
current_df = pd.read_excel("C:\\Users\\Admin\\Desktop\\Datsets\\current_data.xlsx")

# Compute balance difference
history_df['balance_diff'] = history_df['gl_balance'].fillna(0) - history_df['ub_balance'].fillna(0)
current_df['balance_diff'] = current_df['gl_balance'].fillna(0) - current_df['ub_balance'].fillna(0)

# Compute historical anomaly rate per account
anomaly_rates = history_df.groupby('account_id')['status'].apply(lambda x: (x == 'Break').mean())

# Set contamination dynamically per account
optimal_contamination = anomaly_rates.to_dict()
def get_contamination(account_id):
    return min(max(optimal_contamination.get(account_id, 0.05), 0.01), 0.5)

# Train Isolation Forest per account and predict anomalies
def detect_anomaly(row):
    account_data = history_df[history_df['account_id'] == row['account_id']][['balance_diff']]
    
    if account_data.empty:
        print(f"Warning: No historical data for Account ID {row['account_id']}")
        return "No"  # Default to No if no historical data exists
    
    if len(account_data) < 2:
        print(f"Warning: Not enough historical data for Account ID {row['account_id']}")
        return "No"  # Avoid model failure due to single data point
    
    contamination = get_contamination(row['account_id'])
    model = IsolationForest(contamination=contamination, random_state=42)
    model.fit(account_data)  # Fit model per account
    
    # Ensure prediction input is a 2D array
    anomaly = model.predict(pd.DataFrame([[row['balance_diff']]], columns=['balance_diff']))[0]
    return "Yes" if anomaly == -1 else "No"

current_df['Is_Anomaly'] = current_df.apply(detect_anomaly, axis=1)

# Determine status (Break or Match)
def determine_status(row):
    account_history = history_df[history_df['account_id'] == row['account_id']]
    
    if account_history.empty:
        return "Match"

    mean_diff = account_history['balance_diff'].mean()
    std_dev = account_history['balance_diff'].std()

    # Handle cases where std_dev is NaN (only one record exists)
    threshold = mean_diff + (std_dev if pd.notna(std_dev) else 0)
    
    return "Break" if abs(row['balance_diff']) > threshold else "Match"

current_df['status'] = current_df.apply(determine_status, axis=1)

# Save back to the same Excel file, keeping existing data
output_path = "C:\\Users\\Admin\\Desktop\\Datsets\\current_data.xlsx"
with pd.ExcelWriter(output_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
    current_df.to_excel(writer, sheet_name='Sheet1', index=False)

print(f"Processing complete! '{output_path}' updated with Is_Anomaly and status columns.")
