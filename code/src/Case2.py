import pandas as pd
import numpy as np
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Email Configuration
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587  # Use 587 for TLS
EMAIL_SENDER = "swathidhanraj14@gmail.com"  # Replace with sender email
EMAIL_PASSWORD = "jmcq ehnx qdwe byem"  # Replace with App Password

# Load historical and current data
history_file = "C:\\Users\\Admin\\Desktop\\Datsets\\Case2\\trading_data_with_history_final.xlsx"
current_file = "C:\\Users\\Admin\\Desktop\\Datsets\\Case2\\trading_data_final.xlsx"

history_df = pd.read_excel(history_file)
current_df = pd.read_excel(current_file)

# Ensure numeric columns
numeric_cols = ['Catalyst_Quantity', 'Catalyst_Price', 'Impact_Quantity', 'Impact_Price']
history_df[numeric_cols] = history_df[numeric_cols].apply(pd.to_numeric, errors='coerce')
current_df[numeric_cols] = current_df[numeric_cols].apply(pd.to_numeric, errors='coerce')

# Compute quantity and price differences
current_df['Quantity_Difference'] = current_df['Catalyst_Quantity'] - current_df['Impact_Quantity']
current_df['Price_Difference'] = current_df['Catalyst_Price'] - current_df['Impact_Price']

# Function to determine anomaly and match status
def detect_anomaly(row):
    trade_history = history_df[history_df['Trade_ID'] == row['Trade_ID']]
    if trade_history.empty:
        return "Match", "Trade details are in line with historical data.", "No"
    
    # Compute historical averages
    avg_qty_diff = (trade_history['Catalyst_Quantity'] - trade_history['Impact_Quantity']).mean()
    avg_price_diff = (trade_history['Catalyst_Price'] - trade_history['Impact_Price']).mean()

    # Compute standard deviations
    std_qty = (trade_history['Catalyst_Quantity'] - trade_history['Impact_Quantity']).std()
    std_price = (trade_history['Catalyst_Price'] - trade_history['Impact_Price']).std()

    # Avoid false positives when both differences are 0
    if row['Quantity_Difference'] == 0 and row['Price_Difference'] == 0:
        return "Match", "Trade details are in line with historical data.", "No"

    # Threshold: Consider a break only if deviation > 1.5 * std deviation
    qty_spike = abs(row['Quantity_Difference'] - avg_qty_diff) > (1.5 * std_qty if std_qty > 0 else 10)
    price_spike = abs(row['Price_Difference'] - avg_price_diff) > (1.5 * std_price if std_price > 0 else 5)

    # Determine match status
    if qty_spike and price_spike:
        status = "Quantity_and_Price_Break"
        comment = f"🚨 Sudden spike detected in both Quantity and Price. Historical avg: Q={avg_qty_diff:.2f}, P={avg_price_diff:.2f}, current delta: Q={row['Quantity_Difference']}, P={row['Price_Difference']}."
        anomaly = "Yes"
    elif qty_spike:
        status = "Quantity_Break"
        comment = f"⚠️ Sudden spike detected in Quantity. Historical avg: {avg_qty_diff:.2f}, current delta: {row['Quantity_Difference']}."
        anomaly = "Yes"
    elif price_spike:
        status = "Price_Break"
        comment = f"⚠️ Sudden spike detected in Price. Historical avg: {avg_price_diff:.2f}, current delta: {row['Price_Difference']}."
        anomaly = "Yes"
    else:
        status = "Match"
        comment = "Trade details are in line with historical data."
        anomaly = "No"

    return status, comment, anomaly

# Apply anomaly detection to current data
current_df[['Match_Status', 'Comment', 'Anomaly']] = current_df.apply(lambda row: detect_anomaly(row), axis=1, result_type="expand")

# Define email recipients based on anomaly type
def get_recipient(row):
    if row['Anomaly'] == "Yes":
        if row['Match_Status'] == "Quantity_Break":
            return "swathidhanraj7@gmail.com"  # Replace with Catalyst team email
        elif row['Match_Status'] == "Price_Break":
            return "swathidhanraj7@gmail.com"  # Replace with Impact team email
        elif row['Match_Status'] == "Quantity_and_Price_Break":
            return ["swathidhanraj7@gmail.com", "swathidhanraj7@gmail.com"]  # Notify both teams
    return None  # No email if there's no anomaly

# Function to send email
def send_email(to_email, trade_id, comment):
    if not to_email:
        return  # Skip email if no recipient
    
    subject = f"🚨 Trade Anomaly Alert: {trade_id}"
    body = f"""
    Dear Team,

    An anomaly has been detected in Trade ID: {trade_id}.

    Issue: {comment}

    Please review and take necessary action.

    Regards,
    Trading Anomaly Detection System
    """
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    # Handle single or multiple recipients
    if isinstance(to_email, list):
        msg['To'] = ", ".join(to_email)
        recipient_list = to_email
    else:
        msg['To'] = to_email
        recipient_list = [to_email]

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()  # Secure connection
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, recipient_list, msg.as_string())
        server.quit()
        print(f"📧 Email sent to {', '.join(recipient_list)} for Trade ID {trade_id}")
    except Exception as e:
        print(f"⚠️ Failed to send email: {e}")

# Send emails for anomalies
for index, row in current_df.iterrows():
    recipient_email = get_recipient(row)
    if recipient_email:
        send_email(recipient_email, row['Trade_ID'], row['Comment'])

# Save updated current data
output_file = "C:\\Users\\Admin\\Desktop\\Datsets\\Case2\\trading_data_with_anomalies_fixed.xlsx"
current_df.to_excel(output_file, index=False)

print(f"✅ Anomaly detection complete! Results saved in '{output_file}'. Emails sent where necessary.")
