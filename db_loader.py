import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import pytz

engine = create_engine("sqlite:///./store_monitoring.db")

# Load store_status
df_status = pd.read_csv('data/store_status.csv')
df_status['timestamp_utc'] = pd.to_datetime(df_status['timestamp_utc'])
df_status.insert(0, 'id', range(1, len(df_status) + 1))
df_status.to_sql('store_status', con=engine, if_exists='replace', index=False)

# Load business_hours with safety check
df_hours = pd.read_csv('data/business_hours.csv')

# Ensure required column exists
if 'day_of_week' not in df_hours.columns:
    raise Exception("❌ 'day_of_week' column is missing in business_hours.csv!")

df_hours['start_time_local'] = pd.to_datetime(df_hours['start_time_local'], format='%H:%M:%S').dt.time
df_hours['end_time_local'] = pd.to_datetime(df_hours['end_time_local'], format='%H:%M:%S').dt.time
df_hours.insert(0, 'id', range(1, len(df_hours) + 1))
df_hours.to_sql('business_hours', con=engine, if_exists='replace', index=False)

# Load store_timezones
df_tz = pd.read_csv('data/store_timezones.csv')
df_tz.insert(0, 'id', range(1, len(df_tz) + 1))
df_tz.to_sql('store_timezone', con=engine, if_exists='replace', index=False)

print("✅ All CSVs loaded into the database.")