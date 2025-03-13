from pandas import read_csv, DataFrame, to_datetime, to_numeric, concat
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import time

measurements_data = DataFrame()

# First Stage: Loading the dataset
def load_dataset(filename: str, delimiter: str = ';') -> DataFrame:
    try:
        df = read_csv(filename, delimiter=delimiter)
        print(f"✅ Loaded dataset with {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    except FileNotFoundError:
        print("❌ File not found")
        return DataFrame()
    except Exception as e:
        print("❌ An error occurred", e)
        return DataFrame()

# Second Stage: Cleaning the dataset and adding hour metrics
def get_cleaned_dataset(df: DataFrame) -> DataFrame:
    try:
        if df.empty:
            print("⚠️ DataFrame is empty, skipping cleaning process.")
            return df

        # Task #1: Remove the Dev test rows
        df = df[df['direct_consumption'] != 'Dev test']

        # Task #2: Convert columns to numeric, handling errors
        for col in ['grid_purchase', 'grid_feedin', 'direct_consumption']:
            df.loc[:, col] = to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Task #3: Remove the redundant date column if it exists
        df = df.drop(columns=['date'], errors='ignore')

        # Task #4: Convert the timestamp column to datetime
        df['timestamp'] = to_datetime(df['timestamp'], errors='coerce')

        # Task #5: Drop rows where timestamp is missing before setting as index
        df = df.dropna(subset=['timestamp']).set_index('timestamp')

        # Task #6: Replace null values in selected columns with 0
        df.loc[:, ['grid_purchase', 'grid_feedin']] = df[['grid_purchase', 'grid_feedin']].fillna(0)

        # Task #7: Add a flag column to indicate where direct_consumption is greater than zero
        df['direct_consumption_flag'] = df['direct_consumption'] > 0
        return df
    except Exception as e:
        print("❌ An error occurred while cleaning", e)
        return df

def add_hour_metrics(df: DataFrame) -> DataFrame:
    try:
        if df.empty:
            print("⚠️ DataFrame is empty, skipping hour metrics.")
            return df
        
        # Calculate total grid_purchase and grid_feedin per hour
        df['hour'] = df.index.hour
        hourly_totals = df.groupby('hour')[['grid_purchase', 'grid_feedin']].sum()
        hourly_totals.columns = [col +'_total' for col in hourly_totals.columns]
        hourly_totals = df['hour'].map(hourly_totals.to_dict())

        # Concatenate the hourly totals to the original DataFrame
        df = concat([df, hourly_totals], axis=1)
        
        # Identify the hour with the highest grid_feedin of the day
        df['max_grid_feedin_hour'] = df.groupby(df.index.date)['grid_feedin'].transform(lambda x: x == x.max())
        return df
    
    except Exception as e:
        print("❌ An error occurred while adding hour metrics", e)
        return df


# Third Stage: Exporting the cleaned dataset
def export_dataset(df: DataFrame, filename: str, delimiter: str = ',') -> None:
    try:
        if df.empty:
            print("⚠️ No data to export.")
            return
        df.to_csv(filename, sep=delimiter, index=True, encoding='utf-8')
        print(f"✅ Exported dataset with {df.shape[0]} rows and {df.shape[1]} columns.")
    except Exception as e:
        print("❌ An error occurred while exporting", e)

# Fourth Stage: Scheduling the pipeline
def load_dataset_job():
    global measurements_data
    measurements_data = load_dataset('measurements_coding_challenge.csv', ';')

def cleaned_dataset_job():
    global measurements_data
    if measurements_data.empty:
        print("⚠️ Skipping cleaning: No data loaded yet.")
        return
    cleaned_data = get_cleaned_dataset(measurements_data)
    if not cleaned_data.empty:
        measurements_data = cleaned_data

def add_hour_metrics_job():
    global measurements_data
    if measurements_data.empty:
        print("⚠️ Skipping hour metrics: No data available.")
        return
    measurements_data = add_hour_metrics(measurements_data)

def export_dataset_job():
    global measurements_data
    if measurements_data.empty:
        print("⚠️ Skipping export: No data available.")
        return
    export_dataset(measurements_data, 'cleaned_measurements.csv', ',')

def schedule_pipeline() -> None:
    scheduler = BackgroundScheduler()
    
    if scheduler.get_jobs():
        print("⚠️ Scheduler is already running. Skipping duplicate scheduling.")
        return

    scheduler.add_job(load_dataset_job, 'interval', minutes=5, next_run_time=datetime.now())
    scheduler.add_job(cleaned_dataset_job, 'interval', minutes=5, next_run_time=datetime.now() + timedelta(seconds=10))
    scheduler.add_job(add_hour_metrics_job, 'interval', minutes=5, next_run_time=datetime.now() + timedelta(seconds=15))
    scheduler.add_job(export_dataset_job, 'interval', minutes=5, next_run_time=datetime.now() + timedelta(seconds=20))
    
    scheduler.start()
    print("✅ Pipeline scheduler started.")

# Run the pipeline
if __name__ == '__main__':
    while True:
        schedule_pipeline()
        time.sleep(60)
