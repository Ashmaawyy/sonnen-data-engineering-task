from pandas import read_csv, DataFrame, to_datetime, to_numeric
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import logging
import time
import atexit

# Configure logging once for the module with UTF-8 encoding for emojis
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more verbosity during development
    format='%(asctime)s %(name)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler("pipeline.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Global DataFrame to hold the measurements
measurements_data = DataFrame()

# First Stage: Loading the dataset
def load_dataset(filename: str, delimiter: str = ';') -> DataFrame:
    """Load dataset from a CSV file."""
    try:
        df = read_csv(filename, delimiter=delimiter)
        logger.info("✅ Loaded dataset with %s rows and %s columns.", df.shape[0], df.shape[1])
        return df

    except FileNotFoundError:
        logger.error("❌ File not found: %s", filename)
        return DataFrame()

    except Exception as e:
        logger.exception("❌ An error occurred while loading dataset")
        return DataFrame()

# Second Stage: Cleaning the dataset
def get_cleaned_dataset(df: DataFrame) -> DataFrame:
    """Clean and preprocess raw measurement data."""
    try:
        if df.empty:
            logger.warning("⚠️ DataFrame is empty, skipping cleaning process.")
            return df
        
        required_columns = ['timestamp', 'grid_purchase', 'grid_feedin', 'direct_consumption']
        if not all(col in df.columns for col in required_columns):
            logger.error("❌ Missing required columns in dataset.")
            return DataFrame()

        # Task #1: Remove the Dev test rows
        df = df[df['direct_consumption'] != 'Dev test']

        # Task #2: Convert columns to numeric, handling errors
        for col in ['grid_purchase', 'grid_feedin', 'direct_consumption']:
            df.loc[:, col] = to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Task #3: Remove the redundant date column
        df = df.drop(columns=['date'], errors='ignore')

        # Task #4: Convert the timestamp column to datetime
        df['timestamp'] = to_datetime(df['timestamp'], errors='coerce')

        # Task #5: Drop rows where timestamp is missing before setting as index
        df = df.dropna(subset=['timestamp']).set_index('timestamp')
        df = df[~df.index.duplicated(keep='first')]

        # Task #6: Replace null values in selected columns with 0
        df.loc[:, ['grid_purchase', 'grid_feedin']] = df[['grid_purchase', 'grid_feedin']].fillna(0).copy()

        # Task #7: Add a flag column to indicate where direct_consumption is greater than zero
        df['direct_consumption_flag'] = df['direct_consumption'] > 0

        return df

    except Exception as e:
        logger.exception("❌ An error occurred while cleaning")
        return df

def add_hour_metrics(df: DataFrame) -> DataFrame:
    """Add hourly aggregated metrics to DataFrame."""
    try:
        if df.empty:
            logger.warning("⚠️ DataFrame is empty, skipping hour metrics.")
            return df

        df['hour'] = df.index.hour
        hourly_totals = df.groupby('hour')[['grid_purchase', 'grid_feedin']].sum()
        hourly_totals.columns = [col + '_total' for col in hourly_totals.columns]

        df = df.join(hourly_totals, on='hour', how='left')
        df = df.drop(columns=['hour'])

        # Identify the hour with the maximum grid purchase and grid feed-in
        df['max_grid_purchase_hour'] = df.groupby(df.index.floor('D'))['grid_purchase'].transform(lambda x: x == x.max())
        df['max_grid_feedin_hour'] = df.groupby(df.index.floor('D'))['grid_feedin'].transform(lambda x: x == x.max())

        return df

    except Exception as e:
        logger.exception("❌ An error occurred while adding hour metrics")
        return df

# Third Stage: Exporting the cleaned dataset
def export_dataset(df: DataFrame, filename: str, delimiter: str = ',') -> None:
    """Export DataFrame to CSV file."""
    try:
        if df.empty:
            logger.warning("⚠️ No data to export.")
            return
        df.to_csv(filename, sep=delimiter, index=True, encoding='utf-8')
        logger.info("✅ Exported dataset with %s rows and %s columns.", df.shape[0], df.shape[1])

    except Exception as e:
        logger.exception("❌ An error occurred while exporting")

# Fourth Stage: Scheduling the pipeline
def load_dataset_job():
    """Scheduled job to load dataset into global measurements_data."""
    global measurements_data
    measurements_data = load_dataset('measurements_coding_challenge.csv', ';')

def get_cleaned_dataset_job():
    """Scheduled job to clean global measurements_data in-place."""
    global measurements_data
    if measurements_data.empty:
        logger.warning("⚠️ Skipping cleaning: No data loaded yet.")
        return
    measurements_data = get_cleaned_dataset(measurements_data)

def add_hour_metrics_job():
    """Scheduled job to add hourly metrics to global measurements_data in-place."""
    global measurements_data
    if measurements_data.empty:
        logger.warning("⚠️ Skipping hour metrics: No data available.")
        return
    measurements_data = add_hour_metrics(measurements_data)

def export_dataset_job():
    """Scheduled job to export global measurements_data to a CSV file."""
    global measurements_data
    if measurements_data.empty:
        logger.warning("⚠️ Skipping export: No data available.")
        return
    export_dataset(measurements_data, 'cleaned_measurements.csv', ',')

scheduler_instance = None

def schedule_pipeline() -> None:
    """Initialize and start the scheduled pipeline jobs."""
    global scheduler_instance
    if scheduler_instance is not None:
        logger.warning("⚠️ Scheduler is already running. Skipping duplicate scheduling.")
        return

    scheduler_instance = BackgroundScheduler()
    scheduler_instance.add_job(load_dataset_job, 'interval', minutes=5, next_run_time=datetime.now())
    scheduler_instance.add_job(get_cleaned_dataset_job, 'interval', minutes=5, next_run_time=datetime.now() + timedelta(seconds=10))
    scheduler_instance.add_job(add_hour_metrics_job, 'interval', minutes=5, next_run_time=datetime.now() + timedelta(seconds=15))
    scheduler_instance.add_job(export_dataset_job, 'interval', minutes=5, next_run_time=datetime.now() + timedelta(seconds=20))

    scheduler_instance.start()
    logger.info("🚀 Pipeline scheduler started.")

    # Shut down the scheduler when exiting the script
    atexit.register(lambda: scheduler_instance.shutdown())

# Run the pipeline
if __name__ == '__main__':
    while True:
        schedule_pipeline()
        time.sleep(60)
