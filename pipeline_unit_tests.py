import unittest
import pandas as pd
from pandas import DataFrame
import os
import logging
from measurements_pipeline import (
    load_dataset,
    get_cleaned_dataset,
    add_hour_metrics,
    export_dataset,
)

# Configure logging for unit tests with UTF-8 encoding for emojis
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TestMeasurementPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logger.info("🔧 Setting up test files for unit tests.")
        cls.test_filename = "test_data.csv"
        cls.export_filename = "test_export.csv"
        cls.sample_data = {
            "timestamp": ["2023-10-01 00:00:00", "2023-10-01 01:00:00", "2023-10-01 02:00:00"],
            "grid_purchase": [100, 200, 300],
            "grid_feedin": [10, 20, 30],
            "direct_consumption": [50, 60, 70],
            "date": ["2023-10-01", "2023-10-01", "2023-10-01"],  # Redundant column
        }
        cls.sample_df = pd.DataFrame(cls.sample_data)
        cls.sample_df.to_csv(cls.test_filename, index=False, sep=';')
        logger.info("✅ Test file '%s' created.", cls.test_filename)

    @classmethod
    def tearDownClass(cls):
        logger.info("🧹 Tearing down test files.")
        if os.path.exists(cls.test_filename):
            os.remove(cls.test_filename)
            logger.info("🗑️ Removed test file '%s'.", cls.test_filename)
        if os.path.exists(cls.export_filename):
            os.remove(cls.export_filename)
            logger.info("🗑️ Removed export file '%s'.", cls.export_filename)

    def setUp(self):
        logger.info("🚀 Starting test: %s", self._testMethodName)

    def tearDown(self):
        logger.info("🏁 Finished test: %s", self._testMethodName)

    def test_load_dataset(self):
        """Test loading a valid dataset."""
        df = load_dataset(self.test_filename, delimiter=';')
        self.assertFalse(df.empty, "DataFrame should not be empty for valid file.")
        self.assertEqual(df.shape[0], 3, "DataFrame should have 3 rows.")
        self.assertEqual(df.shape[1], 5, "DataFrame should have 5 columns.")

        # Test loading a non-existent file
        df_nonexistent = load_dataset("nonexistent.csv")
        self.assertTrue(df_nonexistent.empty, "DataFrame should be empty for nonexistent file.")

    def test_get_cleaned_dataset(self):
        """Test cleaning the dataset."""
        df = load_dataset(self.test_filename, delimiter=';')
        cleaned_df = get_cleaned_dataset(df)

        # Check if redundant column is removed
        self.assertNotIn("date", cleaned_df.columns, "Redundant 'date' column should be removed.")

        # Check if timestamp is set as index
        self.assertIsInstance(cleaned_df.index, pd.DatetimeIndex, "Index should be a DatetimeIndex.")

        # Check if numeric columns are converted
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_df["grid_purchase"]), "grid_purchase should be integer dtype.")
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_df["grid_feedin"]), "grid_feedin should be integer dtype.")
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_df["direct_consumption"]), "direct_consumption should be integer dtype.")

        # Check if direct_consumption_flag is added
        self.assertIn("direct_consumption_flag", cleaned_df.columns, "direct_consumption_flag should be present.")

        # Test with empty DataFrame
        empty_df = get_cleaned_dataset(DataFrame())
        self.assertTrue(empty_df.empty, "Cleaning an empty DataFrame should return an empty DataFrame.")

    def test_add_hour_metrics(self):
        """Test adding hourly metrics."""
        df = load_dataset(self.test_filename, delimiter=';')
        cleaned_df = get_cleaned_dataset(df)
        df_with_metrics = add_hour_metrics(cleaned_df)

        # Check if hourly totals are added
        self.assertIn("grid_purchase_total", df_with_metrics.columns, "grid_purchase_total should be present.")
        self.assertIn("grid_feedin_total", df_with_metrics.columns, "grid_feedin_total should be present.")

        # Check if max flags are added
        self.assertIn("max_grid_purchase_hour", df_with_metrics.columns, "max_grid_purchase_hour should be present.")
        self.assertIn("max_grid_feedin_hour", df_with_metrics.columns, "max_grid_feedin_hour should be present.")

        # Test with empty DataFrame
        empty_df = add_hour_metrics(DataFrame())
        self.assertTrue(empty_df.empty, "Adding hour metrics on an empty DataFrame should return an empty DataFrame.")

    def test_export_dataset(self):
        """Test exporting the dataset."""
        df = load_dataset(self.test_filename, delimiter=';')
        cleaned_df = get_cleaned_dataset(df)
        df_with_metrics = add_hour_metrics(cleaned_df)

        # Export and check if file is created
        export_dataset(df_with_metrics, self.export_filename)
        self.assertTrue(os.path.exists(self.export_filename), "Export file should exist after exporting.")

        # Load exported file and compare with original
        exported_df = pd.read_csv(self.export_filename, index_col='timestamp', parse_dates=True)
        self.assertEqual(exported_df.shape[0], df_with_metrics.shape[0], "Exported DataFrame row count should match.")
        self.assertEqual(exported_df.shape[1], df_with_metrics.shape[1], "Exported DataFrame column count should match.")

        # Test exporting empty DataFrame
        export_dataset(DataFrame(), "empty_export.csv")
        self.assertFalse(os.path.exists("empty_export.csv"), "Empty DataFrame should not produce an export file.")

    def test_duplicate_timestamps(self):
        """Test handling of duplicate timestamps."""
        duplicate_data = {
            "timestamp": ["2023-10-01 00:00:00", "2023-10-01 00:00:00", "2023-10-01 01:00:00"],
            "grid_purchase": [100, 200, 300],
            "grid_feedin": [10, 20, 30],
            "direct_consumption": [50, 60, 70],
        }
        duplicate_df = pd.DataFrame(duplicate_data)
        cleaned_df = get_cleaned_dataset(duplicate_df)

        # Check if duplicates are removed
        self.assertEqual(len(cleaned_df), 2, "There should be only 2 unique timestamps after cleaning.")

    def test_missing_timestamps(self):
        """Test handling of missing timestamps."""
        missing_data = {
            "timestamp": ["2023-10-01 00:00:00", None, "2023-10-01 02:00:00"],
            "grid_purchase": [100, 200, 300],
            "grid_feedin": [10, 20, 30],
            "direct_consumption": [50, 60, 70],
        }
        missing_df = pd.DataFrame(missing_data)
        cleaned_df = get_cleaned_dataset(missing_df)

        # Check if rows with missing timestamps are dropped
        self.assertEqual(len(cleaned_df), 2, "Rows with missing timestamps should be dropped.")

if __name__ == "__main__":
    unittest.main()
