import unittest
import pandas as pd
from pandas import DataFrame, to_datetime
from datetime import datetime
import os
from measurements_pipeline import (
    load_dataset,
    get_cleaned_dataset,
    add_hour_metrics,
    export_dataset,
)

class TestMeasurementPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Create a sample CSV file for testing."""
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

    @classmethod
    def tearDownClass(cls):
        """Clean up test files."""
        if os.path.exists(cls.test_filename):
            os.remove(cls.test_filename)
        if os.path.exists(cls.export_filename):
            os.remove(cls.export_filename)

    def test_load_dataset(self):
        """Test loading a valid dataset."""
        df = load_dataset(self.test_filename, delimiter=';')
        self.assertFalse(df.empty)
        self.assertEqual(df.shape[0], 3)
        self.assertEqual(df.shape[1], 5)

        # Test loading a non-existent file
        df = load_dataset("nonexistent.csv")
        self.assertTrue(df.empty)

    def test_get_cleaned_dataset(self):
        """Test cleaning the dataset."""
        df = load_dataset(self.test_filename, delimiter=';')
        cleaned_df = get_cleaned_dataset(df)

        # Check if redundant column is removed
        self.assertNotIn("date", cleaned_df.columns)

        # Check if timestamp is set as index
        self.assertIsInstance(cleaned_df.index, pd.DatetimeIndex)

        # Check if numeric columns are converted
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_df["grid_purchase"]))
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_df["grid_feedin"]))
        self.assertTrue(pd.api.types.is_integer_dtype(cleaned_df["direct_consumption"]))

        # Check if direct_consumption_flag is added
        self.assertIn("direct_consumption_flag", cleaned_df.columns)

        # Test with empty DataFrame
        empty_df = get_cleaned_dataset(pd.DataFrame())
        self.assertTrue(empty_df.empty)

    def test_add_hour_metrics(self):
        """Test adding hourly metrics."""
        df = load_dataset(self.test_filename, delimiter=';')
        cleaned_df = get_cleaned_dataset(df)
        df_with_metrics = add_hour_metrics(cleaned_df)

        # Check if hourly totals are added
        self.assertIn("grid_purchase_total", df_with_metrics.columns)
        self.assertIn("grid_feedin_total", df_with_metrics.columns)

        # Check if max flags are added
        self.assertIn("max_grid_purchase_hour", df_with_metrics.columns)
        self.assertIn("max_grid_feedin_hour", df_with_metrics.columns)

        # Test with empty DataFrame
        empty_df = add_hour_metrics(pd.DataFrame())
        self.assertTrue(empty_df.empty)

    def test_export_dataset(self):
        """Test exporting the dataset."""
        df = load_dataset(self.test_filename, delimiter=';')
        cleaned_df = get_cleaned_dataset(df)
        df_with_metrics = add_hour_metrics(cleaned_df)

        # Export and check if file is created
        export_dataset(df_with_metrics, self.export_filename)
        self.assertTrue(os.path.exists(self.export_filename))

        # Load exported file and compare with original
        exported_df = pd.read_csv(self.export_filename)
        self.assertEqual(exported_df.shape[0], df_with_metrics.shape[0])
        self.assertEqual(exported_df.shape[1], df_with_metrics.shape[1])

        # Test exporting empty DataFrame
        export_dataset(pd.DataFrame(), "empty_export.csv")
        self.assertFalse(os.path.exists("empty_export.csv"))  # Should not create file

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
        self.assertEqual(len(cleaned_df), 2)  # Only 2 unique timestamps

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
        self.assertEqual(len(cleaned_df), 2)

if __name__ == "__main__":
    unittest.main()
