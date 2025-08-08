"""Data preprocessing module for Marvel characters."""

import time

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_utc_timestamp
from sklearn.model_selection import train_test_split

from insurance.config import ProjectConfig


class DataProcessor:
    """A class for preprocessing and managing insurance DataFrame operations.

    This class handles data preprocessing, splitting, and saving to Databricks tables.
    """

    def __init__(self, pandas_df: pd.DataFrame, config: ProjectConfig, spark: SparkSession) -> None:
        self.df = pandas_df  # Store the DataFrame as self.df
        self.config = config  # Store the configuration
        self.spark = spark

    def preprocess(self) -> None:
        """Preprocess the DataFrame stored in self.df."""
        for col in self.config.num_features:
            self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

        for col in self.config.cat_features:
            self.df[col] = self.df[col].astype("category")

        self.df[self.config.target] = pd.to_numeric(self.df[self.config.target], errors="coerce")
        self.df.dropna(subset=self.config.num_features, inplace=True)

        # Create final feature list after preprocessing
        feature_columns = [col for col in self.df.columns if col != self.config.target]
        relevant_columns = feature_columns + [self.config.target]
        self.df = self.df[relevant_columns]
        self.df["Id"] = self.df["Id"].astype("int64")
        print("\n[DataProcessor] Data after preprocessing:")
        print(self.df.head())

    def split_data(self, test_size: float = 0.2, random_state: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Split the DataFrame (self.df) into training and test sets.

        :param test_size: The proportion of the dataset to include in the test split.
        :param random_state: Controls the shuffling applied to the data before applying the split.
        :return: A tuple containing the training and test DataFrames.
        """
        train_set, test_set = train_test_split(self.df, test_size=test_size, random_state=random_state)
        return train_set, test_set

    def save_to_catalog(self, train_set: pd.DataFrame, test_set: pd.DataFrame) -> None:
        """Save the train and test sets into Databricks tables.

        :param train_set: The training DataFrame to be saved.
        :param test_set: The test DataFrame to be saved.
        """
        train_set_with_timestamp = self.spark.createDataFrame(train_set).withColumn(
            "update_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC")
        )

        test_set_with_timestamp = self.spark.createDataFrame(test_set).withColumn(
            "update_timestamp_utc", to_utc_timestamp(current_timestamp(), "UTC")
        )

        train_set_with_timestamp.write.mode("overwrite").saveAsTable(
            f"{self.config.catalog_name}.{self.config.schema_name}.train_set"
        )

        test_set_with_timestamp.write.mode("overwrite").saveAsTable(
            f"{self.config.catalog_name}.{self.config.schema_name}.test_set"
        )

    def enable_change_data_feed(self) -> None:
        """Enable Change Data Feed for train and test set tables.

        This method alters the tables to enable Change Data Feed functionality.
        """
        self.spark.sql(
            f"ALTER TABLE {self.config.catalog_name}.{self.config.schema_name}.train_set "
            "SET TBLPROPERTIES (delta.enableChangeDataFeed = true);"
        )

        self.spark.sql(
            f"ALTER TABLE {self.config.catalog_name}.{self.config.schema_name}.test_set "
            "SET TBLPROPERTIES (delta.enableChangeDataFeed = true);"
        )


def generate_synthetic_data_insurance(df: pd.DataFrame, drift: bool = False, num_rows: int = 500) -> pd.DataFrame:
    """Generate synthetic insurance data matching input DataFrame distributions with optional drift.

    Creates artificial dataset replicating statistical patterns from source columns including numeric,
    categorical, and datetime types. Supports intentional data drift for specific features when enabled.

    :param df: Source DataFrame containing original data distributions
    :param drift: Flag to activate synthetic data drift injection
    :param num_rows: Number of synthetic records to generate
    :return: DataFrame containing generated synthetic data
    """
    synthetic_data = pd.DataFrame()

    for column in df.columns:
        if column in ["Id", "update_timestamp_utc"]:
            continue

        if pd.api.types.is_numeric_dtype(df[column]):
            mean, std = df[column].mean(), df[column].std()
            synthetic_data[column] = np.random.normal(mean, std, num_rows)

            # Ensure positive for charges, bmi, age
            if column in {"charges", "bmi", "age", "children"}:
                synthetic_data[column] = np.maximum(0, synthetic_data[column])

        elif pd.api.types.is_categorical_dtype(df[column]) or df[column].dtype == object:
            # Custom constraint for known columns
            if column == "sex":
                synthetic_data[column] = np.random.choice(["male", "female"], size=num_rows)
            elif column == "smoker":
                synthetic_data[column] = np.random.choice(["yes", "no"], size=num_rows)
            else:
                choices = df[column].dropna().unique()
                probs = df[column].value_counts(normalize=True).reindex(choices, fill_value=1.0 / len(choices)).values
                synthetic_data[column] = np.random.choice(choices, size=num_rows, p=probs)

    # Convert relevant numeric columns to appropriate types
    # Assign unique IDs and timestamps
    timestamp_base = int(time.time() * 1000)
    synthetic_data["Id"] = [timestamp_base + i for i in range(num_rows)]
    synthetic_data["update_timestamp_utc"] = pd.Timestamp.utcnow()

    # Inject drift if enabled
    if drift:
        if "charges" in synthetic_data.columns:
            synthetic_data["charges"] *= 100.5  # Simulate cost increase
        if "bmi" in synthetic_data.columns:
            synthetic_data["bmi"] += 20  # Obesity trend
        if "region" in synthetic_data.columns:
            synthetic_data["region"] = np.random.choice(["southeast", "northeast"], size=num_rows)

    # Cast types to match schema
    synthetic_data = synthetic_data.astype(
        {
            "Id": "int64",
            "age": "int64",
            "children": "int64",
            "bmi": "float64",
            "charges": "float64",
            "sex": "object",
            "smoker": "object",
            "region": "object",
        }
    )
    return synthetic_data


def generate_test_data(df: pd.DataFrame, drift: bool = False, num_rows: int = 100) -> pd.DataFrame:
    """Generate test data matching input DataFrame distributions with optional drift."""
    return generate_synthetic_data_insurance(df, drift, num_rows)