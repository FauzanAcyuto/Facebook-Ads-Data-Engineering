
import os
import logging
import time
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pendulum
import requests
import smtplib
import ssl
from email.message import EmailMessage
from sqlalchemy import create_engine, Engine
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

def main():
    """Main entry point for the application."""
    try:
        # Load configuration
        config = Config.from_environment()
        
        # Create and run pipeline
        pipeline = AdDataPipeline(config)
        pipeline.run()
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ad_data_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Configuration class for the data pipeline."""
    
    # Database configuration
    database_url: str
    
    # BigQuery configuration
    gbq_credentials_path: str
    gbq_project: str
    gbq_dataset: str
    gbq_table: str
    gbq_source_table: str
    
    # Email configuration
    email_sender: str
    email_password: str
    email_receiver: str
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 465
    
    # Health check configuration
    health_check_url: str
    
    @classmethod
    def from_environment(cls) -> 'Config':
        """Create configuration from environment variables."""
        required_vars = [
            'DATABASE_URL', 'GBQ_CREDENTIALS_PATH', 'GBQ_PROJECT',
            'GBQ_DATASET', 'GBQ_TABLE', 'GBQ_SOURCE_TABLE',
            'EMAIL_SENDER', 'EMAIL_PASSWORD', 'EMAIL_RECEIVER',
            'HEALTH_CHECK_URL'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        return cls(
            database_url=os.getenv('DATABASE_URL'),
            gbq_credentials_path=os.getenv('GBQ_CREDENTIALS_PATH'),
            gbq_project=os.getenv('GBQ_PROJECT'),
            gbq_dataset=os.getenv('GBQ_DATASET'),
            gbq_table=os.getenv('GBQ_TABLE'),
            gbq_source_table=os.getenv('GBQ_SOURCE_TABLE'),
            email_sender=os.getenv('EMAIL_SENDER'),
            email_password=os.getenv('EMAIL_PASSWORD'),
            email_receiver=os.getenv('EMAIL_RECEIVER'),
            health_check_url=os.getenv('HEALTH_CHECK_URL')
        )


class DatabaseConnector:
    """Handles database connections and operations."""
    
    def __init__(self, config: Config):
        self.config = config
        self.sql_engine: Optional[Engine] = None
        self.gbq_client: Optional[bigquery.Client] = None
        self.gbq_credentials = None
    
    def setup_connections(self) -> None:
        """Initialize database connections."""
        try:
            # Setup SQL connection
            self.sql_engine = create_engine(self.config.database_url)
            logger.info("SQL engine connection established")
            
            # Setup BigQuery connection
            self.gbq_credentials = service_account.Credentials.from_service_account_file(
                self.config.gbq_credentials_path
            )
            self.gbq_client = bigquery.Client(
                self.config.gbq_project, 
                credentials=self.gbq_credentials
            )
            logger.info("BigQuery client connection established")
            
        except Exception as e:
            logger.error(f"Failed to setup database connections: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a BigQuery table exists."""
        try:
            table_ref = self.gbq_client.dataset(self.config.gbq_dataset).table(table_name)
            self.gbq_client.get_table(table_ref)
            return True
        except NotFound:
            return False
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            raise
    
    def read_sql_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame."""
        try:
            return pd.read_sql(query, self.sql_engine)
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise
    
    def read_gbq_query(self, query: str) -> pd.DataFrame:
        """Execute BigQuery query and return DataFrame."""
        try:
            return pd.read_gbq(query, credentials=self.gbq_credentials)
        except Exception as e:
            logger.error(f"Failed to execute BigQuery query: {e}")
            raise
    
    def write_to_gbq(self, df: pd.DataFrame, table_name: str, 
                     table_schema: Optional[List[Dict]] = None) -> None:
        """Write DataFrame to BigQuery."""
        try:
            destination = f"{self.config.gbq_project}.{self.config.gbq_dataset}.{table_name}"
            df.to_gbq(
                destination,
                credentials=self.gbq_credentials,
                if_exists="replace",
                progress_bar=True,
                table_schema=table_schema
            )
            logger.info(f"Successfully wrote {len(df)} rows to {destination}")
        except Exception as e:
            logger.error(f"Failed to write to BigQuery: {e}")
            raise
    
    def close_connections(self) -> None:
        """Close all database connections."""
        if self.sql_engine:
            self.sql_engine.dispose()
            logger.info("SQL engine connection closed")


class EmailNotifier:
    """Handles email notifications."""
    
    def __init__(self, config: Config):
        self.config = config
        self.ssl_context = ssl.create_default_context()
    
    def send_error_notification(self, error_timezones: List[str]) -> None:
        """Send email notification about timezone errors."""
        if not error_timezones:
            return
        
        try:
            subject = "Unknown timezone code in Autoloan!!"
            message = (
                f"There is an unknown timezone code or blank timezone code "
                f"in the Autoloan Adset Cost Update.\n\n"
                f"Unknown timezones: [{', '.join(error_timezones)}]"
            )
            
            em = EmailMessage()
            em["From"] = self.config.email_sender
            em["To"] = self.config.email_receiver
            em["Subject"] = subject
            em.set_content(message)
            
            with smtplib.SMTP_SSL(
                self.config.smtp_server, 
                self.config.smtp_port, 
                context=self.ssl_context
            ) as smtp:
                smtp.login(self.config.email_sender, self.config.email_password)
                smtp.sendmail(
                    self.config.email_sender,
                    self.config.email_receiver,
                    em.as_string()
                )
            
            logger.info(f"Error notification sent for timezones: {error_timezones}")
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")


class DataProcessor:
    """Handles data processing operations."""
    
    def __init__(self, db_connector: DatabaseConnector, email_notifier: EmailNotifier):
        self.db = db_connector
        self.email_notifier = email_notifier
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize column names."""
        df = df.copy()
        df.columns = df.columns.str.lower()
        return df
    
    def standardize_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert DataFrame columns to appropriate data types."""
        df = df.copy()
        
        type_mapping = {
            "date_start": "str",
            "date_stop": "str",
            "account_id": "str",
            "campaign_name": "str",
            "ad_set_id": "str",
            "ad_set_name": "str",
        }
        
        # Only convert columns that exist in the DataFrame
        existing_columns = {col: dtype for col, dtype in type_mapping.items() 
                          if col in df.columns}
        
        if existing_columns:
            df = df.astype(existing_columns)
            logger.info(f"Converted data types for columns: {list(existing_columns.keys())}")
        
        return df
    
    def process_hour_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract and process hourly data."""
        df = df.copy()
        
        # Rename the hourly stats column if it exists
        hourly_col = "hourly_stats_aggregated_by_advertiser_time_zone"
        if hourly_col in df.columns:
            df = df.rename(columns={hourly_col: "hour"})
        
        # Extract hour information
        if "hour" in df.columns:
            df["hour"] = df["hour"].str.split(" - ").str[0]
            df["source_datetime"] = df["date_start"] + "T" + df["hour"]
        
        return df
    
    def get_timezone_data(self) -> pd.DataFrame:
        """Fetch timezone data from the database."""
        query = "SELECT adaccount_id, timezone FROM account_timezones"
        
        try:
            timezone_df = self.db.read_sql_query(query)
            # Clean timezone format
            timezone_df["timezone"] = timezone_df["timezone"].str.rsplit(" ").str[-1]
            logger.info(f"Retrieved {len(timezone_df)} timezone records")
            return timezone_df
        except Exception as e:
            logger.error(f"Failed to fetch timezone data: {e}")
            raise
    
    def parse_datetime_with_timezone(self, row: pd.Series) -> pendulum.DateTime:
        """Safely parse datetime with timezone information."""
        try:
            return pendulum.parse(row['source_datetime'], tz=row['timezone'])
        except (pendulum.ParsingException, ValueError, TypeError) as e:
            logger.warning(f"Failed to parse datetime for timezone {row.get('timezone')}: {e}")
            return pendulum.datetime(1999, 1, 1)
    
    def process_timezones(self, df: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
        """Process timezone data and convert to Pacific time."""
        df = df.copy()
        error_timezones = []
        
        # Get timezone data
        timezone_df = self.get_timezone_data()
        
        # Merge with main dataset
        df = df.merge(
            timezone_df,
            how="left",
            left_on="account_id",
            right_on="adaccount_id"
        ).drop("adaccount_id", axis=1)
        
        # Parse datetimes with timezone - vectorized approach with error tracking
        def parse_with_error_tracking(row):
            parsed_dt = self.parse_datetime_with_timezone(row)
            if parsed_dt.year == 1999:  # Our error indicator
                error_timezones.append(row.get('timezone', 'Unknown'))
            return parsed_dt
        
        df['source_datetime'] = df.apply(parse_with_error_tracking, axis=1)
        
        # Convert to Pacific time
        df['pacific_datetime'] = df['source_datetime'].apply(
            lambda dt: dt.in_tz("US/Pacific") if hasattr(dt, 'in_tz') else dt
        )
        
        # Convert datetime objects to strings for SQL compatibility
        df['source_datetime'] = df['source_datetime'].astype(str)
        df['pacific_datetime'] = df['pacific_datetime'].astype(str)
        
        # Convert amount_spend to float if it exists
        if 'amount_spend' in df.columns:
            df['amount_spend'] = pd.to_numeric(df['amount_spend'], errors='coerce')
        
        # Remove duplicates from error list
        error_timezones = list(set([tz for tz in error_timezones if tz and tz != 'Unknown']))
        
        logger.info(f"Processed {len(df)} records with {len(error_timezones)} timezone errors")
        return df, error_timezones
    
    def merge_datasets(self, new_data: pd.DataFrame, historical_data: pd.DataFrame) -> pd.DataFrame:
        """Merge new data with historical data, prioritizing new data."""
        # Add source labels
        new_data = new_data.copy()
        historical_data = historical_data.copy()
        
        new_data['source'] = '1. Coupler'
        historical_data['source'] = '2. GBQ'
        
        # Ensure consistent data types
        for df in [new_data, historical_data]:
            df['date_start'] = df['date_start'].astype(str)
            df['date_stop'] = df['date_stop'].astype(str)
            df['source_datetime'] = df['source_datetime'].astype(str)
            if 'amount_spend' in df.columns:
                df['amount_spend'] = pd.to_numeric(df['amount_spend'], errors='coerce')
        
        # Combine datasets
        merged = pd.concat([new_data, historical_data], ignore_index=True)
        
        # Sort by source to prioritize Coupler data
        merged = merged.sort_values('source', ascending=True)
        
        # Remove duplicates, keeping first occurrence (Coupler data)
        merged = merged.drop_duplicates(
            subset=['source_datetime', 'account_id', 'ad_set_id'],
            keep='first'
        )
        
        # Clean up and sort
        merged = merged.drop('source', axis=1)
        merged = merged.sort_values('source_datetime').reset_index(drop=True)
        
        logger.info(f"Merged dataset contains {len(merged)} records")
        return merged


class HealthChecker:
    """Handles health check notifications."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def ping_health_check(self) -> None:
        """Send health check ping."""
        try:
            response = requests.get(self.config.health_check_url, timeout=10)
            response.raise_for_status()
            logger.info("Health check ping successful")
        except requests.RequestException as e:
            logger.error(f"Health check ping failed: {e}")


class AdDataPipeline:
    """Main pipeline orchestrator."""
    
    def __init__(self, config: Config):
        self.config = config
        self.db_connector = DatabaseConnector(config)
        self.email_notifier = EmailNotifier(config)
        self.data_processor = DataProcessor(self.db_connector, self.email_notifier)
        self.health_checker = HealthChecker(config)
        self.start_time = time.time()
    
    def run(self) -> None:
        """Execute the complete data pipeline."""
        try:
            logger.info("Starting ad data pipeline")
            
            # Setup connections
            self.db_connector.setup_connections()
            
            # Load source data
            source_data = self._load_source_data()
            
            # Process the data
            processed_data, error_timezones = self._process_data(source_data)
            
            # Send error notifications if needed
            if error_timezones:
                self.email_notifier.send_error_notification(error_timezones)
            
            # Handle data insertion/merging
            self._handle_data_output(processed_data)
            
            # Send health check
            self.health_checker.ping_health_check()
            
            # Log completion
            execution_time = time.time() - self.start_time
            logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.db_connector.close_connections()
    
    def _load_source_data(self) -> pd.DataFrame:
        """Load data from the source BigQuery table."""
        query = f"SELECT * FROM `{self.config.gbq_source_table}`"
        return self.db_connector.read_gbq_query(query)
    
    def _process_data(self, raw_data: pd.DataFrame) -> tuple[pd.DataFrame, List[str]]:
        """Process the raw data through all transformation steps."""
        # Clean and standardize
        data = self.data_processor.clean_column_names(raw_data)
        data = self.data_processor.standardize_data_types(data)
        
        # Sort by date
        data = data.sort_values('date_start')
        
        # Process hourly data
        data = self.data_processor.process_hour_data(data)
        
        # Process timezones
        data, error_timezones = self.data_processor.process_timezones(data)
        
        return data, error_timezones
    
    def _handle_data_output(self, processed_data: pd.DataFrame) -> None:
        """Handle the final data output to BigQuery."""
        table_schema = [
            {"name": "date_start", "type": "DATE"},
            {"name": "date_stop", "type": "DATE"},
        ]
        
        # Check if destination table exists
        if not self.db_connector.table_exists(self.config.gbq_table):
            logger.info("Destination table not found, creating new table")
            self.db_connector.write_to_gbq(
                processed_data, 
                self.config.gbq_table,
                table_schema
            )
        else:
            logger.info("Destination table found, merging with historical data")
            
            # Load historical data
            historical_query = f"SELECT * FROM `{self.config.gbq_project}.{self.config.gbq_dataset}.{self.config.gbq_table}`"
            historical_data = self.db_connector.read_gbq_query(historical_query)
            
            # Merge datasets
            merged_data = self.data_processor.merge_datasets(processed_data, historical_data)
            
            # Write merged data
            self.db_connector.write_to_gbq(
                merged_data,
                self.config.gbq_table,
                table_schema
            )





if __name__ == "__main__":
    main()
