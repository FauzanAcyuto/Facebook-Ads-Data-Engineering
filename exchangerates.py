

import os
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import pandas as pd
import pytz
import requests
from sqlalchemy import create_engine, types, Engine
from sqlalchemy.exc import SQLAlchemyError

def main():
    """Main entry point for the application."""
    try:
        # Load configuration
        config = Config.from_environment()
        
        # Create and run pipeline
        pipeline = CurrencyExchangePipeline(config)
        pipeline.run()
        
    except Exception as e:
        logger.error(f"Application failed: {e}")
        raise


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('currency_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class Config:
    """Configuration class for the currency pipeline."""
    
    # API configuration
    api_url: str
    api_key: str
    base_currency: str
    target_currencies: List[str]
    
    # Database configuration
    database_url: str
    table_name: str
    
    # Health check configuration
    health_check_url: str
    
    # Timezone configuration
    timezone: str = "UTC"
    
    @classmethod
    def from_environment(cls) -> 'Config':
        """Create configuration from environment variables."""
        required_vars = [
            'CURRENCY_API_URL', 'CURRENCY_API_KEY', 'DATABASE_URL',
            'CURRENCY_TABLE_NAME', 'HEALTH_CHECK_URL'
        ]
        
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
        
        # Parse target currencies from comma-separated string
        target_currencies_str = os.getenv('TARGET_CURRENCIES', 'CAD,GBP,EUR,HKD')
        target_currencies = [currency.strip() for currency in target_currencies_str.split(',')]
        
        return cls(
            api_url=os.getenv('CURRENCY_API_URL'),
            api_key=os.getenv('CURRENCY_API_KEY'),
            base_currency=os.getenv('BASE_CURRENCY', 'USD'),
            target_currencies=target_currencies,
            database_url=os.getenv('DATABASE_URL'),
            table_name=os.getenv('CURRENCY_TABLE_NAME'),
            health_check_url=os.getenv('HEALTH_CHECK_URL'),
            timezone=os.getenv('TIMEZONE', 'UTC')
        )


class CurrencyAPIClient:
    """Handles interactions with the currency API."""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        # Set timeout and retry strategy
        self.session.timeout = 30
    
    def fetch_latest_rates(self) -> Dict[str, Any]:
        """Fetch the latest exchange rates from the API."""
        try:
            params = {
                "apikey": self.config.api_key,
                "base_currency": self.config.base_currency,
                "currencies": ",".join(self.config.target_currencies),
            }
            
            logger.info(f"Fetching rates for {self.config.base_currency} -> {params['currencies']}")
            
            response = self.session.get(self.config.api_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            logger.info("Successfully fetched exchange rate data from API")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch exchange rates: {e}")
            raise
        except ValueError as e:
            logger.error(f"Failed to parse API response as JSON: {e}")
            raise
    
    def close(self) -> None:
        """Close the requests session."""
        self.session.close()


class DatabaseManager:
    """Handles database operations for currency data."""
    
    def __init__(self, config: Config):
        self.config = config
        self.engine: Optional[Engine] = None
    
    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.engine = create_engine(self.config.database_url)
            logger.info("Database connection established")
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def fetch_historical_data(self, cutoff_date: date) -> pd.DataFrame:
        """Fetch historical exchange rate data up to a specific date."""
        if not self.engine:
            raise ValueError("Database connection not established")
        
        try:
            query = f"SELECT * FROM {self.config.table_name} WHERE `Date` <= %s"
            df = pd.read_sql(query, self.engine, params=[str(cutoff_date)])
            logger.info(f"Retrieved {len(df)} historical records up to {cutoff_date}")
            return df
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to fetch historical data: {e}")
            raise
    
    def save_exchange_rates(self, df: pd.DataFrame) -> None:
        """Save exchange rate data to the database."""
        if not self.engine:
            raise ValueError("Database connection not established")
        
        try:
            # Define column types based on target currencies
            dtype_mapping = {"Date": types.DATE}
            for currency in self.config.target_currencies:
                dtype_mapping[currency] = types.DECIMAL(precision=30, scale=5)
            
            df.to_sql(
                self.config.table_name,
                self.engine,
                if_exists="replace",
                index=False,
                dtype=dtype_mapping
            )
            
            logger.info(f"Successfully saved {len(df)} records to database")
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to save data to database: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")


class ExchangeRateProcessor:
    """Processes and transforms exchange rate data."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def parse_api_response(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse and flatten the API response data."""
        try:
            # Extract exchange rate values
            rate_data = {}
            if "data" in api_data:
                for currency_code, currency_info in api_data["data"].items():
                    if isinstance(currency_info, dict) and "value" in currency_info:
                        rate_data[currency_code] = currency_info["value"]
                    else:
                        # Handle case where data is already flattened
                        rate_data[currency_code] = currency_info
            
            # Extract and parse date
            if "meta" in api_data and "last_updated_at" in api_data["meta"]:
                date_str = api_data["meta"]["last_updated_at"].split("T")[0]
                rate_data["Date"] = date_str
            else:
                # Fallback to current date if no timestamp in response
                rate_data["Date"] = date.today().strftime("%Y-%m-%d")
                logger.warning("No timestamp in API response, using current date")
            
            logger.info(f"Parsed API response for date: {rate_data['Date']}")
            return rate_data
            
        except (KeyError, TypeError, IndexError) as e:
            logger.error(f"Failed to parse API response: {e}")
            raise ValueError(f"Invalid API response format: {e}")
    
    def create_dataframe(self, rate_data: Dict[str, Any]) -> pd.DataFrame:
        """Convert rate data dictionary to DataFrame."""
        try:
            df = pd.DataFrame([rate_data])
            logger.info(f"Created DataFrame with columns: {list(df.columns)}")
            return df
        except Exception as e:
            logger.error(f"Failed to create DataFrame: {e}")
            raise
    
    def merge_with_historical(self, new_data: pd.DataFrame, 
                            historical_data: pd.DataFrame) -> pd.DataFrame:
        """Merge new data with historical data, removing duplicates."""
        try:
            # Combine datasets
            combined_df = pd.concat([historical_data, new_data], ignore_index=True)
            
            # Convert Date column to datetime for proper handling
            combined_df["Date"] = pd.to_datetime(combined_df["Date"], yearfirst=True)
            
            # Remove duplicates, keeping the latest data
            combined_df = combined_df.drop_duplicates(subset="Date", keep="last")
            
            # Sort by date and reset index
            combined_df = combined_df.sort_values("Date").reset_index(drop=True)
            
            logger.info(f"Merged data contains {len(combined_df)} total records")
            return combined_df
            
        except Exception as e:
            logger.error(f"Failed to merge datasets: {e}")
            raise
    
    def add_future_placeholders(self, df: pd.DataFrame, days_ahead: int = 2) -> pd.DataFrame:
        """Add placeholder rows for future dates."""
        try:
            df_copy = df.copy()
            
            if len(df_copy) == 0:
                logger.warning("Cannot add future placeholders to empty DataFrame")
                return df_copy
            
            # Get the last row for replication
            last_row = df_copy.iloc[-1].copy()
            last_date = last_row["Date"]
            
            future_rows = []
            for i in range(1, days_ahead + 1):
                future_row = last_row.copy()
                future_row["Date"] = last_date + timedelta(days=i)
                future_rows.append(future_row)
            
            if future_rows:
                future_df = pd.DataFrame(future_rows)
                df_copy = pd.concat([df_copy, future_df], ignore_index=True)
                logger.info(f"Added {len(future_rows)} future placeholder rows")
            
            return df_copy
            
        except Exception as e:
            logger.error(f"Failed to add future placeholders: {e}")
            raise
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate the processed data before saving."""
        try:
            # Check if DataFrame is empty
            if df.empty:
                logger.error("DataFrame is empty")
                return False
            
            # Check if Date column exists
            if "Date" not in df.columns:
                logger.error("Date column missing from DataFrame")
                return False
            
            # Check if target currencies exist
            missing_currencies = [curr for curr in self.config.target_currencies 
                                if curr not in df.columns]
            if missing_currencies:
                logger.error(f"Missing currency columns: {missing_currencies}")
                return False
            
            # Check for null values in currency columns
            for currency in self.config.target_currencies:
                null_count = df[currency].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in {currency} column")
            
            logger.info("Data validation passed")
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return False


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


class CurrencyExchangePipeline:
    """Main pipeline orchestrator for currency exchange rates."""
    
    def __init__(self, config: Config):
        self.config = config
        self.api_client = CurrencyAPIClient(config)
        self.db_manager = DatabaseManager(config)
        self.processor = ExchangeRateProcessor(config)
        self.health_checker = HealthChecker(config)
    
    def run(self) -> None:
        """Execute the complete currency exchange pipeline."""
        try:
            logger.info("Starting currency exchange rate pipeline")
            
            # Setup database connection
            self.db_manager.connect()
            
            # Calculate yesterday's date for historical data cutoff
            yesterday = date.today() - timedelta(days=1)
            
            # Fetch historical data
            historical_data = self.db_manager.fetch_historical_data(yesterday)
            
            # Fetch latest rates from API
            api_response = self.api_client.fetch_latest_rates()
            
            # Process API response
            rate_data = self.processor.parse_api_response(api_response)
            latest_df = self.processor.create_dataframe(rate_data)
            
            # Merge with historical data
            merged_data = self.processor.merge_with_historical(latest_df, historical_data)
            
            # Add future placeholders
            final_data = self.processor.add_future_placeholders(merged_data)
            
            # Validate data before saving
            if not self.processor.validate_data(final_data):
                raise ValueError("Data validation failed")
            
            # Save to database
            self.db_manager.save_exchange_rates(final_data)
            
            # Send health check
            self.health_checker.ping_health_check()
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            # Cleanup resources
            self.api_client.close()
            self.db_manager.close()


if __name__ == "__main__":
    main()
