#!/usr/bin/env python
"""
Financial Modeling Prep (FMP) API Wrapper
Provides functions for retrieving 1-minute cryptocurrency data and scheduled updates
"""
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pandas as pd
import requests
import time
import logging
from typing import List
from datetime import timezone
import pytz

load_dotenv()
API_KEY = os.getenv("FMP_API_KEY", "")

# Import configuration
from config import DATA_PATH_FORMAT

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_1min_historical_data(symbol: str, start_date: str, end_date: str,
                             api_key: str = API_KEY,
                             save_to_parquet: bool = True) -> pd.DataFrame:
    """
    获取指定时间段内的1分钟历史数据

    Args:
        symbol: 加密货币符号 (e.g. "BTCUSD")
        start_date: 开始日期 (格式: "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS")
        end_date: 结束日期 (格式: "YYYY-MM-DD")
        api_key: FMP API密钥
        save_to_parquet: 是否保存数据到parquet文件，默认为True

    Returns:
        包含1分钟历史数据的DataFrame
    """
    # 将字符串日期转为datetime对象
    # Handle both "YYYY-MM-DD" and "YYYY-MM-DD HH:MM:SS" formats
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")

    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # FMP的分钟数据API每次可返回最多5000条记录，约为3.47天的分钟数据
    # 使用3天的时间窗口确保在限制内
    # If the start date includes time information, we still make requests by date
    # but will filter the results afterwards
    delta = timedelta(days=2)

    all_data = []
    current_start = start_dt

    # If the start date includes time, we need to make sure to get the full day
    # but then filter the results to start from the exact time
    start_date_only = start_dt.date()
    end_date_only = end_dt.date()

    # Adjust current_start to date only for API requests
    current_start = datetime.combine(current_start.date(), datetime.min.time())

    # If we're starting from the same day as the end date and the start has time info
    if start_date_only == end_date_only and start_dt != datetime.combine(start_dt.date(), datetime.min.time()):
        # If start and end are the same day and start has time, just request that single day
        start_str = start_date_only.strftime("%Y-%m-%d")
        end_str = end_date_only.strftime("%Y-%m-%d")

        logger.info(f"Fetching 1min data for {symbol} from {start_str} to {end_str}...")

        try:
            # FMP提供分钟级历史数据的API端点
            url = f"https://financialmodelingprep.com/stable/historical-chart/1min?symbol={symbol}&apikey={api_key}&from={start_str}&to={end_str}"
            resp = requests.get(url, timeout=30)  # Increased timeout

            if resp.status_code != 200:
                logger.error(f"API request failed with status {resp.status_code}: {resp.text}")
                return pd.DataFrame()

            data = resp.json()

            if data and isinstance(data, list):
                df = pd.DataFrame(data)

                # 确保DataFrame有数据且包含必要的列
                if not df.empty and 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])

                    # Convert from Eastern Time to UTC
                    eastern_tz = pytz.timezone('US/Eastern')
                    df['date'] = df['date'].dt.tz_localize(eastern_tz).dt.tz_convert('UTC')

                    # Filter to only include data after the specified start time
                    df = df[df['date'] >= start_dt]

                    df = df.sort_values("date")
                    all_data.append(df)
                else:
                    logger.warning(f"No valid data received for {symbol} from {start_str} to {end_str}")
            else:
                logger.warning(f"No data received for {symbol} from {start_str} to {end_str}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol} from {start_str} to {end_str}: {e}")
        except Exception as e:
            logger.error(f"Error processing data for {symbol} from {start_str} to {end_str}: {e}")
    else:
        # Handle multi-day requests
        last_successful_date = None

        while current_start.date() <= end_dt.date():
            current_end = min(current_start + delta, end_dt.replace(hour=23, minute=59, second=59))

            # 转为字符串格式
            start_str = current_start.strftime("%Y-%m-%d")
            end_str = current_end.strftime("%Y-%m-%d")

            logger.info(f"Fetching 1min data for {symbol} from {start_str} to {end_str}...")

            try:
                # FMP提供分钟级历史数据的API端点
                url = f"https://financialmodelingprep.com/stable/historical-chart/1min?symbol={symbol}&apikey={api_key}&from={start_str}&to={end_str}"
                resp = requests.get(url, timeout=30)  # Increased timeout

                if resp.status_code != 200:
                    logger.error(f"API request failed with status {resp.status_code}: {resp.text}")
                    # Track the last successful date to allow continuation from the gap
                    if last_successful_date:
                        current_start = datetime.combine(last_successful_date, datetime.min.time()) + timedelta(days=1)
                    else:
                        current_start = current_end + timedelta(days=1)
                    continue

                data = resp.json()

                if data and isinstance(data, list):
                    df = pd.DataFrame(data)

                    # 确保DataFrame有数据且包含必要的列
                    if not df.empty and 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])

                        # Convert from Eastern Time to UTC
                        eastern_tz = pytz.timezone('US/Eastern')
                        df['date'] = df['date'].dt.tz_localize(eastern_tz).dt.tz_convert('UTC')

                        # For the first day, filter to only include data after the specified start time
                        if current_start.date() == start_date_only and start_dt != datetime.combine(start_dt.date(),
                                                                                                    datetime.min.time()):
                            df = df[df['date'] >= start_dt]

                        df = df.sort_values("date")
                        all_data.append(df)
                        last_successful_date = current_end.date()  # Track successful date
                    else:
                        logger.warning(f"No valid data received for {symbol} from {start_str} to {end_str}")
                        # Even if no data, continue to the next date range
                        last_successful_date = current_end.date()
                else:
                    logger.warning(f"No data received for {symbol} from {start_str} to {end_str}")
                    # Even if no data, continue to the next date range
                    last_successful_date = current_end.date()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed for {symbol} from {start_str} to {end_str}: {e}")
                # Continue with next date range despite the error
                last_successful_date = current_end.date()
            except Exception as e:
                logger.error(f"Error processing data for {symbol} from {start_str} to {end_str}: {e}")
                # Continue with next date range despite the error
                last_successful_date = current_end.date()
            finally:
                # 移动到下一个时间段
                current_start = current_end + timedelta(days=1)

                # 添加延迟以避免API限频
                time.sleep(0.25)

    # 拼接所有数据
    if all_data:
        full_df = pd.concat(all_data, ignore_index=True)
        if save_to_parquet and not full_df.empty:
            save_to_parquet_file(full_df, symbol)
        return full_df
    else:
        return pd.DataFrame()



def save_to_parquet_file(df: pd.DataFrame, symbol: str) -> None:
    """
    将DataFrame保存到parquet文件，按照年份组织，追加到现有数据

    Args:
        df: 包含历史数据的DataFrame
        symbol: 加密货币符号
    """
    if df.empty or 'date' not in df.columns:
        logger.warning("DataFrame is empty or missing 'date' column, cannot save to parquet")
        return

    # Ensure the date column is datetime type
    df['date'] = pd.to_datetime(df['date'])

    # Group by year to create separate files
    grouped = df.groupby(df['date'].dt.year)

    for year, year_df in grouped:
        # Create the path with the year included
        file_path = DATA_PATH_FORMAT.format(market="crypto", freq="1min", symbol=symbol, year=year)

        # Create directory if it doesn't exist
        directory = os.path.dirname(file_path)
        os.makedirs(directory, exist_ok=True)

        # Check if the file already exists
        if os.path.exists(file_path):
            # Load existing data
            existing_df = pd.read_parquet(file_path)

            # Combine with new data and remove duplicates
            combined_df = pd.concat([existing_df, year_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
            combined_df = combined_df.sort_values('date')

            # Save to parquet
            combined_df.to_parquet(file_path, index=False)
            logger.info(f"Updated {file_path} with {len(year_df)} new rows, total rows: {len(combined_df)}")
        else:
            # Save new data to parquet
            year_df.to_parquet(file_path, index=False)
            logger.info(f"Saved {len(year_df)} rows to new file {file_path}")


def get_latest_data(symbol: str, limit: int = 1) -> pd.DataFrame:
    """
    获取指定加密货币的最新数据

    Args:
        symbol: 加密货币符号
        limit: 返回的最新记录数

    Returns:
        包含最新数据的DataFrame
    """
    # Find the most recent year for the symbol
    base_path = os.path.join(os.getenv("BASE_DATA_DIR", "/root/data"), "crypto", "1min", symbol)

    if not os.path.exists(base_path):
        logger.warning(f"No existing data found for symbol {symbol}")
        return pd.DataFrame()

    # Find all year directories
    years = [d for d in os.listdir(base_path)
             if os.path.isdir(os.path.join(base_path, d)) and d.isdigit()]

    if not years:
        logger.warning(f"No year directories found for symbol {symbol}")
        return pd.DataFrame()

    # Sort years in descending order to get the most recent first
    years.sort(reverse=True)

    all_data = []
    rows_needed = limit

    for year in years:
        file_path = os.path.join(base_path, year, "data.parquet")

        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)

                # Ensure date column is in UTC timezone if it exists
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
                    if df['date'].dt.tz is None:
                        # Assume it's in Eastern Time and convert to UTC
                        eastern_tz = pytz.timezone('US/Eastern')
                        df['date'] = df['date'].dt.tz_localize(eastern_tz).dt.tz_convert('UTC')

                if df.empty:
                    continue

                df = df.sort_values('date', ascending=False)  # Sort descending to get latest first

                if len(df) >= rows_needed:
                    all_data.append(df.head(rows_needed))
                    break
                else:
                    all_data.append(df)
                    rows_needed -= len(df)

                if rows_needed <= 0:
                    break
            except Exception as e:
                logger.error(f"Error reading data for {symbol} from {file_path}: {e}")

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        result = result.sort_values('date', ascending=False)  # Most recent first
        return result.head(limit).sort_values('date')  # Return requested number, sorted chronologically
    else:
        return pd.DataFrame()


def scheduled_update(symbols: List[str],
                     api_key: str = API_KEY,
                     db_table_prefix: str = "FMP",
                     write_to_database: bool = False) -> dict:
    """
    定时更新数据到数据库

    Args:
        symbols: 要更新的加密货币符号列表
        api_key: FMP API密钥
        db_table_prefix: 数据库表名前缀
        write_to_database: 是否写入数据库，默认为False

    Returns:
        更新结果的字典
    """
    # Import here to make it optional
    if write_to_database:
        from db_info import write_to_db

    results = {}

    for symbol in symbols:
        try:
            logger.info(f"Starting scheduled update for {symbol}...")

            # 查询已有的最新数据日期
            latest_df = get_latest_data(symbol, limit=1)

            if not latest_df.empty and 'date' in latest_df.columns:
                # 获取最新数据的日期，并从下一分钟开始获取新数据
                latest_date = latest_df['date'].max()
                # Calculate the next minute from the latest data point
                next_datetime = latest_date + timedelta(minutes=1)
                start_date = next_datetime.strftime("%Y-%m-%d")
                start_time = next_datetime.strftime("%H:%M:%S")

                logger.info(
                    f"Latest data for {symbol} found at {latest_date}, starting update from {start_date} {start_time}")

                # Combine date and time for the actual start
                actual_start = f"{start_date} {start_time}"
            else:
                # 如果没有现有数据，则从30天前开始获取
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                actual_start = start_date  # Use only date for initial fetch
                logger.info(f"No existing data for {symbol}, starting update from {start_date}")

            # 结束日期为今天
            end_date = datetime.now().strftime("%Y-%m-%d")

            # 获取数据
            data = get_1min_historical_data(symbol, actual_start, end_date, api_key)

            if not data.empty:
                # 过滤数据，只保留从最新日期之后的数据（避免重复）
                if not latest_df.empty and 'date' in latest_df.columns:
                    latest_date = latest_df['date'].max()
                    data = data[data['date'] > latest_date]

                if not data.empty:
                    # 准备数据库表名
                    table_name = f"{db_table_prefix}_{symbol}_Min1"

                    if write_to_database:
                        # 写入数据库
                        write_to_db(data, table_name)

                    results[symbol] = {
                        "status": "success",
                        "rows_inserted": len(data),
                        "date_range": [str(data['date'].min()),
                                       str(data['date'].max())] if 'date' in data.columns else "N/A"
                    }
                    logger.info(f"Successfully updated {symbol}, {len(data)} rows inserted")
                else:
                    results[symbol] = {
                        "status": "success",
                        "rows_inserted": 0,
                        "message": "No new data to insert after filtering duplicates"
                    }
                    logger.info(f"No new data for {symbol} after filtering duplicates")
            else:
                results[symbol] = {
                    "status": "success",
                    "rows_inserted": 0,
                    "message": "No new data to insert"
                }
                logger.warning(f"No new data for {symbol}")

        except Exception as e:
            results[symbol] = {
                "status": "error",
                "error_message": str(e)
            }
            logger.error(f"Error updating {symbol}: {e}")

        # 添加延迟以避免API限频
        time.sleep(0.5)

    return results


if __name__ == '__main__':

    # history
    crypto_to_update = ["BTCUSD", "ETHUSD", "DOGEUSD", "SOLUSD", "BNBUSD", "XRPUSD"]
    # us_to_update = ["TSLA", "NDX", "NVDA", "AMZN", "GOOGL"]
    # symbols_to_update = crypto_to_update + us_to_update
    symbols_to_update = crypto_to_update
    for symbol in symbols_to_update:
        a = get_1min_historical_data(symbol, start_date="2022-10-12", end_date="2022-10-25")

    # update
    logger.info(f"Starting scheduled update for {len(symbols_to_update)} symbols...")

    # Perform the scheduled update (without writing to DB for testing)
    update_results = scheduled_update(symbols=symbols_to_update, write_to_database=False)

    # Print results
    for symbol, result in update_results.items():
        logger.info(f"{symbol}: {result}")
