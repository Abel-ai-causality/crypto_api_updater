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
        start_date: 开始日期 (格式: "YYYY-MM-DD")
        end_date: 结束日期 (格式: "YYYY-MM-DD")
        api_key: FMP API密钥
        save_to_parquet: 是否保存数据到parquet文件，默认为True
    
    Returns:
        包含1分钟历史数据的DataFrame
    """
    # 将字符串日期转为datetime对象
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # FMP的分钟数据API每次可返回最多5000条记录，约为3.47天的分钟数据
    # 使用3天的时间窗口确保在限制内
    delta = timedelta(days=3)
    
    all_data = []
    current_start = start_dt
    
    while current_start <= end_dt:
        current_end = min(current_start + delta, end_dt)
        
        # 转为字符串格式
        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")
        
        logger.info(f"Fetching 1min data for {symbol} from {start_str} to {end_str}...")
        
        try:
            # FMP提供分钟级历史数据的API端点
            url = f"https://financialmodelingprep.com/stable/historical-chart/1min?symbol={symbol}&apikey={api_key}&from={start_str}&to={end_str}"
            resp = requests.get(url, timeout=10)
            
            if resp.status_code != 200:
                logger.error(f"API request failed with status {resp.status_code}: {resp.text}")
                current_start = current_end + timedelta(days=1)
                continue
                
            data = resp.json()
            
            if data and isinstance(data, list):
                df = pd.DataFrame(data)
                
                # 确保DataFrame有数据且包含必要的列
                if not df.empty and 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'])
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


def get_minute_update(symbol: str, api_key: str = API_KEY) -> pd.DataFrame:
    """
    获取过去1分钟的最新数据，用于分钟级更新

    Args:
        symbol: 加密货币符号
        api_key: API密钥

    Returns:
        包含最新1分钟数据的DataFrame
    """
    # Get the latest stored data time
    latest_df = get_latest_data(symbol, limit=1)
    
    if not latest_df.empty and 'date' in latest_df.columns:
        # Get the most recent date from stored data
        last_date = latest_df['date'].max()
        
        # Start from one minute after the last stored data
        start_time = last_date + timedelta(minutes=1)
    else:
        # If no previous data, get data from a day ago
        start_time = datetime.now() - timedelta(days=1)
    
    # Format as required by API
    start_str = start_time.strftime("%Y-%m-%d")
    end_str = datetime.now().strftime("%Y-%m-%d")
    
    # Fetch data from API
    try:
        url = f"https://financialmodelingprep.com/stable/historical-chart/1min?symbol={symbol}&apikey={api_key}&from={start_str}&to={end_str}"
        resp = requests.get(url, timeout=10)

        if resp.status_code != 200:
            logger.error(f"API request failed with status {resp.status_code}: {resp.text}")
            return pd.DataFrame()

        data = resp.json()

        if data and isinstance(data, list):
            df = pd.DataFrame(data)

            # Ensure DataFrame has data and contains the necessary columns
            if not df.empty and 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                
                # Filter to only include new data after our last stored time
                if not latest_df.empty:
                    df = df[df['date'] > last_date]
                
                df = df.sort_values("date")
                return df
            else:
                logger.warning(f"No valid data received for {symbol}")
                return pd.DataFrame()
        else:
            logger.warning(f"No data received for {symbol}")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed for {symbol}: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error processing data for {symbol}: {e}")
        return pd.DataFrame()


def minute_update(symbol: str, api_key: str = API_KEY) -> dict:
    """
    执行单个符号的分钟级更新

    Args:
        symbol: 加密货币符号
        api_key: API密钥

    Returns:
        更新结果的字典
    """
    try:
        logger.info(f"Starting minute update for {symbol}...")
        
        # Get the latest minute data
        new_data = get_minute_update(symbol, api_key)
        
        if not new_data.empty:
            # Save the new data to parquet files
            save_to_parquet_file(new_data, symbol)
            
            result = {
                "status": "success",
                "rows_inserted": len(new_data),
                "date_range": [str(new_data['date'].min()), str(new_data['date'].max())] 
                              if 'date' in new_data.columns else "N/A"
            }
            logger.info(f"Minute update for {symbol} completed: {len(new_data)} rows added")
        else:
            result = {
                "status": "success",
                "rows_inserted": 0,
                "message": "No new data to insert"
            }
            logger.info(f"Minute update for {symbol}: No new data")
            
        return result
        
    except Exception as e:
        result = {
            "status": "error",
            "error_message": str(e)
        }
        logger.error(f"Error in minute update for {symbol}: {e}")
        return result


def scheduled_update(symbols: List[str], 
                     lookback_days: int = 1, 
                     api_key: str = API_KEY,
                     db_table_prefix: str = "FMP",
                     write_to_database: bool = True) -> dict:
    """
    定时更新数据到数据库
    
    Args:
        symbols: 要更新的加密货币符号列表
        lookback_days: 回溯天数，默认为1天
        api_key: FMP API密钥
        db_table_prefix: 数据库表名前缀
        write_to_database: 是否写入数据库，默认为True
    
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
            
            # 计算开始和结束日期
            end_date = datetime.today().strftime("%Y-%m-%d")
            start_date = (datetime.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            
            # 获取数据
            data = get_1min_historical_data(symbol, start_date, end_date, api_key)
            
            if not data.empty:
                # 准备数据库表名
                table_name = f"{db_table_prefix}_{symbol}_Min1"
                
                if write_to_database:
                    # 写入数据库
                    write_to_db(data, table_name)
                    
                results[symbol] = {
                    "status": "success",
                    "rows_inserted": len(data),
                    "date_range": [str(data['date'].min()), str(data['date'].max())] if 'date' in data.columns else "N/A"
                }
                logger.info(f"Successfully updated {symbol}, {len(data)} rows inserted")
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


def get_jsonparsed_data(symbol: str, key: str, start: str, end: str) -> pd.DataFrame:
    """
    获取1小时历史数据的辅助函数（保留原有功能）
    """
    url = f"https://financialmodelingprep.com/stable/historical-chart/1hour?symbol={symbol}&apikey={key}&from={start}&to={end}"
    resp = requests.get(url, timeout=10)
    data = resp.json()
    return pd.DataFrame(data)


def fetch_full_data(symbol: str, key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    获取1小时历史数据（保留原有功能，但 with improved error handling）
    """
    all_data = []

    # 将字符串日期转 datetime 对象
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # API 每次最多返回约 2140 条数据，大约 2140 小时 = 89 天左右
    delta = timedelta(hours=2160)

    current_start = start_dt
    while current_start < end_dt:
        current_end = min(current_start + delta, end_dt)
        # 转为字符串
        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")
        print(f"Fetching hourly data from {start_str} to {end_str}...")

        try:
            df = get_jsonparsed_data(symbol, key, start_str, end_str).sort_values("date")
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Error fetching data for {symbol} from {start_str} to {end_str}: {e}")
        finally:
            current_start = current_end + timedelta(days=1)

    # 拼接所有数据
    if len(all_data) > 0:
        full_df = pd.concat(all_data, ignore_index=True)
    else:
        full_df = pd.DataFrame()
    return full_df


if __name__ == '__main__':

    # history
    symbols_to_update = ["BTCUSD", "ETHUSD", "DOGEUSD", "SOLUSD", "BNBUSD", "XRPUSD"]
    for symbol in symbols_to_update:
        a = get_1min_historical_data(symbol, start_date="2022-10-12", end_date="2022-10-25")

    # update
    logger.info(f"Starting scheduled update for {len(symbols_to_update)} symbols...")

    # Perform the scheduled update (without writing to DB for testing)
    update_results = scheduled_update(symbols=symbols_to_update, lookback_days=1, write_to_database=False)

    # Print results
    for symbol, result in update_results.items():
        logger.info(f"{symbol}: {result}")
    
    # Demonstrate minute-level updates
    logger.info("Starting minute-level updates...")
    for symbol in symbols_to_update:  # Only test with first 2 symbols
        minute_result = minute_update(symbol)
        logger.info(f"Minute update for {symbol}: {minute_result}")