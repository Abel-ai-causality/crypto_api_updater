#!/usr/bin/env python
"""
Financial Modeling Prep (FMP) API Wrapper
Provides functions for retrieving 1-minute cryptocurrency data and scheduled updates
"""
from datetime import datetime, timedelta
import pandas as pd
import requests
import time
import logging
from typing import List, Optional

API_KEY = "Fg717Owfnfye5r2HQVIhDuBTefMJs7uA"

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_1min_historical_data(symbol: str, start_date: str, end_date: str, 
                             api_key: str = API_KEY) -> pd.DataFrame:
    """
    获取指定时间段内的1分钟历史数据
    
    Args:
        symbol: 加密货币符号 (e.g. "BTCUSD")
        start_date: 开始日期 (格式: "YYYY-MM-DD")
        end_date: 结束日期 (格式: "YYYY-MM-DD")
        api_key: FMP API密钥
    
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
        return full_df
    else:
        return pd.DataFrame()


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
    # Example usage of the new functions
    # Load symbols to update (with fallback if files not available)
    get_1min_historical_data("BTCUSD", start_date="2025-10-01", end_date="2025-10-31")

    try:
        all_coins = pd.read_csv("all_data.csv")
        fmp_list = pd.read_csv("symbol_list.csv")
        fmp_list["exchg_symbol"] = fmp_list["symbol"].str.replace("USD", "", case=False).str.lower()
        filtered_symbols = fmp_list.loc[fmp_list["exchg_symbol"].isin(all_coins["symbol"])]

        # Get the symbols to update
        symbols_to_update = filtered_symbols["symbol"].tolist()[:5]  # Only first 5 for testing
    except FileNotFoundError:
        # Fallback to a simple test case
        symbols_to_update = ["BTCUSD"]  # Default to Bitcoin for testing
        print("CSV files not found, using BTCUSD as default for testing")

    logger.info(f"Starting scheduled update for {len(symbols_to_update)} symbols...")

    # Perform the scheduled update (without writing to DB for testing)
    update_results = scheduled_update(symbols=symbols_to_update, lookback_days=1, write_to_database=False)

    # Print results
    for symbol, result in update_results.items():
        logger.info(f"{symbol}: {result}")