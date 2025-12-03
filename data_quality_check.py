#!/usr/bin/env python
"""
Data Quality Check for FMP Crypto Data
This script checks the quality and integrity of FMP crypto data stored in parquet files.
"""
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_data_integrity(base_path: str = "D:\\data") -> Dict[str, any]:
    """
    Check the integrity of FMP crypto data
    
    Args:
        base_path: Base path where data is stored (default: "D:\\data")
    
    Returns:
        Dictionary containing results of data quality checks
    """
    results = {
        "summary": {},
        "issues": [],
        "data_gaps": [],
        "duplicate_dates": [],
        "data_stats": {}
    }
    
    # Path structure: D:\data\crypto\1min\{symbol}\{year}\data.parquet
    crypto_path = os.path.join(base_path, "crypto", "1min")
    
    if not os.path.exists(crypto_path):
        logger.error(f"Path does not exist: {crypto_path}")
        return results
    
    # Get all symbols
    symbols = [d for d in os.listdir(crypto_path) 
               if os.path.isdir(os.path.join(crypto_path, d))]
    
    logger.info(f"Found {len(symbols)} symbols to check: {symbols}")
    
    for symbol in symbols:
        symbol_path = os.path.join(crypto_path, symbol)
        years = [d for d in os.listdir(symbol_path) 
                 if os.path.isdir(os.path.join(symbol_path, d)) and d.isdigit()]
        
        logger.info(f"Checking symbol {symbol} with years: {years}")
        
        # Read all years for this symbol
        all_data = []
        for year in years:
            file_path = os.path.join(symbol_path, year, "data.parquet")
            if os.path.exists(file_path):
                try:
                    df = pd.read_parquet(file_path)
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'])
                        # Ensure date column is timezone-aware (in UTC)
                        if df['date'].dt.tz is None:
                            # If no timezone info, assume it's in UTC as per FMP updates
                            df['date'] = df['date'].dt.tz_localize('UTC')
                        all_data.append(df)
                        logger.info(f"Symbol {symbol}, Year {year}: {len(df)} records")
                        
                        # Check for data quality issues in this file
                        file_issues = check_single_file_quality(df, symbol, year, file_path)
                        results["issues"].extend(file_issues)
                    else:
                        logger.warning(f"File {file_path} does not contain 'date' column")
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {e}")
                    results["issues"].append({
                        "type": "file_error",
                        "symbol": symbol,
                        "year": year,
                        "file_path": file_path,
                        "error": str(e)
                    })
            else:
                logger.warning(f"File does not exist: {file_path}")
        
        if all_data:
            # Combine all years for this symbol
            full_df = pd.concat(all_data, ignore_index=True)
            full_df = full_df.sort_values(by='date').reset_index(drop=True)
            
            # Store data stats for this symbol
            results["data_stats"][symbol] = {
                "total_records": len(full_df),
                "date_range": {
                    "start": str(full_df['date'].min()) if 'date' in full_df.columns else "N/A",
                    "end": str(full_df['date'].max()) if 'date' in full_df.columns else "N/A"
                },
                "missing_dates_count": 0,
                "duplicate_dates_count": 0
            }
            
            # Check for gaps in data (only significant gaps of 5+ minutes)
            if 'date' in full_df.columns:
                gaps = find_data_gaps(full_df, symbol, min_gap_minutes=5)
                results["data_gaps"].extend(gaps)

                # Count missing dates
                results["data_stats"][symbol]["missing_dates_count"] = len(gaps)

                # Also identify periods with no data (e.g., entire days missing)
                daily_coverage = check_daily_coverage(full_df, symbol)
                results["data_gaps"].extend(daily_coverage)

                # Check for duplicate dates
                dupes = find_duplicate_dates(full_df, symbol)
                results["duplicate_dates"].extend(dupes)

                # Count duplicate dates
                results["data_stats"][symbol]["duplicate_dates_count"] = len(dupes)
    
    # Generate summary
    total_symbols = len(results["data_stats"])
    total_records = sum(stats["total_records"] for stats in results["data_stats"].values())
    total_gaps = len(results["data_gaps"])
    total_dupes = len(results["duplicate_dates"])
    
    results["summary"] = {
        "total_symbols": total_symbols,
        "total_records": total_records,
        "total_gaps": total_gaps,
        "total_duplicates": total_dupes,
        "issues_count": len(results["issues"])
    }
    
    return results


def check_single_file_quality(df: pd.DataFrame, symbol: str, year: str, file_path: str) -> List[Dict]:
    """Check quality of a single parquet file"""
    issues = []
    
    # Check for required columns
    required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        issues.append({
            "type": "missing_columns",
            "symbol": symbol,
            "year": year,
            "file_path": file_path,
            "missing_columns": missing_cols
        })
    
    # Check for missing values
    if 'date' in df.columns:
        missing_dates = df['date'].isna().sum()
        if missing_dates > 0:
            issues.append({
                "type": "missing_dates",
                "symbol": symbol,
                "year": year,
                "file_path": file_path,
                "count": int(missing_dates)
            })
    
    # Check for null values in other columns
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                issues.append({
                    "type": "null_values",
                    "symbol": symbol,
                    "year": year,
                    "file_path": file_path,
                    "column": col,
                    "count": int(null_count)
                })
    
    # Check for negative values in financial data
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                issues.append({
                    "type": "negative_values",
                    "symbol": symbol,
                    "year": year,
                    "file_path": file_path,
                    "column": col,
                    "count": int(negative_count)
                })
    
    # Check for unrealistic values (e.g., too high volume or prices)
    if 'volume' in df.columns:
        high_volume_thresh = df['volume'].quantile(0.999)  # Top 0.1% as potential outliers
        if pd.notna(high_volume_thresh):
            high_vol_count = (df['volume'] > high_volume_thresh * 10).sum()  # 10x the 99.9%ile
            if high_vol_count > 0:
                issues.append({
                    "type": "high_volume_outliers",
                    "symbol": symbol,
                    "year": year,
                    "file_path": file_path,
                    "count": int(high_vol_count)
                })
    
    return issues


def find_data_gaps(df: pd.DataFrame, symbol: str, min_gap_minutes: int = 5) -> List[Dict]:
    """Find gaps in the time series data"""
    gaps = []

    if 'date' not in df.columns:
        return gaps

    df_sorted = df.sort_values('date').reset_index(drop=True)

    # Check for 1-minute intervals
    df_sorted['next_date'] = df_sorted['date'].shift(-1)
    df_sorted['time_diff'] = df_sorted['next_date'] - df_sorted['date']

    # Find intervals greater than specified minutes (default 5 minutes to focus on significant gaps)
    gap_threshold = timedelta(minutes=min_gap_minutes)
    gap_rows = df_sorted[df_sorted['time_diff'] > gap_threshold]

    for _, row in gap_rows.iterrows():
        gaps.append({
            "symbol": symbol,
            "gap_start": str(row['date']),
            "gap_end": str(row['next_date']),
            "duration": str(row['time_diff'])
        })

    return gaps


def find_duplicate_dates(df: pd.DataFrame, symbol: str) -> List[Dict]:
    """Find duplicate dates in the data"""
    duplicates = []

    if 'date' not in df.columns:
        return duplicates

    # Find duplicate dates
    duplicate_mask = df['date'].duplicated(keep=False)
    duplicate_rows = df[duplicate_mask].sort_values('date')

    for date_val, group in duplicate_rows.groupby('date'):
        duplicates.append({
            "symbol": symbol,
            "date": str(date_val),
            "count": len(group),
            "duplicate_data": group.to_dict('records')  # Include the actual duplicate records for review
        })

    return duplicates


def check_daily_coverage(df: pd.DataFrame, symbol: str) -> List[Dict]:
    """Check for missing days in the data"""
    gaps = []

    if 'date' not in df.columns:
        return gaps

    # For timezone-aware dates, we need to handle the date extraction properly
    # Convert to UTC timezone-naive for date operations, then back if needed
    if df['date'].dt.tz is not None:
        date_col = df['date'].dt.tz_localize(None)  # Remove timezone info for date operations
    else:
        date_col = df['date']

    # Create a complete date range
    start_date = date_col.min().date()
    end_date = date_col.max().date()

    # Generate all dates between start and end
    full_date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    available_dates = date_col.dt.date.unique()

    # Find missing dates
    missing_dates = [date for date in full_date_range.date if date not in available_dates]

    # Group consecutive missing dates
    if missing_dates:
        consecutive_gaps = []
        current_gap_start = missing_dates[0]
        current_gap_end = missing_dates[0]

        for i in range(1, len(missing_dates)):
            # Check if the current date is consecutive to the previous one
            if missing_dates[i] == current_gap_end + timedelta(days=1):
                current_gap_end = missing_dates[i]
            else:
                # Gap ended, record it
                consecutive_gaps.append({
                    "symbol": symbol,
                    "gap_start": str(current_gap_start),
                    "gap_end": str(current_gap_end),
                    "duration": str(current_gap_end - current_gap_start + timedelta(days=1))
                })
                # Start a new gap
                current_gap_start = missing_dates[i]
                current_gap_end = missing_dates[i]

        # Record the last gap
        consecutive_gaps.append({
            "symbol": symbol,
            "gap_start": str(current_gap_start),
            "gap_end": str(current_gap_end),
            "duration": str(current_gap_end - current_gap_start + timedelta(days=1))
        })

        gaps.extend(consecutive_gaps)

    return gaps


def print_report(results: Dict):
    """Print a formatted report of the data quality check"""
    print("\n" + "="*60)
    print("FMP CRYPTO DATA QUALITY REPORT")
    print("="*60)
    
    # Summary
    print(f"\nSUMMARY:")
    print(f"  Total Symbols: {results['summary']['total_symbols']}")
    print(f"  Total Records: {results['summary']['total_records']:,}")
    print(f"  Data Gaps: {results['summary']['total_gaps']}")
    print(f"  Duplicate Dates: {results['summary']['total_duplicates']}")
    print(f"  Other Issues: {results['summary']['issues_count']}")
    
    # Detailed stats per symbol
    print(f"\nDETAILED STATS BY SYMBOL:")
    for symbol, stats in results['data_stats'].items():
        print(f"  {symbol}:")
        print(f"    Records: {stats['total_records']:,}")
        print(f"    Date Range: {stats['date_range']['start']} to {stats['date_range']['end']}")
        print(f"    Missing Dates: {stats['missing_dates_count']}")
        print(f"    Duplicate Dates: {stats['duplicate_dates_count']}")
    
    # Issues
    if results['issues']:
        print(f"\nDATA ISSUES FOUND:")
        for i, issue in enumerate(results['issues'], 1):
            print(f"  {i}. {issue['type']} - {issue['symbol']} ({issue['year']})")
            print(f"     File: {issue['file_path']}")
            if 'missing_columns' in issue:
                print(f"     Missing columns: {', '.join(issue['missing_columns'])}")
            elif 'count' in issue:
                print(f"     Count: {issue['count']}")
            elif 'error' in issue:
                print(f"     Error: {issue['error']}")
    
    # Data Gaps
    if results['data_gaps']:
        print(f"\nDATA GAPS FOUND:")
        for i, gap in enumerate(results['data_gaps'][:10], 1):  # Show first 10 gaps
            print(f"  {i}. {gap['symbol']}: {gap['gap_start']} to {gap['gap_end']} ({gap['duration']})")
        if len(results['data_gaps']) > 10:
            print(f"     ... and {len(results['data_gaps']) - 10} more gaps")
    
    # Duplicate Dates
    if results['duplicate_dates']:
        print(f"\nDUPLICATE DATES FOUND:")
        for i, dup in enumerate(results['duplicate_dates'][:10], 1):  # Show first 10 duplicates
            print(f"  {i}. {dup['symbol']}: {dup['date']} ({dup['count']} occurrences)")
        if len(results['duplicate_dates']) > 10:
            print(f"     ... and {len(results['duplicate_dates']) - 10} more duplicates")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    # Run the data quality check
    results = check_data_integrity()
    
    # Print the report
    print_report(results)
    
    # Optionally save results to a file
    import json
    output_file = "data_quality_report.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nDetailed report saved to {output_file}")