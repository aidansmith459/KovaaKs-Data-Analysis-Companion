"""
Kovaak's CSV Data Loader

This module provides functions to parse and load Kovaak's aim trainer CSV files.
Kovaak's CSV files contain 3 sections:
1. Variable-length kill/event table (can be empty)
2. Weapon information table (usually 1 row)
3. Vertical key-value pairs (stats)
"""

import pandas as pd
import os
import re
from collections import defaultdict


def parse_kovaaks_csv(filepath):
    """
    Parse a Kovaak's CSV file which contains 3 sections:
    - Variable-length kill/event table (can be empty)
    - Weapon information table (usually 1 row)
    - Vertical key-value pairs (stats)
    
    Args:
        filepath: Path to the CSV file
        
    Returns:
        tuple: (main_df, weapon_df, stats_dict)
            - main_df: DataFrame with kill/event data
            - weapon_df: DataFrame with weapon information
            - stats_dict: Dictionary with stats as key-value pairs
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # section boundaries -----------------------------
    # from what I am seeing, it is always:
    # Table 1 ends when we see "Weapon,"
    # Table 2 ends when we see a line starting with "Kills:"
    
    section1_end = None
    section2_end = None
    
    for i, line in enumerate(lines):
        if line.strip().startswith('Weapon,') and section1_end is None:
            section1_end = i
        if line.strip().startswith('Kills:,') and section2_end is None:
            section2_end = i
            break
    
    # PARSE FIRST TABLE: Main kill/event table
    if section1_end is not None and section1_end > 0:
        # Read the first table (may be empty so just header row)
        try:
            main_df = pd.read_csv(filepath, nrows=section1_end-1)
            # If empty, ensure we have the column structure
            if len(main_df) == 0:
                # Re-read with nrows=0 to get column names
                main_df = pd.read_csv(filepath, nrows=0)
        except Exception as e:
            try:
                main_df = pd.read_csv(filepath, nrows=0)
            except:
                main_df = pd.DataFrame()
    else:
        main_df = pd.DataFrame()
    
    # PARSE SECOND TABLE: Weapon information table
    if section1_end is not None and section2_end is not None:
        try:
            # Read weapon table (skip to section1_end, read until section2_end)
            weapon_df = pd.read_csv(filepath, skiprows=section1_end, nrows=section2_end-section1_end-1)
            # Remove completely empty rows
            weapon_df = weapon_df.dropna(how='all')
        except Exception as e:
            weapon_df = pd.DataFrame()
    else:
        weapon_df = pd.DataFrame()
    
    # PARSE THIRD TABLE: Vertical key-value pairs
    stats_dict = {}
    if section2_end is not None:
        for i in range(section2_end, len(lines)):
            line = lines[i].strip()
            if line and ':' in line:
                parts = line.split(':', 1)
                if len(parts) == 2:
                    key = parts[0].strip()
                    value = parts[1].strip()
                    # Clean trailing/leading commas that break num conversion
                    value = value.strip().lstrip(',').rstrip(',').strip()
                    # Try to convert to number if possible
                    if value:  # Only try conversion if value is not empty
                        try:
                            if '.' in value:
                                value = float(value)
                            else:
                                value = int(value)
                        except ValueError:
                            pass  # prolly want to keep this as string
                    stats_dict[key] = value
    
    return main_df, weapon_df, stats_dict


def load_all_kovaaks_data(csv_dir="./stats"):
    """
    Load all Kovaak's CSV files from a directory into nested dictionaries.
    
    Args:
        csv_dir: Directory containing CSV files (default: "./stats")
        
    Returns:
        tuple: (main_data, weapon_data, stats_data)
            - main_data: dict[task_name][datetime] -> DataFrame
            - weapon_data: dict[task_name][datetime] -> DataFrame
            - stats_data: dict[task_name][datetime] -> dict
    """
    # Initialize nested dictionaries for the three data types
    main_data = defaultdict(lambda: defaultdict(pd.DataFrame))  # Main kill/event data
    weapon_data = defaultdict(lambda: defaultdict(pd.DataFrame))  # Weapon information
    stats_data = defaultdict(lambda: defaultdict(dict))  # Stats as key-value pairs
    
    i = 0
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            # Parse filename: "task name - Challenge - YYYY.MM.DD-HH.MM.SS Stats.csv"
            match = re.match(r'(.*?)\s*-\s*Challenge\s*-\s*(\d{4}\.\d{2}\.\d{2}-\d{2}\.\d{2}\.\d{2})\s*Stats\.csv', filename)
            
            if match:
                task_type = match.group(1).strip()
                date = match.group(2)
                
                filepath = os.path.join(csv_dir, filename)
                try:
                    main_df, weapon_df, stats_dict = parse_kovaaks_csv(filepath)
                    
                    main_data[task_type][date] = main_df
                    weapon_data[task_type][date] = weapon_df
                    stats_data[task_type][date] = stats_dict
                    
                    i += 1
                    if i % 10 == 0:
                        print(f"Processed {i} files...")
                except Exception as e:
                    print(f"Error processing {filename}: {e}")
                    continue
    
    print(f"\nTotal files processed: {i}")
    print(f"Unique tasks: {len(main_data)}")
    
    return main_data, weapon_data, stats_data

