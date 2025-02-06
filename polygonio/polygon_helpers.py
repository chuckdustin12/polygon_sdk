#!/usr/bin/env python3
"""
Module: polygon_helpers.py
----------------
This module provides a collection of helper functions for data processing,
formatting, flattening nested dictionaries, timestamp conversions, XML parsing,
HTTP requests, plotting, and more.

Configuration & Dependencies:
    - Ensure your environment is configured with necessary API keys:
        * YOUR_POLYGON_KEY (used for API requests)
        * YOUR_DISCORD_HTTP_TOKEN (used for sending images to Discord)
    - Required Python packages:
        * pandas
        * numpy
        * requests
        * aiohttp
        * asyncpg
        * pytz
        * plotly, kaleido
        * bs4 (BeautifulSoup)
        * natsort
        * Pillow
    - Install via pip:
          pip install pandas numpy requests aiohttp asyncpg pytz plotly kaleido beautifulsoup4 natsort Pillow

Usage:
    Import the functions as needed:
        >>> from utils import flatten_dict, format_large_number, paginate_concurrent, convert_to_datetime, ...
    Many of these functions support asynchronous usage (see paginate_concurrent, fetch_url, etc.).
"""

import sys
from pathlib import Path
import os
import re
import io
import time
from datetime import datetime
from typing import Any, Dict, List, Union, Tuple
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
import pytz
import requests
import asyncio
import aiohttp
import asyncpg
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from colorsys import rgb_to_hsv
import matplotlib.pyplot as plt
from PIL import Image
import plotly.graph_objects as go
import uuid
import natsort
from kaleido.scopes.plotly import PlotlyScope

# Add the project directory to sys.path if not already present
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)

# Load API key from environment
YOUR_API_KEY = os.environ.get("YOUR_POLYGON_KEY")

# Headers for HTTP requests
headers_sec = {
    'User-Agent': 'Fudstop https://discord.gg/fudstop',
    'Content-Type': 'application/json'
}

# Mapping constants (make sure these exist in your mapping module)
from polygonio.mapping import (
    OPTIONS_EXCHANGES,
    option_condition_dict,
    STOCK_EXCHANGES,
    stock_condition_dict,
    TAPES
)

# ------------------ Data Formatting & Flattening ------------------ #
def flatten_nested_dict(row: pd.Series, column_name: str) -> pd.Series:
    """
    If a given column in a DataFrame row is a dictionary,
    flatten its key/value pairs into new columns and drop the original column.
    """
    if isinstance(row[column_name], dict):
        for key, value in row[column_name].items():
            row[key] = value
        row = row.drop(column_name)
    return row

def flatten_dict(d: Dict[Any, Any], parent_key: str = '', sep: str = '.') -> Dict[Any, Any]:
    """
    Recursively flatten a nested dictionary.
    
    Args:
        d: The dictionary to flatten.
        parent_key: The base key for nested items.
        sep: Separator between keys.
    
    Returns:
        A flat dictionary.
    """
    items = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_dict(v, new_key, sep=sep))
        else:
            items[new_key] = v
    return items

def flatten_list_of_dicts(lst: List[Union[Dict, List]]) -> List[Dict]:
    """Flatten each dictionary in a list."""
    return [flatten_dict(item) for item in lst]

def flatten_object(obj: Any, parent_key: str = '', separator: str = '_') -> Dict[str, Any]:
    """
    Flatten an object's __dict__ recursively.
    
    Args:
        obj: Object to flatten.
    
    Returns:
        Dictionary with flattened attributes.
    """
    items = {}
    for k, v in obj.__dict__.items():
        new_key = f"{parent_key}{separator}{k}" if parent_key else k
        if isinstance(v, dict):
            items.update(flatten_object(v, new_key, separator=separator))
        elif hasattr(v, '__dict__'):
            items.update(flatten_object(v, new_key, separator=separator))
        else:
            items[new_key] = v
    return items

# ------------------ Number Formatting ------------------ #
def format_large_number(num: Union[float, int, str, None]) -> str:
    """
    Format a large number to a human-readable string with suffixes.
    
    Args:
        num: The number to format.
    
    Returns:
        A string with appropriate suffix (K, M, B, T) or original number as string.
    """
    if isinstance(num, list):
        return [format_large_number(n) for n in num]
    elif isinstance(num, str):
        try:
            num = float(num)
        except ValueError:
            return num
    if num is None:
        return ""
    if abs(num) >= 1_000_000_000_000:
        return f"{num / 1_000_000_000_000:.2f} T"
    elif abs(num) >= 1_000_000_000:
        return f"{num / 1_000_000_000:.2f} B"
    elif abs(num) >= 1_000_000:
        return f"{num / 1_000_000:.2f} M"
    elif abs(num) >= 1_000:
        return f"{num / 1_000:.2f} K"
    else:
        return str(num)

def format_large_numbers_in_dataframe(df: pd.DataFrame, columns_to_format: List[str]) -> pd.DataFrame:
    """
    Format selected DataFrame columns to display large numbers in human-readable format.
    """
    formatted_df = df.copy()
    for column in columns_to_format:
        if column in formatted_df.columns:
            formatted_df[column] = formatted_df[column].apply(format_large_number)
    return formatted_df

def format_large_numbers_in_dict(data_dict: Dict[Any, Any], keys_to_format: List[str]) -> Dict[Any, Any]:
    """
    Format selected keys in a dictionary with large number formatting.
    """
    formatted_dict = data_dict.copy()
    for key in keys_to_format:
        if key in formatted_dict:
            formatted_dict[key] = format_large_number(formatted_dict[key])
    return formatted_dict

def format_value(value: Any) -> str:
    """Format a value to bold rounded float if available, else display **N/A**."""
    return f"**{round(float(value), 2)}**" if value is not None else "**N/A**"

# ------------------ Technical Analysis ------------------ #
def calculate_td9_series(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate the TD9 series and add it as a column in the DataFrame.
    """
    setup_count = 0
    td9_series = pd.Series(index=df.index, dtype='Int64')
    for i in range(4, len(df)):
        if df['Close'][i] > df['Close'][i - 4]:
            setup_count += 1
        else:
            setup_count = 0
        if setup_count >= 9:
            td9_series.at[df.index[i]] = setup_count
    df['TD9'] = td9_series
    return df

def calculate_setup(df: pd.DataFrame) -> bool:
    """
    Check if there is a TD9 Buy Setup in the DataFrame.
    """
    setup_count = 0
    for i in range(4, len(df)):
        if df['Close'][i] > df['Close'][i-4]:
            setup_count += 1
        else:
            setup_count = 0
        if setup_count >= 9:
            return True
    return False

def calculate_countdown(df: pd.DataFrame) -> bool:
    """
    Check if there is a TD Countdown (>=9) in the DataFrame.
    """
    countdown_count = 0
    for i in range(2, len(df)):
        if df['High'][i] > df['High'][i-2]:
            countdown_count += 1
        else:
            countdown_count = 0
        if countdown_count >= 9:
            return True
    return False

# ------------------ Discord & Image Functions ------------------ #
def send_to_discord(image_path: str, webhook_url: str) -> None:
    """
    Send an image file to a Discord webhook.
    """
    with open(image_path, 'rb') as f:
        files = {'file': ('image.png', f)}
        payload = {
            "embeds": [{
                "title": "TD9 Chart",
                "image": {"url": "attachment://image.png"}
            }]
        }
        headers = {
            'Content-Type': 'multipart/form-data',
            'Authorization': f"Bearer {os.environ.get('YOUR_DISCORD_HTTP_TOKEN')}"
        }
        r = requests.post(webhook_url, headers=headers, files=files, json=payload)
        if r.status_code == 204:
            print("Successfully sent image to Discord.")
        else:
            print(f"Failed to send image to Discord. Status Code: {r.status_code}")

def save_df_as_image(df: pd.DataFrame, image_path: str) -> None:
    """
    Save a DataFrame as an image file using matplotlib.
    """
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axis('tight')
    ax.axis('off')
    plt.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    plt.savefig(image_path)
    plt.close(fig)

# ------------------ Color & Image Processing ------------------ #
def describe_color(rgb_tuple: Tuple[int, int, int]) -> str:
    """
    Describe an RGB color tuple in human-readable terms.
    """
    red, green, blue = rgb_tuple
    red, green, blue = red / 255.0, green / 255.0, blue / 255.0
    hue, saturation, value = rgb_to_hsv(red, green, blue)
    hue_degree = hue * 360
    if saturation < 0.1 and value > 0.9:
        return "White"
    elif saturation < 0.2 and value < 0.2:
        return "Black"
    elif saturation < 0.2:
        return "Gray"
    elif 0 <= hue_degree < 12:
        return "Red"
    elif 12 <= hue_degree < 35:
        return "Orange"
    elif 35 <= hue_degree < 85:
        return "Yellow"
    elif 85 <= hue_degree < 170:
        return "Green"
    elif 170 <= hue_degree < 260:
        return "Blue"
    elif 260 <= hue_degree < 320:
        return "Purple"
    else:
        return "Red"

def autocrop_image(image: Any, border: int = 0) -> Any:
    """
    Crop empty space from a PIL image and add a border if specified.
    """
    bbox = image.getbbox()
    image = image.crop(bbox)
    (width, height) = image.size
    width += border * 2
    height += border * 2
    cropped_image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    cropped_image.paste(image, (border, border))
    return cropped_image

# ------------------ Unit Conversion ------------------ #
conversion_mapping = {"K": 1_000, "M": 1_000_000}
all_units = "|".join(conversion_mapping.keys())
float_re = natsort.numeric_regex_chooser(natsort.ns.FLOAT | natsort.ns.SIGNED)
unit_finder = re.compile(rf"({float_re})\s*({all_units})", re.IGNORECASE)

def fill_missing_values(data_dict: Dict[str, List[Any]]) -> None:
    """
    Ensure all lists in a dictionary have the same length by filling missing values with None.
    """
    max_length = max(len(v) for v in data_dict.values())
    for k, v in data_dict.items():
        if len(v) < max_length:
            data_dict[k] = v + [None] * (max_length - len(v))

# ------------------ Plotly Image Saving ------------------ #
scope = PlotlyScope()

def save_image(filename: str, fig: go.Figure = None, bytesIO: io.BytesIO = None) -> str:
    """
    Save a Plotly figure or BytesIO object as an image file.
    
    Args:
        filename: Base filename to save as.
        fig: Plotly figure to transform and save.
        bytesIO: BytesIO object containing image data.
    
    Returns:
        The filename with a UUID appended.
    
    Raises:
        Exception if neither fig nor bytesIO is provided.
    """
    imagefile = "image.jpg"
    if fig:
        fig = scope.transform(fig, scale=3, format="png")
        imgbytes = io.BytesIO(fig)
    elif bytesIO:
        imgbytes = bytesIO
    else:
        raise Exception("Function requires a go.Figure or io.BytesIO object")
    image = Image.open(imgbytes)
    image = autocrop_image(image, 0)
    imgbytes.seek(0)
    image.save(imagefile, "jpg", quality=100)
    image.close()
    return imagefile

# ------------------ Column Normalization ------------------ #
def normalize_columns(data: Union[Dict, pd.DataFrame], columns_to_normalize: List[str]) -> pd.DataFrame:
    """
    Normalize selected columns in a DataFrame by flattening nested dictionaries.
    """
    if isinstance(data, dict):
        df = pd.DataFrame.from_dict(data)
    else:
        df = data
    for column in columns_to_normalize:
        if column in df.columns:
            df = df.apply(lambda row: flatten_nested_dict(row, column), axis=1)
        else:
            print(f"Warning: Column '{column}' does not exist in the dataframe and will be skipped.")
    return df

# ------------------ Time & Timestamp Conversions ------------------ #
def convert_to_ns_datetime(unix_timestamp_str: str) -> datetime:
    """
    Convert a Unix timestamp string (assumed in milliseconds) to a datetime in Eastern Time.
    """
    unix_timestamp = int(unix_timestamp_str) / 1000.0
    dt_utc = datetime.utcfromtimestamp(unix_timestamp)
    dt_utc = pytz.utc.localize(dt_utc)
    dt_et = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    return dt_et.replace(tzinfo=None)

def convert_to_datetime(unix_timestamp_str: str) -> datetime:
    """
    Convert a Unix timestamp string (in seconds) to a datetime in Eastern Time.
    """
    unix_timestamp = int(unix_timestamp_str)
    dt_utc = datetime.utcfromtimestamp(unix_timestamp)
    dt_utc = pytz.utc.localize(dt_utc)
    dt_et = dt_utc.astimezone(pytz.timezone('US/Eastern'))
    return dt_et.replace(tzinfo=None)

def convert_to_datetime_or_str(input_str: str) -> datetime:
    """
    Convert a Unix timestamp (as string) or a date string to a datetime object.
    """
    try:
        unix_timestamp = int(input_str)
        dt_utc = datetime.utcfromtimestamp(unix_timestamp)
        dt_utc = pytz.utc.localize(dt_utc)
        dt_et = dt_utc.astimezone(pytz.timezone('US/Eastern'))
        return dt_et.replace(tzinfo=None)
    except ValueError:
        return datetime.strptime(input_str, '%B %d, %Y')

def convert_datetime_list(timestamps: List[Any], unit: str = 'ms') -> List[datetime]:
    """
    Convert a list of Unix timestamps to a list of datetime objects.
    
    Args:
        timestamps: List of timestamps.
        unit: Unit for conversion ('ms' for milliseconds by default).
    
    Returns:
        List of datetime objects.
    """
    dt_series = pd.Series(pd.to_datetime(timestamps, unit=unit, utc=True))
    dt_series = dt_series.dt.tz_localize(None)
    return dt_series.tolist()

def convert_to_et(timestamp: str) -> str:
    """
    Convert a UTC timestamp string to a human-readable Eastern Time (ET) string.
    """
    utc_time = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    et_timezone = pytz.timezone('US/Eastern')
    et_time = utc_time.astimezone(et_timezone)
    return et_time.strftime('%Y-%m-%d %H:%M:%S %Z')

def calculate_days_to_expiry(expiry_str: str, timestamp: datetime) -> int:
    """
    Calculate the number of days until expiry given an expiry string in MM/DD/YYYY format and a timestamp.
    """
    expiry = datetime.strptime(expiry_str, '%m/%d/%Y').date()
    return (expiry - timestamp.date()).days

def calculate_price_to_strike(price: float, strike: float) -> float:
    """
    Calculate the ratio of price to strike.
    """
    return price / strike if strike != 0 else 0

def convert_to_yymmdd(expiry_str: str) -> str:
    """
    Convert an expiry date from YYYY-MM-DD to YYMMDD format.
    """
    expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
    return expiry_date.strftime('%y%m%d')

# ------------------ XML Parsing Functions ------------------ #
def traverse_tree(element: ET.Element, unique_tags: set, unique_keys: set) -> None:
    """
    Recursively traverse an XML tree and collect unique tags and attribute keys.
    """
    unique_tags.add(element.tag)
    for key in element.keys():
        unique_keys.add(key)
    for child in element:
        traverse_tree(child, unique_tags, unique_keys)

def traverse_and_extract(element: ET.Element, target_tags: List[str], extracted_data: dict) -> None:
    """
    Traverse an XML tree and extract text for target tags.
    """
    if element.tag in target_tags:
        extracted_data[element.tag] = element.text
    for child in element:
        traverse_and_extract(child, target_tags, extracted_data)

def parse_element(element: ET.Element, parsed: dict = None) -> dict:
    """
    Recursively parse XML elements and return a nested dictionary.
    """
    if parsed is None:
        parsed = {}
    for child in element:
        if child.text is None or child.text.strip() == '':
            continue
        if child.tag in parsed:
            if not isinstance(parsed[child.tag], list):
                parsed[child.tag] = [parsed[child.tag]]
            parsed[child.tag].append(parse_element(child, {}))
        else:
            parsed[child.tag] = parse_element(child, {})
        if child.text.strip():
            parsed[child.tag]['value'] = child.text.strip()
    return parsed

def download_xml_file(url: str, file_path: str) -> None:
    """
    Download an XML file from a URL and save it locally.
    """
    response = requests.get(url, headers=headers_sec)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"File downloaded successfully at {file_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

def prepare_data_for_insertion(element: ET.Element, parsed: dict = None, current_table: str = None, current_record: dict = None) -> dict:
    """
    Recursively prepare XML data for database insertion by organizing it into table-specific records.
    """
    if parsed is None:
        parsed = {
            'ownershipDocument': [],
            'issuer': [],
            'reportingOwner': [],
            'reportingOwnerAddress': [],
            'nonDerivativeTransaction': [],
            'transactionAmounts': [],
            'postTransactionAmounts': [],
            'footnote': []
        }
    if current_table and current_record is not None:
        if element.text and element.text.strip():
            current_record[element.tag] = element.text.strip()
    if element.tag in parsed:
        current_table = element.tag
        new_record = {}
        parsed[current_table].append(new_record)
        current_record = new_record
    for child in element:
        prepare_data_for_insertion(child, parsed, current_table, current_record)
    return parsed

# ------------------ Safe Arithmetic ------------------ #
def safe_divide(a: float, b: float) -> Union[float, None]:
    """Return a / b, or None if division is not possible."""
    if a is None or b is None or b == 0:
        return None
    return a / b

def safe_subtract(a: float, b: float) -> Union[float, None]:
    """Return a - b, or None if inputs are invalid."""
    if a is None or b is None:
        return None
    return a - b

def safe_multiply(a: float, b: float) -> Union[float, None]:
    """Return a * b, or None if inputs are invalid."""
    if a is None or b is None:
        return None
    return a * b

def safe_max(a: float, b: float) -> Union[float, None]:
    """Return the maximum of a and b, or None if both are None."""
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return max(a, b)

# ------------------ Web Scraping ------------------ #
def get_first_image_url(webpage_url: str) -> Union[str, None]:
    """
    Download a webpage and return the URL of the first image found.
    """
    response = requests.get(webpage_url)
    if response.status_code != 200:
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    img_tag = soup.find('img')
    if img_tag is None:
        return None
    return img_tag.get('src')

# ------------------ DataFrame Parsing ------------------ #
def parse_to_dataframe(data: dict) -> pd.DataFrame:
    """
    Parse a JSON response (or list of dicts) to a Pandas DataFrame.
    
    If the data has a 'results' key, extract fields from each result.
    Otherwise, flatten the data.
    """
    results = data['results'] if 'results' in data else data
    parsed_data = []
    if results is not data:
        for result in results:
            parsed_data.append({
                "article_url": result.get("article_url"),
                "author": result.get("author"),
                "description": result.get("description"),
                "id": result.get("id"),
                "published_utc": result.get("published_utc"),
                "publisher_name": result.get("publisher", {}).get("name"),
                "tickers": ", ".join(result.get("tickers", [])),
                "title": result.get("title")
            })
        return pd.DataFrame(parsed_data)
    flattened_data = []
    for item in data:
        flat_item = {}
        def flatten_dict_inner(d: dict, parent_key: str = ''):
            for key, value in d.items():
                new_key = parent_key + '_' + key if parent_key else key
                if isinstance(value, dict):
                    flatten_dict_inner(value, new_key)
                else:
                    if isinstance(value, list):
                        value = str(value)
                    flat_item[new_key] = value
        flatten_dict_inner(item)
        flattened_data.append(flat_item)
    df = pd.DataFrame(flattened_data)
    print(df)
    return df

def get_first_index_from_dict(data_dict: dict) -> dict:
    """
    Given a dictionary with list values, return a new dictionary
    with each key mapped to the first element of its list.
    """
    first_index_data_dict = {}
    for key, value in data_dict.items():
        first_index_data_dict[key] = value[0] if value else None
    return first_index_data_dict

def calculate_percent_decrease(open_price: float, close_price: float) -> float:
    """
    Calculate the percentage decrease from open_price to close_price.
    """
    return ((open_price - close_price) / close_price) * 100

# ------------------ Async HTTP Functions ------------------ #
async def fetch_url(session: aiohttp.ClientSession, url: str) -> Any:
    """
    Fetch data from a URL asynchronously.
    
    Returns JSON data if status code is 200, else prints error and returns None.
    """
    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f"Error: {response.status}")
            return None

async def fetch_page(url: str) -> Any:
    """
    Asynchronously fetch a URL using aiohttp.
    
    Handles timeout and HTTP errors gracefully.
    """
    try:
        async with aiohttp.ClientSession() as session, session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except asyncio.TimeoutError:
        print(f"Timeout when accessing {url}")
    except aiohttp.ClientResponseError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

async def paginate_concurrent(url: str, as_dataframe: bool = False, concurrency: int = 25) -> Union[pd.DataFrame, List[Any]]:
    """
    Paginate through multiple API pages concurrently.
    
    Args:
        url: Starting URL (must include your apiKey).
        as_dataframe: Whether to return results as a DataFrame.
        concurrency: Maximum number of concurrent requests.
    
    Returns:
        A list of all results, or a DataFrame if as_dataframe is True.
    """
    all_results = []
    async with aiohttp.ClientSession() as session:
        pages_to_fetch = [url]
        while pages_to_fetch:
            tasks = []
            for _ in range(min(concurrency, len(pages_to_fetch))):
                next_url = pages_to_fetch.pop(0)
                tasks.append(fetch_page(next_url))
            results = await asyncio.gather(*tasks)
            if results is not None:
                for data in results:
                    if data is not None:
                        if "results" in data:
                            all_results.extend(data["results"])
                        next_url = data.get("next_url")
                        if next_url:
                            next_url += f'&{urlencode({"apiKey": YOUR_API_KEY})}'
                            pages_to_fetch.append(next_url)
                    else:
                        break
    return pd.DataFrame(all_results) if as_dataframe else all_results

async def paginate_tickers(url: str, as_dataframe: bool = False, concurrency: int = 5) -> Union[pd.DataFrame, List[Any]]:
    """
    Paginate through ticker API pages concurrently.
    
    Args:
        url: Starting URL.
        as_dataframe: Whether to return results as a DataFrame.
        concurrency: Maximum concurrent requests.
    
    Returns:
        A DataFrame or list of tickers.
    """
    all_results = []
    async with aiohttp.ClientSession() as session:
        pages_to_fetch = [url]
        while pages_to_fetch:
            tasks = []
            for _ in range(min(concurrency, len(pages_to_fetch))):
                next_url = pages_to_fetch.pop(0)
                tasks.append(fetch_page(next_url))
            results = await asyncio.gather(*tasks)
            if results is not None:
                for data in results:
                    if data is not None:
                        if "tickers" in data:
                            all_results.extend(data["tickers"])
                        next_url = data.get("next_url")
                        if next_url:
                            next_url += f'&{urlencode({"apiKey": YOUR_API_KEY})}'
                            pages_to_fetch.append(next_url)
    return pd.DataFrame(all_results) if as_dataframe else all_results

async def fetch_and_parse_data(url: str) -> List[Dict]:
    """
    Asynchronously fetch JSON data from a URL, flatten nested dictionaries, and return the list.
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
            results = data.get('results', [])
            flattened_results = [flatten_dict(result) for result in results]
            return flattened_results

# ------------------ Miscellaneous ------------------ #
def get_first_index_from_dict(data_dict: Dict[str, List[Any]]) -> Dict[str, Any]:
    """
    Return a dictionary mapping each key to the first element in its list.
    """
    return {k: v[0] if v else None for k, v in data_dict.items()}

def calculate_percent_decrease(open_price: float, close_price: float) -> float:
    """
    Calculate the percentage decrease from open_price to close_price.
    """
    return ((open_price - close_price) / close_price) * 100

# ------------------ Color & Image Functions ------------------ #
def describe_color(rgb_tuple: Tuple[int, int, int]) -> str:
    """
    Convert an RGB tuple to a human-readable color description.
    """
    red, green, blue = rgb_tuple
    red, green, blue = red / 255.0, green / 255.0, blue / 255.0
    hue, saturation, value = rgb_to_hsv(red, green, blue)
    hue_degree = hue * 360
    if saturation < 0.1 and value > 0.9:
        return "White"
    elif saturation < 0.2 and value < 0.2:
        return "Black"
    elif saturation < 0.2:
        return "Gray"
    elif 0 <= hue_degree < 12:
        return "Red"
    elif 12 <= hue_degree < 35:
        return "Orange"
    elif 35 <= hue_degree < 85:
        return "Yellow"
    elif 85 <= hue_degree < 170:
        return "Green"
    elif 170 <= hue_degree < 260:
        return "Blue"
    elif 260 <= hue_degree < 320:
        return "Purple"
    else:
        return "Red"

def save_df_as_image(df: pd.DataFrame, image_path: str) -> None:
    """
    Save a DataFrame as an image file using matplotlib.
    """
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axis('tight')
    ax.axis('off')
    plt.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    plt.savefig(image_path)
    plt.close(fig)

# ------------------ Additional Helper Functions ------------------ #
def get_first_index_from_dict(data_dict: Dict[str, List[Any]]) -> Dict[str, Any]:
    """
    Return a dictionary with each key mapped to the first element of its list.
    """
    return {k: v[0] if v else None for k, v in data_dict.items()}

def map_conditions(conditions: Union[List, Any]) -> Union[List, Any]:
    """
    Map option conditions using option_condition_dict.
    """
    if isinstance(conditions, list):
        return [option_condition_dict.get(cond, cond) for cond in conditions]
    return conditions

def map_stock_conditions(conditions: Union[List, Any]) -> Union[List, Any]:
    """
    Map stock conditions using stock_condition_dict.
    """
    if isinstance(conditions, list):
        return [stock_condition_dict.get(cond, cond) for cond in conditions]
    return conditions

def count_itm_otm(group: pd.DataFrame) -> pd.Series:
    """
    Count ITM and OTM options for calls and puts in a group.
    """
    underlying_price = group['underlying_price'].iloc[0]
    itm_call = len(group[(group['call_put'] == 'call') & (group['strike'] < underlying_price)])
    otm_call = len(group[(group['call_put'] == 'call') & (group['strike'] >= underlying_price)])
    itm_put = len(group[(group['call_put'] == 'put') & (group['strike'] > underlying_price)])
    otm_put = len(group[(group['call_put'] == 'put') & (group['strike'] <= underlying_price)])
    return pd.Series({
        'ITM_calls': itm_call,
        'OTM_calls': otm_call,
        'ITM_puts': itm_put,
        'OTM_puts': otm_put
    })

def calculate_candlestick(data: List[Dict[str, Any]], interval: str) -> Dict[str, Any]:
    """
    Calculate candlestick data from a list of price dictionaries.
    """
    open_price = data[0]['open_price']
    close_price = data[-1]['close_price']
    high_price = max(item['high_price'] for item in data)
    low_price = min(item['low_price'] for item in data)
    volume = sum(item['volume'] for item in data)
    return {
        'open_price': open_price,
        'close_price': close_price,
        'high_price': high_price,
        'low_price': low_price,
        'volume': volume
    }

def to_unix_timestamp_eastern(timestamp_ns: Any) -> int:
    """
    Convert a timestamp (in ns) to a Unix timestamp in Eastern Time.
    """
    timestamp_eastern = to_datetime_eastern(timestamp_ns)
    return int(timestamp_eastern.timestamp())

def to_datetime_eastern(timestamp_ns: Any) -> datetime:
    """
    Convert a timestamp (in ns) to a datetime object in Eastern Time.
    """
    timestamp_utc = pd.to_datetime(timestamp_ns, unit='ns').tz_localize('UTC')
    timestamp_eastern = timestamp_utc.tz_convert('US/Eastern')
    return timestamp_eastern

# ------------------ XML Utilities ------------------ #
def traverse_tree(element: ET.Element, unique_tags: set, unique_keys: set) -> None:
    """
    Traverse an XML tree to collect unique tags and keys.
    """
    unique_tags.add(element.tag)
    for key in element.keys():
        unique_keys.add(key)
    for child in element:
        traverse_tree(child, unique_tags, unique_keys)

def traverse_and_extract(element: ET.Element, target_tags: List[str], extracted_data: dict) -> None:
    """
    Traverse an XML tree and extract text for target tags.
    """
    if element.tag in target_tags:
        extracted_data[element.tag] = element.text
    for child in element:
        traverse_and_extract(child, target_tags, extracted_data)


def chunk_string(string, size):
    """Yield successive size-sized chunks from string."""
    for i in range(0, len(string), size):
        yield string[i:i + size]



def parse_element(element: ET.Element, parsed: dict = None) -> dict:
    """
    Recursively parse an XML element and return a nested dictionary.
    """
    if parsed is None:
        parsed = {}
    for child in element:
        if child.text is None or child.text.strip() == '':
            continue
        if child.tag in parsed:
            if not isinstance(parsed[child.tag], list):
                parsed[child.tag] = [parsed[child.tag]]
            parsed[child.tag].append(parse_element(child, {}))
        else:
            parsed[child.tag] = parse_element(child, {})
        if child.text.strip():
            parsed[child.tag]['value'] = child.text.strip()
    return parsed

def download_xml_file(url: str, file_path: str) -> None:
    """
    Download an XML file from a URL and save it locally.
    """
    response = requests.get(url, headers=headers_sec)
    if response.status_code == 200:
        with open(file_path, 'wb') as f:
            f.write(response.content)
        print(f"File downloaded successfully at {file_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

def prepare_data_for_insertion(element: ET.Element, parsed: dict = None, current_table: str = None, current_record: dict = None) -> dict:
    """
    Recursively prepare XML data for database insertion.
    """
    if parsed is None:
        parsed = {
            'ownershipDocument': [],
            'issuer': [],
            'reportingOwner': [],
            'reportingOwnerAddress': [],
            'nonDerivativeTransaction': [],
            'transactionAmounts': [],
            'postTransactionAmounts': [],
            'footnote': []
        }
    if current_table and current_record is not None:
        if element.text and element.text.strip():
            current_record[element.tag] = element.text.strip()
    if element.tag in parsed:
        current_table = element.tag
        new_record = {}
        parsed[current_table].append(new_record)
        current_record = new_record
    for child in element:
        prepare_data_for_insertion(child, parsed, current_table, current_record)
    return parsed

# ------------------ Miscellaneous Helper Functions ------------------ #
def remove_html_tags(text: str) -> str:
    """
    Remove HTML tags from a string.
    """
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def convert_str_to_datetime(date_time_str: str) -> datetime:
    """
    Convert a formatted date-time string to a datetime object.
    
    Expects a format like "12:34 PM ... Month Day, Year".
    """
    print(f"Debug: {date_time_str}")
    time_str, am_pm, _, _, month, day_with_comma, year = date_time_str.split()
    hour, minute = map(int, time_str.split(':'))
    if am_pm == 'PM' and hour != 12:
        hour += 12
    elif am_pm == 'AM' and hour == 12:
        hour = 0
    day = int(day_with_comma.replace(",", ""))
    dt_string = f"{year}-{month}-{day:02} {hour:02}:{minute:02}"
    dt_et = datetime.strptime(dt_string, '%Y-%B-%d %H:%M')
    eastern = pytz.timezone('US/Eastern')
    return eastern.localize(dt_et)

def map_months(month_str: str) -> str:
    """
    Map a full month name to its two-digit representation.
    """
    month_dict = {
        "January": "01", "February": "02", "March": "03",
        "April": "04", "May": "05", "June": "06",
        "July": "07", "August": "08", "September": "09",
        "October": "10", "November": "11", "December": "12"
    }
    return month_dict.get(month_str, "Invalid month")

def current_time_to_unix() -> int:
    """
    Return the current Unix timestamp.
    """
    now = datetime.now()
    return int(time.mktime(now.timetuple()))

def convert_timestamp_to_human_readable(url: str) -> str:
    """
    Extract and convert a Unix timestamp from a URL to a human-readable date.
    """
    try:
        timestamp_str = url.split("timestamp=")[-1]
        timestamp = int(timestamp_str)
    except (ValueError, IndexError):
        return "Invalid URL or timestamp"
    dt_object = datetime.fromtimestamp(timestamp)
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')

# ------------------ Key Parsing & Renaming ------------------ #
def get_human_readable_string(string: str) -> dict:
    """
    Extract details from an options symbol string in a specific format.
    """
    result = {}
    try:
        match = re.search(r'(\w{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)', string)
        underlying_symbol, year, month, day, call_put, strike_price = match.groups()
    except TypeError:
        underlying_symbol = "AMC"
        year = "23"
        month = "02"
        day = "17"
        call_put = "CALL"
        strike_price = "380000"
    expiry_date = '20' + year + '-' + month + '-' + day
    call_put = 'call' if call_put == 'C' else 'put'
    result['underlying_symbol'] = underlying_symbol
    result['strike_price'] = float(strike_price) / 1000
    result['call_put'] = call_put
    result['expiry_date'] = expiry_date
    return result

def human_readable(string: str) -> str:
    """
    Convert an options symbol string into a human-readable format.
    """
    try:
        match = re.search(r'(\w{1,5})(\d{2})(\d{2})(\d{2})([CP])(\d+)', string)
        underlying_symbol, year, month, day, call_put, strike_price = match.groups()
    except TypeError:
        underlying_symbol = "AMC"
        year = "23"
        month = "02"
        day = "17"
        call_put = "CALL"
        strike_price = "380000"
    expiry_date = f"{month}/{day}/20{year}"
    call_put = 'Call' if call_put == 'C' else 'Put'
    strike_price = '${:.2f}'.format(float(strike_price) / 1000)
    return f"{underlying_symbol} {strike_price} {call_put} Expiring {expiry_date}"

def rename_keys(original_dict: dict, key_mapping: dict) -> dict:
    """
    Rename keys in a dictionary based on a mapping.
    """
    return {key_mapping.get(k, k): v for k, v in original_dict.items()}

def shorten_form4_keys(data_dict: dict) -> dict:
    """
    Shorten keys in a Form 4 data dictionary by removing common prefixes.
    """
    shortened_dict = {}
    for key, value in data_dict.items():
        new_key = key
        new_key = new_key.replace('issuer_', '')
        new_key = new_key.replace('reportingOwner_', '')
        new_key = new_key.replace('reportingOwnerId_', '')
        new_key = new_key.replace('reportingOwnerAddress_', '')
        new_key = new_key.replace('reportingOwnerRelationship_', '')
        new_key = new_key.replace('nonDerivativeTable_', '')
        new_key = new_key.replace('nonDerivativeTransaction_', '')
        new_key = new_key.replace('transactionCoding_', '')
        new_key = new_key.replace('transactionAmounts_', '')
        new_key = new_key.replace('postTransactionAmounts_', '')
        new_key = new_key.replace('ownershipNature_', '')
        new_key = new_key.replace('securityTitle_', '')
        new_key = new_key.replace('transactionDate_', '')
        new_key = new_key.replace('transactionTimeliness_', '')
        new_key = new_key.replace('footnotes_', '')
        new_key = new_key.replace('ownerSignature_', '')
        shortened_dict[new_key] = value
    return shortened_dict

# ------------------ Plotly Figure Generation ------------------ #
def save_image(filename: str, fig: go.Figure = None, bytesIO: io.BytesIO = None) -> str:
    """
    Save a Plotly figure or BytesIO object as an image with a unique filename.
    
    Args:
        filename: Base filename.
        fig: Plotly figure to be saved.
        bytesIO: io.BytesIO object containing image data.
    
    Returns:
        Filename with UUID appended.
    
    Raises:
        Exception if neither fig nor bytesIO is provided.
    """
    imagefile = "image.jpg"
    if fig:
        fig = scope.transform(fig, scale=3, format="png")
        imgbytes = io.BytesIO(fig)
    elif bytesIO:
        imgbytes = bytesIO
    else:
        raise Exception("Function requires a go.Figure or io.BytesIO object")
    image = Image.open(imgbytes)
    image = autocrop_image(image, 0)
    imgbytes.seek(0)
    image.save(imagefile, "jpg", quality=100)
    image.close()
    return imagefile

# ------------------ End of Module ------------------ #

if __name__ == '__main__':
    # This main guard can be used for testing purposes.
    print("Running utils module tests...")
    
    # Test flatten_dict function
    sample_dict = {"a": {"b": 1, "c": {"d": 2}}, "e": 3}
    print("Flattened dict:", flatten_dict(sample_dict))
    
    # Test human_readable conversion
    sample_option = "AAPL230217C150000"
    print("Human readable:", human_readable(sample_option))
    
    # Test convert_to_datetime function
    unix_time = "1700000000"
    print("Converted datetime:", convert_to_datetime(unix_time))
    
    # Test paginate_concurrent (you can replace the URL with an actual endpoint)
    test_url = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey=" + (YOUR_API_KEY or "")
    # asyncio.run(paginate_concurrent(test_url, as_dataframe=True))
    
    print("All tests executed.")
