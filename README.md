# Polygon Options Integration

![License: MIT](https://img.shields.io/badge/License-MIT-green)
![Python Version](https://img.shields.io/badge/Python-3.8%2B-blue)

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture & Design](#architecture--design)
- [Requirements](#requirements)
- [Installation](#installation)
  - [Clone the Repository](#clone-the-repository)
  - [Setup Virtual Environment](#setup-virtual-environment)
  - [Install Dependencies](#install-dependencies)
- [Configuration](#configuration)
  - [Environment Variables](#environment-variables)
  - [Database Setup](#database-setup)
- [Usage](#usage)
  - [Basic Usage](#basic-usage)
  - [Examples](#examples)
  - [Asynchronous Execution](#asynchronous-execution)
- [Advanced Topics](#advanced-topics)
  - [Batch Insert/Upsert](#batch-insertupsert)
  - [Pagination & Concurrency](#pagination--concurrency)
  - [Utility Functions](#utility-functions)
  - [Theoretical Pricing with Binomial Model](#theoretical-pricing-with-binomial-model)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)
- [References](#references)

## Overview

The **Polygon Options Integration** project is a robust, production‑ready Python module designed for integrating with the [Polygon.io](https://polygon.io/) API. It is built to fetch, process, and store options data in a PostgreSQL database using asynchronous programming patterns. The module leverages modern libraries such as `asyncio`, `aiohttp`, `httpx`, and `asyncpg` for efficient API calls and database operations, and it includes a comprehensive set of helper functions for data transformation, flattening of nested structures, timestamp conversion, and technical analysis.

This project is ideal for developers and quantitative analysts looking to build scalable market data pipelines, perform options analysis, and explore advanced trading strategies using a wide variety of market data.

## Features

- **Asynchronous API Requests:**  
  Uses `aiohttp` and `httpx` for non-blocking HTTP requests to the Polygon.io API.
  
- **Database Integration:**  
  Uses `asyncpg` to connect to a PostgreSQL database, with batch insert and upsert capabilities to handle large datasets efficiently.
  
- **Automatic Pagination:**  
  Handles API pagination automatically when a `next_url` is returned from endpoints.
  
- **Comprehensive Data Utilities:**  
  Provides a wealth of helper functions to:
  - Flatten nested dictionaries and XML structures.
  - Format large numbers into human-readable strings (e.g., converting 1,000,000 to 1.00 M).
  - Convert timestamps (nanoseconds, milliseconds) to Eastern Time and human-readable dates.
  - Remove HTML tags from text.
  
- **Technical Analysis:**  
  Implements technical indicators such as RSI along with a binomial model for theoretical option pricing.
  
- **Concurrent Data Fetching:**  
  Uses semaphores and timeouts to safely and efficiently manage multiple concurrent API requests.
  
- **History Tracking:**  
  Optionally creates history tables and triggers to log changes to main tables.

## Architecture & Design

The project is structured around a single core class, `PolygonOptions`, which encapsulates all functionality related to API communication, data processing, and database interactions. The class is designed with asynchronous patterns throughout, ensuring high performance and scalability when processing large volumes of data. Additionally, a series of helper functions are provided for:
  
- Data formatting and type conversion.
- Flattening nested data structures.
- XML parsing and extraction.
- Plotting and image processing (for reporting purposes).

## Requirements

- **Python 3.8 or higher**
- **PostgreSQL** (Ensure it is installed and running with the correct credentials)
- Required Python packages:
  - `aiohttp`
  - `asyncpg`
  - `pandas`
  - `numpy`
  - `pytz`
  - `httpx`
  - `plotly`
  - `kaleido`
  - `beautifulsoup4`
  - `more_itertools`
  - `python-dotenv`
  - `tabulate`

## Installation

### Clone the Repository

```bash
git clone [https://github.com/<your_username>/<your_repository>.git](https://github.com/chuckdustin12/polygon_sdk)
```
