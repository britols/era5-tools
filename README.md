# More efficient ERA5 downloads + some extra useful tools

This code is heavily inspired by [weather-dl](https://github.com/google/weather-tools). 

## Installation

I recommend using [uv](https://docs.astral.sh/uv/) to manage the python enviroment and the project dependencies. With `uv`, you can set up the python enviroment for this project with a single command:

``` shell
uv sync
```

### Set up API key

- Create a `API_KEY` file in the repository root 
- Retrieve your CDS API key from https://cds.climate.copernicus.eu/how-to-api 
- Insert your API key information as the example below
``` shell
[key.a]
api_url=https://cds.climate.copernicus.eu/api
api_key=YOUR-KEY
api_email=YOUR-EMAIL
```
- Download parallelization is achieved by inserting additional API keys.

### Downloading ERA5 data

`era5_downloader.py` handles the downloads using a `.cfg` file. The `examples` folder has some examples of how to set up a `.cfg` (more ideas available at https://github.com/google-research/arco-era5/tree/main/raw). 

How to run:

```
.venv/bin/python era5_downloader.py examples/download_hourly_data_by_day/era5_hourly.cfg API_KEY 
```

### 2. Misc

Sometimes it's desirable to work with daily data instead. Check `examples/convert_hourly_to_daily` example, and adapt the `examples/convert_hourly_to_daily/convert_to_daily.py` according to your needs.

To run: 

```
.venv/bin/python examples/convert_hourly_to_daily/convert_to_daily.py
```

