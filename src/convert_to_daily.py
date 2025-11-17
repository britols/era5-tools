#!/usr/bin/env python3
"""
Convert hourly ERA5 temperature data to daily min/max.
Reads monthly NetCDF files, converts to daily, and saves as single file.
"""
import xarray as xr
from pathlib import Path
import tempfile
import shutil

def main():
    # Input and output paths
    input_dir = Path("raw/monthly/2m_temperature")
    output_dir = Path("raw/daily/2m_temperature")
    output_file = output_dir / "t2m_daily_1940_2025.nc"

    # Create temp directory for intermediate files
    temp_dir = Path(tempfile.mkdtemp(prefix="era5_daily_"))
    print(f"Using temp directory: {temp_dir}")

    # Get all monthly NetCDF files, sorted
    input_files = sorted(input_dir.glob("*.nc"))
    print(f"Found {len(input_files)} monthly files to process")

    # Process files in batches and save to temp files
    print("\nProcessing files in yearly batches...")
    temp_files = []

    for i in range(0, len(input_files), 12):
        batch = input_files[i:i+12]
        year = batch[0].stem[:4]  # Extract year from filename
        print(f"\n  Processing year {year} ({len(batch)} files)...")

        # Load this batch of monthly files
        batch_datasets = []
        for file in batch:
            ds = xr.open_dataset(file, engine='netcdf4')
            batch_datasets.append(ds)

        # Concatenate this year's months
        year_ds = xr.concat(batch_datasets, dim='valid_time')

        # Convert from Kelvin to Celsius
        year_ds['t2m'] = year_ds['t2m'] - 273.15

        # Resample to daily and calculate min/max
        daily_min = year_ds['t2m'].resample(valid_time='1D').min()
        daily_max = year_ds['t2m'].resample(valid_time='1D').max()

        # Create dataset for this year
        year_daily = xr.Dataset({
            't2m_min': daily_min,
            't2m_max': daily_max
        })

        # Save to temp file
        temp_file = temp_dir / f"daily_{year}.nc"
        year_daily.to_netcdf(temp_file)
        temp_files.append(temp_file)
        print(f"    Processed {len(year_daily.valid_time)} days -> {temp_file.name}")

        # Clean up
        for ds in batch_datasets:
            ds.close()
        year_ds.close()
        year_daily.close()

    # Now use open_mfdataset to efficiently combine all temp files
    print("\nCombining all years from temp files...")
    daily_ds = xr.open_mfdataset(
        sorted(temp_files),
        combine='by_coords',
        engine='netcdf4'
    )

    print(f"\nTotal daily timesteps: {len(daily_ds.valid_time)}")
    print(f"Date range: {daily_ds.valid_time.min().values} to {daily_ds.valid_time.max().values}")

    # Add metadata
    daily_ds['t2m_min'].attrs = {
        'long_name': 'Daily minimum 2 metre temperature',
        'units': 'degrees Celsius',
        'standard_name': 'air_temperature'
    }
    daily_ds['t2m_max'].attrs = {
        'long_name': 'Daily maximum 2 metre temperature',
        'units': 'degrees Celsius',
        'standard_name': 'air_temperature'
    }

    # Add global attributes
    daily_ds.attrs = {
        'title': 'Daily min/max 2m temperature from ERA5',
        'source': 'ERA5 reanalysis',
        'institution': 'European Centre for Medium-Range Weather Forecasts',
        'history': 'Converted from hourly to daily using xarray resample',
        'conversion': 'Hourly data resampled to daily min/max, converted from K to °C'
    }

    # Save to final NetCDF
    print(f"\nSaving to {output_file}...")
    daily_ds.to_netcdf(
        output_file,
        encoding={
            't2m_min': {'zlib': True, 'complevel': 5, 'dtype': 'float32'},
            't2m_max': {'zlib': True, 'complevel': 5, 'dtype': 'float32'}
        }
    )

    daily_ds.close()

    print(f"\nSuccess! Daily file saved to: {output_file}")
    print(f"File size: {output_file.stat().st_size / (1024**3):.2f} GB")

    # Clean up temp directory
    print(f"\nCleaning up temp directory...")
    shutil.rmtree(temp_dir)
    print("Done!")

if __name__ == "__main__":
    main()
