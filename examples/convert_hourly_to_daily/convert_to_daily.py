#!/usr/bin/env python3
"""
Parallel conversion of hourly ERA5 data to daily min/max using Dask.
"""

import xarray as xr
from pathlib import Path
import time
import dask
from dask.diagnostics import ProgressBar
import multiprocessing


def convert_file_to_daily(input_file: Path, output_dir: Path) -> dict:
    """Convert a single hourly NetCDF file to daily min/max."""
    start = time.time()

    # Open the hourly dataset
    ds = xr.open_dataset(input_file)

    # Convert from Kelvin to Celsius
    ds['t2m'] = ds['t2m'] - 273.15

    # Resample to daily min/max
    daily_min = ds['t2m'].resample(valid_time='1D').min()
    daily_max = ds['t2m'].resample(valid_time='1D').max()

    # Create daily dataset
    daily_ds = xr.Dataset({
        't2m_min': daily_min,
        't2m_max': daily_max
    })

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

    # Save to output directory
    output_file = output_dir / f"daily_{input_file.stem}.nc"
    daily_ds.to_netcdf(
        output_file,
        encoding={
            't2m_min': {'zlib': True, 'complevel': 5, 'dtype': 'float32'},
            't2m_max': {'zlib': True, 'complevel': 5, 'dtype': 'float32'}
        }
    )

    # Clean up
    ds.close()
    daily_ds.close()

    elapsed = time.time() - start
    return {
        'file': input_file.name,
        'days': len(daily_min.valid_time),
        'time': elapsed
    }


def main():
    # Configuration
    input_dir = Path("examples/convert_hourly_to_daily/data/hourly")
    output_dir = Path("examples/convert_hourly_to_daily/data/daily")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Get first 100 NetCDF files for ~3000 days
    all_files = sorted(input_dir.glob("*.nc"))
    input_files = all_files[:100]

    # Determine number of workers
    n_workers = multiprocessing.cpu_count()

    print(f"Dask Parallel Processing - Large Dataset")
    print("=" * 80)
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Files to process: {len(input_files)}")
    print(f"Date range: {input_files[0].stem} to {input_files[-1].stem}")
    print(f"Number of workers: {n_workers}")
    print()

    # Create delayed tasks for each file
    delayed_tasks = [
        dask.delayed(convert_file_to_daily)(f, output_dir)
        for f in input_files
    ]

    # Process files in parallel with progress bar
    overall_start = time.time()

    print("Processing files...")
    with ProgressBar():
        with dask.config.set(scheduler='processes', num_workers=n_workers):
            results = dask.compute(*delayed_tasks)

    overall_time = time.time() - overall_start

    # Print summary
    print()
    print("=" * 80)
    print("Summary")
    print("-" * 80)
    print(f"Total files processed: {len(results)}")
    print(f"Total time: {overall_time:.2f}s ({overall_time/60:.2f} minutes)")
    print(f"Average time per file: {overall_time/len(results):.2f}s")
    print(f"Processing rate: {len(results)/overall_time:.2f} files/s")
    print(f"Total days converted: {sum(r['days'] for r in results)}")
    print(f"Daily throughput: {sum(r['days'] for r in results)/overall_time:.1f} days/s")
    print()


if __name__ == "__main__":
    main()
