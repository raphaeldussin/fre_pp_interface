#!/usr/bin/env python

import xarray as xr
import fre_pp_interface as pp
import argparse
import subprocess as sp
import numpy as np

#-- simple argument parser
parser = argparse.ArgumentParser()
parser.add_argument(
            "-v",
            "--variable",
            type=str,
            required=True,
            help="variable name",
        )
parser.add_argument(
            "-s",
            "--stream",
            type=str,
            required=True,
            help="pp stream name (e.g. ocean_annual)",
        )

parser.add_argument(
            "-o",
            "--outdir",
            type=str,
            required=True,
            help="output pp directory",
        )

parser.add_argument(
            "-y",
            "--years",
            type=int,
            required=True,
            help="number of years in output files (e.g. 20)",
        )

args = parser.parse_args()

#- make it a dict for easy use
dictargs = vars(args)
stream = dictargs['stream']
var = dictargs['variable']
nyears_file = dictargs['years']
cyears = str(nyears_file)
outputdir = dictargs['outdir']

def create_pp_path(ppdir, stream):
    """ fill in the path to timeserie """
    if 'annual' in stream:
        pp_path = f'{ppdir}/{stream}/ts/annual'
    elif 'month' in stream:
        pp_path = f'{ppdir}/{stream}/ts/monthly'
    return pp_path


def create_out_path(outputdir, stream, cyears):
    if 'annual' in stream:
        out_path = f"{outputdir}/{stream}/ts/annual/{cyears}yr"
    elif 'month' in stream:
        out_path = f"{outputdir}/{stream}/ts/monthly/{cyears}yr"
    return out_path


# this needs not be changed
cycle1dir = '/archive/Alistair.Adcroft/xanadu_esm4_20190304_mom6_2019.07.21/OM4p25_JRA55do1.4_0netfw/gfdl.ncrc4-intel16-prod/pp'
cycle2dir = '/archive/Alistair.Adcroft/xanadu_esm4_20190304_mom6_2019.07.21/OM4p25_JRA55do1.4_0netfw_cycle2/gfdl.ncrc4-intel16-prod/pp'
cycle3dir = '/archive/Alistair.Adcroft/xanadu_esm4_20190304_mom6_2019.07.21/OM4p25_JRA55do1.4_0netfw_cycle3/gfdl.ncrc4-intel16-prod/pp'
cycle4dir = '/archive/Raphael.Dussin/xanadu_esm4_20190304_mom6_2019.08.08/OM4p25_JRA55do1.4_0netfw_cycle4/gfdl.ncrc4-intel16-prod/pp'
cycle5dir = '/archive/Raphael.Dussin/xanadu_esm4_20190304_mom6_2019.08.08/OM4p25_JRA55do1.4_0netfw_cycle5/gfdl.ncrc4-intel16-prod/pp'
cycle6dir = '/archive/Raphael.Dussin/xanadu_esm4_20190304_mom6_2019.08.08/OM4p25_JRA55do1.4_0netfw_cycle6/gfdl.ncrc4-intel16-prod/pp'

# figure out the list of files:

files_1 = pp.create_timeserie_monofield(create_pp_path(cycle1dir, stream), var)
files_2 = pp.create_timeserie_monofield(create_pp_path(cycle2dir, stream), var)
files_3 = pp.create_timeserie_monofield(create_pp_path(cycle3dir, stream), var)
files_4 = pp.create_timeserie_monofield(create_pp_path(cycle4dir, stream), var)
files_5 = pp.create_timeserie_monofield(create_pp_path(cycle5dir, stream), var)
files_6 = pp.create_timeserie_monofield(create_pp_path(cycle6dir, stream), var)


def create_dmget_string(files):
    """ create one long dmget command """
    dmget_cmd = "dmget -v "
    for f in files:
        dmget_cmd += f"{f} "
    return dmget_cmd


print(f"cycle 1: fetching {len(files_1)} files for {var} in {stream}..." )
sp.check_call(create_dmget_string(files_1), shell=True)
print("dmget done")

print(f"cycle 2: fetching {len(files_2)} files for {var} in {stream}..." )
sp.check_call(create_dmget_string(files_2), shell=True)
print("dmget done")

print(f"cycle 3: fetching {len(files_3)} files for {var} in {stream}..." )
sp.check_call(create_dmget_string(files_3), shell=True)
print("dmget done")

print(f"cycle 4: fetching {len(files_4)} files for {var} in {stream}..." )
sp.check_call(create_dmget_string(files_4), shell=True)
print("dmget done")

print(f"cycle 5: fetching {len(files_5)} files for {var} in {stream}..." )
sp.check_call(create_dmget_string(files_5), shell=True)
print("dmget done")

print(f"cycle 6: fetching {len(files_6)} files for {var} in {stream}..." )
sp.check_call(create_dmget_string(files_6), shell=True)
print("dmget done")

# open all files in a single dataset using time fixes from fre_pp_interface
ds =  pp.merge_cycles([files_1, files_2, files_3, files_4,files_5, files_6])

# now that we have the time axis concatenated, we can decode it in cftime
ds = xr.decode_cf(ds)

first_year = ds.isel(time=0)['time'].dt.year.values
last_year = ds.isel(time=-1)['time'].dt.year.values

dirout = create_out_path(outputdir, stream, cyears)
sp.check_call(f"mkdir -p {dirout}", shell=True)

# init while loop
keep_looping = True
start_year = first_year

while keep_looping:
    end_year = start_year + nyears_file -1

    print(f"extracting time period {start_year} - {end_year}")
    ds_split = ds.sel(time=slice(str(start_year), str(end_year)))

    # override end_year by last value found for year, only useful for last file
    end_year = int(ds_split.isel(time=-1)['time'].dt.year.values)
    fout = f"{stream}.{start_year}-{end_year}.{var}.nc"

    # write file
    print(f"writing into file {dirout}/{fout}")
    ds_split.to_netcdf(f"{dirout}/{fout}")

    # test for final segment:
    if end_year == last_year:
        keep_looping = False
    else:
       # prepare for next iteration
        start_year = end_year + 1
