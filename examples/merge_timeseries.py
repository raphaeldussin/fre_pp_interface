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

parser.add_argument(
            "-f",
            "--freq",
            type=str,
            required=False,
            help="override for frequency",
        )

args = parser.parse_args()

#- make it a dict for easy use
dictargs = vars(args)
stream = dictargs['stream']
var = dictargs['variable']
nyears_file = dictargs['years']
cyears = str(nyears_file)
outputdir = dictargs['outdir']
freq = dictargs['freq']


def create_pp_path(ppdir, stream, freq=None):
    """ fill in the path to timeserie """
    # default freq is monthly, override if find other freq in stream name
    dfreq='monthly'
    if 'annual' in stream:
        dfreq='annual'
    elif 'daily' in stream:
        dfreq='daily'
    if freq is not None:
        dfreq=freq
    pp_path = f'{ppdir}/{stream}/ts/{dfreq}'
    return pp_path


def create_out_path(outputdir, stream, cyears, freq=None):
    # default freq is monthly, override if find other freq in stream name
    dfreq='monthly'
    if 'annual' in stream:
        dfreq='annual'
    elif 'daily' in stream:
        dfreq='daily'
    if freq is not None:
        dfreq=freq
    out_path = f"{outputdir}/{stream}/ts/{dfreq}/{cyears}yr"
    return out_path


# this needs not be changed
cycle1dir = '/archive/Alistair.Adcroft/xanadu_esm4_20190304_mom6_2019.07.21/OM4p25_JRA55do1.4_0netfw/gfdl.ncrc4-intel16-prod/pp'
cycle2dir = '/archive/Alistair.Adcroft/xanadu_esm4_20190304_mom6_2019.07.21/OM4p25_JRA55do1.4_0netfw_cycle2/gfdl.ncrc4-intel16-prod/pp'
cycle3dir = '/archive/Alistair.Adcroft/xanadu_esm4_20190304_mom6_2019.07.21/OM4p25_JRA55do1.4_0netfw_cycle3/gfdl.ncrc4-intel16-prod/pp'
cycle4dir = '/archive/Raphael.Dussin/xanadu_esm4_20190304_mom6_2019.08.08/OM4p25_JRA55do1.4_0netfw_cycle4/gfdl.ncrc4-intel16-prod/pp'
cycle5dir = '/archive/Raphael.Dussin/xanadu_esm4_20190304_mom6_2019.08.08/OM4p25_JRA55do1.4_0netfw_cycle5/gfdl.ncrc4-intel16-prod/pp'
cycle6dir = '/archive/Raphael.Dussin/xanadu_esm4_20190304_mom6_2019.08.08/OM4p25_JRA55do1.4_0netfw_cycle6/gfdl.ncrc4-intel16-prod/pp'

# figure out the list of files:

files_1 = pp.create_timeserie_monofield(create_pp_path(cycle1dir, stream, freq), var)
files_2 = pp.create_timeserie_monofield(create_pp_path(cycle2dir, stream, freq), var)
files_3 = pp.create_timeserie_monofield(create_pp_path(cycle3dir, stream, freq), var)
files_4 = pp.create_timeserie_monofield(create_pp_path(cycle4dir, stream, freq), var)
files_5 = pp.create_timeserie_monofield(create_pp_path(cycle5dir, stream, freq), var)
files_6 = pp.create_timeserie_monofield(create_pp_path(cycle6dir, stream, freq), var)


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
ds_raw =  pp.merge_cycles([files_1, files_2, files_3, files_4,files_5, files_6], gaps=[1,1,1,0,0])

# now that we have the time axis concatenated, we can decode it in cftime
ds = xr.decode_cf(ds_raw, drop_variables=["time_bnds"])
units = ds_raw["time"].attrs["units"]
calendar = ds_raw["time"].attrs["calendar"]

first_year = ds.isel(time=0)['time'].dt.year.values
last_year = ds.isel(time=-1)['time'].dt.year.values

dirout = create_out_path(outputdir, stream, cyears)
sp.check_call(f"mkdir -p {dirout}", shell=True)

# number of years in each cycle
nyears_cycles = np.array([60, 60, 60, 61, 61, 61])
prev_gaps = np.array([0, 1, 1, 1, 0, 0])

# find out how many years are left to add in the last file
extra_years = np.mod(nyears_cycles, nyears_file)
# this is how many files to create for this cycle, last one will include extra years
files_per_cycles = (nyears_cycles - extra_years) / nyears_file

# build start and end dates for each file
startyears = []
endyears = []

beginyear = first_year

for cycle in range(6):
    #print(f"cycle {cycle} will write {int(files_per_cycles[cycle])} files")
    for nf in range(int(files_per_cycles[cycle])):
        startyear = beginyear
        if nf == 0:  # first file account for gap year, if any
            startyear += prev_gaps[cycle]
        endyear = startyear + nyears_file -1
        if nf == files_per_cycles[cycle] -1:  # last file has extra years, if any
            #print(f"add {extra_years[cycle]} to file {nf+1}")
            endyear += extra_years[cycle]
        #print(f"{startyear}-{endyear}")
        startyears.append(startyear)
        endyears.append(endyear)
        # reset beginyear
        beginyear = endyear + 1

# loop over start and end dates for each file
for start_year, end_year in zip(startyears, endyears):

    print(f"extracting time period {start_year} - {end_year}")
    ds_split = ds.sel(time=slice(str(start_year), str(end_year)))

    # fix _FillValue
    for kvar in list(ds_split.coords):
        ds_split[kvar].encoding.update({"_FillValue": None})
    for kvar in ["average_T1", "average_T2"]:
        ds_split[kvar].encoding.update({"_FillValue": 1.e+20})
        ds_split[kvar].attrs.update({"missing_value": 1.e+20})

    # recreate time_bnds due to weird encoding problem
    ds_split["time_bnds"] = xr.concat([ds_split["average_T1"], ds_split["average_T2"]], dim="nv")
    ds_split["time_bnds"] = ds_split["time_bnds"].transpose(*('time', 'nv'))
    ds_split["time_bnds"].attrs.update({"long_name": "time axis boundaries"})
    ds_split["time_bnds"].encoding.update({"units": units})
    ds_split["time_bnds"].encoding.update({"calendar": calendar})

    # override end_year by last value found for year, only useful for last file
    end_year = int(ds_split.isel(time=-1)['time'].dt.year.values)
    fout = f"{stream}.{start_year}-{end_year}.{var}.nc"

    # write file
    print(f"writing into file {dirout}/{fout}")
    ds_split.attrs["filename"] = f"{fout}"
    ds_split.to_netcdf(f"{dirout}/{fout}")
