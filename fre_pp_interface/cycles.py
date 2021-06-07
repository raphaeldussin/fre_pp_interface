import xarray as xr
import numpy as np
import datetime as dt
import cftime


def merge_2_cycles(files_cycle1, files_cycle2, combine='by_coords'):
    """merge files from 2 cycles into one single dataset:
    deal with repeating time axis in 2 cycles"""
    if combine == 'nested':
        kwargs = {'concat_dim': 'time'}
    else:
        kwargs = {}
    ds1 = xr.open_mfdataset(files_cycle1, combine=combine,
                            decode_times=False, **kwargs)
    ds2 = xr.open_mfdataset(files_cycle2, combine=combine,
                            decode_times=False, **kwargs)
    ds = merge_2_datasets(ds1, ds2)
    return ds


def merge_2_datasets(ds1, ds2, calendar='leap'):
    """ merge 2 datasets into one long """
    # time is given in days since origin
    ndays_cycle1 = ds1['time'][-1].values - ds1['time'][0].values
    ndays_cycle2 = ds2['time'][-1].values - ds2['time'][0].values
    # we always come short of the last year
    if calendar == 'leap':
        ndpy=365.25
    elif calendar == 'noleap':
        ndpy=365.

    nyears_cycle1 = np.floor(ndays_cycle1/ndpy + 1)
    nyears_cycle2 = np.floor(ndays_cycle2/ndpy + 1)

    #print(nyears_cycle1, nyears_cycle2)
    units_cycle1 = ds1['time'].attrs['units']
    origin_cycle1 = dt.datetime.strptime(units_cycle1,
                                         "days since %Y-%m-%d %H:%M:%S")
    calendar_cycle1 = ds1['time'].attrs['calendar']

    units_cycle2 = ds2['time'].attrs['units']
    origin_cycle2 = dt.datetime.strptime(units_cycle2,
                                         "days since %Y-%m-%d %H:%M:%S")

    # set the new origin the past
    #origin = origin_cycle1 - dt.timedelta(days=ndpy*(nyears_cycle2))
    #origin = origin_cycle1 - dt.timedelta(days=ndays_cycle2+ndpy)
    offset_years = origin_cycle1.year + nyears_cycle1 - origin_cycle2.year
    #origin = origin_cycle1 - dt.timedelta(days=(ndpy*offset_years)+1)
    new_year_start = int(origin_cycle1.year - offset_years)
    origin =dt.datetime(new_year_start,1,1,0,0,0)
    units = dt.datetime.strftime(origin, "days since %Y-%m-%d %H:%M:%S")

    # set up the merged time axis
    # cycle1 values remains the same but the origin is shifted
    ds1['time'] = cftime.num2date(ds1['time'].values, units,
                                  calendar=calendar_cycle1)
    # cycle2 uses the same updated origin but also needs to have its values
    # shifted nyears_cycle1 in the future
    #ds2['time'] = cftime.num2date(ds2['time'].values + ndays_cycle1 + ndpy,
    ds2['time'] = cftime.num2date(ds2['time'].values + (nyears_cycle1)*ndpy,
                                  units, calendar=calendar_cycle1)
    # we can now concatenate the datasets
    #print('concatenate')
    #print(origin)
    #print(ds1['time'].min(), ds1['time'].max())
    #print(ds2['time'].min(), ds2['time'].max())
    ds = xr.concat([ds1, ds2], dim='time')
    # finally re-encode the time variable (else get garbage in output netcdf)
    timedata = cftime.date2num(ds['time'].values, units,
                               calendar=calendar_cycle1)

    ds['time'] = xr.DataArray(data=timedata, attrs={'units': units,
                                                    'calendar': calendar_cycle1}, dims='time')
    return ds


def merge_cycles(list_of_cycles, combine='by_coords'):
    """ recursively call merge_2_cycles """
    if combine == 'nested':
        kwargs = {'concat_dim': 'time'}
    else:
        kwargs = {}
    # loop from the end of list
    ncycles = len(list_of_cycles)
#    # init to last cycle
#    dsend = xr.open_mfdataset(list_of_cycles[-1], combine=combine,
#                              decode_times=False, **kwargs)
#    for cycle in np.arange(ncycles-1,0,-1):
#        dsstart = xr.open_mfdataset(list_of_cycles[cycle-1], combine=combine,
#                                    decode_times=False, **kwargs)
#        # merge
#        dsend = merge_2_datasets(dsstart, dsend)
#
    # init to the first cycle
    dsstart = xr.open_mfdataset(list_of_cycles[0], combine=combine,
                                decode_times=False, **kwargs)
    for cycle in np.arange(1,ncycles):
        print(f'merge with cycle {cycle} of {ncycles}')
        dsend = xr.open_mfdataset(list_of_cycles[cycle], combine=combine,
                                  decode_times=False, **kwargs)
        # merge
        dsstart = merge_2_datasets(dsstart, dsend)

    return dsstart
