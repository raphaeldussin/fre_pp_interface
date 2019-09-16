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
    # time is given in days since origin
    ndays_cycle1 = ds1['time'][-1].values - ds1['time'][0].values
    ndays_cycle2 = ds2['time'][-1].values - ds2['time'][0].values
    # we always come short of the last year
    nyears_cycle1 = np.ceil(ndays_cycle1/365 + 1)
    nyears_cycle2 = np.ceil(ndays_cycle2/365 + 1)

    units_cycle1 = ds1['time'].attrs['units']
    origin_cycle1 = dt.datetime.strptime(units_cycle1,
                                         "days since %Y-%m-%d %H:%M:%S")
    calendar_cycle1 = ds1['time'].attrs['calendar']

    # set the new origin nyears_cycle2 in the past
    origin = origin_cycle1 - dt.timedelta(days=365*nyears_cycle2)
    units = dt.datetime.strftime(origin, "days since %Y-%m-%d %H:%M:%S")

    # set up the merged time axis
    # cycle1 values remains the same but the origin is shifted
    ds1['time'] = cftime.num2date(ds1['time'].values, units,
                                  calendar=calendar_cycle1)
    # cycle2 uses the same updated origin but also needs to have its values
    # shifted nyears_cycle1 in the future
    ds2['time'] = cftime.num2date(ds2['time'].values + nyears_cycle1*365,
                                  units, calendar=calendar_cycle1)
    # we can now concatenate the datasets
    ds = xr.concat([ds1, ds2], dim='time')
    # finally re-encode the time variable (else get garbage in output netcdf)
    timedata = cftime.date2num(ds['time'].values, units,
                               calendar=calendar_cycle1)

    ds['time'] = xr.DataArray(data=timedata, attrs={'units': units,
                                                    'calendar': calendar_cycle1})
    return ds


#def merge_cycles(list_of_cycles):
    # loop from the end of list
    #ncycles = len(list_of_cycles)
    # nmerge = ncyles - 1
    # for cycle in np.arange(ncycles, 1, -1):
        # out = merge_2_cycles(list_of_cycles[cycle-1], list_of_cycles[cycle])
