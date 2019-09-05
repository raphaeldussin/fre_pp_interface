import glob
import datetime as dt


def create_timeserie_monofield(rep, field, pattern=None):
    """ find files from several time slices and build a unique timeserie """
    fmatches = find_monofield_files(rep, field, pattern=pattern)
    out = build_mixed_slices_listfiles(fmatches)
    return out


def find_monofield_files(rep, field, pattern=None):
    """ find all files in directory rep for field field with optional
    pattern"""
    files = glob.glob(rep + '/' + f'**/*{field}.nc', recursive=True)
    matches = []
    if pattern is not None:
        for f in files:
            if (f.find(pattern) != -1):
                matches.append(f)
    else:
        matches = files
    return matches


def build_mixed_slices_listfiles(listfiles):
    """ from a list of files with different time slices, build a
        unique timeseries monotonically (without duplicates) """
    slices = available_timeslices(listfiles)
    ordered_slices = order_timeslices(slices)
    merged_list = []
    last = dt.datetime(1, 1, 1)  # bogus last time
    # loop over slices
    for tslice in ordered_slices:
        # build list of files
        tmplist = filter_by_slice_and_order_chrono(listfiles, tslice)
        for f in tmplist:
            fname = f.replace('/', ' ').split()[-1]
            start, end = get_dates_from_filename(fname)
            if start > last:
                merged_list.append(f)
                last = end
    return merged_list


def available_timeslices(listfiles):
    """ figure out what time slices are available from list of files """
    tslices = []
    for f in listfiles:
        # break the full path into elements and take N-1 string
        # which is the time slice (provided the database is built properly)
        ts = f.replace('/', ' ').split()[-2]
        tslices.append(ts)
    # remove duplicates
    slices = list(set(tslices))
    return slices


def order_timeslices(slices):
    """ order time slices in decreasing order """
    # make it integers
    nyr_slice = []
    for sl in slices:
        nyr_slice.append(int(sl.rstrip('yr')))
    # we want longer timeseries first
    nyr_slice.sort(reverse=True)
    ordered_slices = []
    # rebuild the string
    for sl in nyr_slice:
        ordered_slices.append(str(sl) + 'yr')
    return ordered_slices


def filter_by_slice_and_order_chrono(listfiles, tslice):
    """ filter list and sort """
    outlist = []
    for f in listfiles:
        fts = f.replace('/', ' ').split()[-2]
        if fts == tslice:
            outlist.append(f)
    outlist.sort()
    return outlist


def get_dates_from_filename(fname):
    """ infer start/end dates from filename """
    cdates = fname.replace('.', ' ').split()[1]
    cstartdate, cenddate = cdates.replace('-', ' ').split()
    if len(cstartdate) == 4:
        timeformat = '%Y'
    elif len(cstartdate) == 6:
        timeformat = '%Y%m'
    elif len(cstartdate) == 8:
        timeformat = '%Y%m%d'
    startdate = dt.datetime.strptime(cstartdate, timeformat)
    enddate = dt.datetime.strptime(cenddate, timeformat)
    return startdate, enddate
