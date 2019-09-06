import pandas as pd
import subprocess as sp

# Utils for GFDL PP/AN machine


def optim_dmget(files):
    """ create a dataframe of all files and directories
    then call dmget on all directories for matching files """
    data = []
    for f in files:
        fname = f.replace('/', ' ').split()[-1]
        fdir = f.rstrip(fname)
        data.append([fdir, fname])
    # create dataframe
    df = pd.DataFrame(data, columns=['directory', 'file'])
    alldirs = list(set(df['directory']))
    for thisdir in alldirs:
        match = df.where(df['directory'] == thisdir)['file'].dropna(how='all')
        files = list(match)
        check = run_dmget(thisdir, files)
        if check != 0:
            print(check)
    return None


def run_dmget(rep, files):
    """ get files from directory rep """
    cmd = ['dmget', '-v', '-d', rep] + files
    cmd_str = " ".join(x for x in cmd)
    check = sp.check_call(cmd_str, shell=True)
    return check
