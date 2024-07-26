import datetime
import pandas as pd
import gzip
import shutil
import os
import rasterio

from . import ftputils


# https://wiki.chc.ucsb.edu/CHIRPS_FAQ#Via_anonymous_ftp
HOST = 'ftp.chc.ucsb.edu'
USER = 'anonymous'
PASSWD = 'your_email_address'


PATH_CHIRPS_V2_GLOBALDAILY_TIFS_P05DEG = '/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/'
PATH_CHIRPS_V2_PRELIM_GLOBALDAILY_FIXED_TIFS = '/pub/org/chg/products/CHIRPS-2.0/prelim/global_daily/fixed/tifs/'


def get_ftp_creds():
    return ftputils.FTPCreds(
        host = HOST,
        user = USER,
        passwd = PASSWD,
    )


def query_chirps_v2_global_daily(
    startdate:datetime.datetime,
    enddate:datetime.datetime,
    product:str
):  
    VALID_PRODUCTS = ['p05', 'prelim']

    if product not in VALID_PRODUCTS:
        raise ValueError(f'Invalid product={product}. product must be from {VALID_PRODUCTS}')

    base_path = {
        'p05': PATH_CHIRPS_V2_GLOBALDAILY_TIFS_P05DEG,
        'prelim': PATH_CHIRPS_V2_PRELIM_GLOBALDAILY_FIXED_TIFS,
    }[product]

    query_years = list(set([startdate.year, enddate.year]))
    query_paths = [f'{base_path}{year}' for year in query_years]

    queried_listdir_dfs = []
    for path in query_paths:
        queried_listdir_dfs.append(
            ftputils.get_listdir_df(
                ftp_creds=get_ftp_creds(),
                path=path,
            )
        )
    
    listdir_df = pd.concat(queried_listdir_dfs)
    del queried_listdir_dfs

    paths_df = listdir_df[listdir_df['type'] == 'File'][['path']].reset_index(drop=True)

    for index, row in paths_df.iterrows():
        path = row['path']
        date_str = path.split('/')[-1].replace('chirps-v2.0.', '').replace('.tif', '').replace('.gz', '')
        date = datetime.datetime.strptime(date_str, '%Y.%m.%d')
        paths_df.loc[index, 'date'] = date

    paths_df = paths_df[
        (paths_df['date'] >= startdate) \
        & (paths_df['date'] <= enddate)
    ].sort_values(by='date').reset_index(drop=True)

    return paths_df


def download_files_from_paths_df(
    paths_df:pd.DataFrame,
    download_folderpath:str,
    overwrite:bool = False,
    njobs:int = 8,
    download_filepath_col:str = 'local_filepath',
):
    local_filepaths = ftputils.download_files(
        ftp_creds = get_ftp_creds(),
        paths = paths_df['path'].to_list(),
        download_folderpath = download_folderpath,
        overwrite = overwrite,
        njobs = njobs,
    )

    paths_df.loc[paths_df.index, download_filepath_col] = local_filepaths

    return paths_df


def decompress_gzip(gzip_filepath:str, out_filepath:str):
    with gzip.open(gzip_filepath) as gzip_file:
        with open(out_filepath, 'wb') as f_out:
            shutil.copyfileobj(gzip_file, f_out)


def read_tif(tif_filepath:str):
    with rasterio.open(tif_filepath) as src:
        ndarray = src.read()
        meta = src.meta.copy()
    return ndarray, meta


def add_epochs_prefix(filepath, additional:str=''):
    folderpath, filename = os.path.split(filepath)
    temp_prefix = f"{additional}{int(datetime.datetime.now().timestamp() * 1000000)}_"
    temp_tif_filepath = os.path.join(folderpath, temp_prefix + filename)
    return temp_tif_filepath


def read_gzip_tif(gzip_tif_filepath):
    gzip_tif_filepath_wo_ext = gzip_tif_filepath[:-3]
    temp_tif_filepath = add_epochs_prefix(filepath=gzip_tif_filepath_wo_ext, additional='temp_')
    decompress_gzip(gzip_filepath=gzip_tif_filepath, out_filepath=temp_tif_filepath)
    ndarray, meta = read_tif(tif_filepath=temp_tif_filepath)
    os.remove(temp_tif_filepath)
    return ndarray, meta
