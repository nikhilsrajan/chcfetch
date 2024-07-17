import ftplib
import datetime
import pandas as pd
import tqdm

import ftputils


# https://wiki.chc.ucsb.edu/CHIRPS_FAQ#Via_anonymous_ftp
HOST = 'ftp.chc.ucsb.edu'
USER = 'anonymous'
PASSWD = 'your_email_address'


PATH_CHIRPS_V2_GLOBALDAILY_TIFS_P05DEG = '/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/'
PATH_CHIRPS_V2_PRELIM_GLOBALDAILY_FIXED_TIFS = '/pub/org/chg/products/CHIRPS-2.0/prelim/global_daily/fixed/tifs/'


def get_ftp():
    return ftplib.FTP(
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

    ftp = get_ftp()

    queried_listdir_dfs = []
    for path in query_paths:
        queried_listdir_dfs.append(
            ftputils.get_listdir_df(
                ftp=ftp,
                path=path,
            )
        )

    ftp.quit()
    
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
):
    ftp = get_ftp()

    paths_df = paths_df.reset_index(drop=True)

    for index, row in tqdm.tqdm(paths_df.iterrows(), total=paths_df.shape[0]):
        download_filepath = ftputils.download_file(
            ftp = ftp,
            path = row['path'],
            download_folderpath = download_folderpath,
            overwrite = overwrite,
        )
        paths_df.loc[index, 'local_filepath'] = download_filepath

    ftp.quit()

    return paths_df
