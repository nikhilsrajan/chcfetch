import datetime
import pandas as pd
import copy
import tqdm

from . import ftputils


# https://wiki.chc.ucsb.edu/CHIRPS_FAQ#Via_anonymous_ftp
HOST = 'ftp.chc.ucsb.edu'
USER = 'anonymous'
PASSWD = 'your_email_address'


PATH_CHIRPS_V2_GLOBALDAILY_TIFS_P05DEG = '/pub/org/chg/products/CHIRPS-2.0/global_daily/tifs/p05/'
PATH_CHIRPS_V2_PRELIM_GLOBALDAILY_FIXED_TIFS = '/pub/org/chg/products/CHIRPS-2.0/prelim/global_daily/fixed/tifs/'


class Products:
    class CHIRPS:
        P05 = 'p05'
        PRELIM = 'prelim'


VALID_PRODUCTS = [Products.CHIRPS.P05, Products.CHIRPS.PRELIM]


PRODUCT_TO_BASE_PATH_DICT = {
    Products.CHIRPS.P05: PATH_CHIRPS_V2_GLOBALDAILY_TIFS_P05DEG,
    Products.CHIRPS.PRELIM: PATH_CHIRPS_V2_PRELIM_GLOBALDAILY_FIXED_TIFS,
}


CACHE_YEARS = {}


def get_ftp_creds():
    return ftputils.FTPCreds(
        host = HOST,
        user = USER,
        passwd = PASSWD,
    )


def query_list_of_available_years(
    product:str,
):
    if product not in VALID_PRODUCTS:
        raise ValueError(f'Invalid product={product}. product must be from {VALID_PRODUCTS}')
    
    if product not in CACHE_YEARS.keys():
        base_path = PRODUCT_TO_BASE_PATH_DICT[product]

        base_path_listdir_df = ftputils.get_listdir_df(
            ftp_creds = get_ftp_creds(),
            path = base_path,
        )

        available_years = base_path_listdir_df[
            (base_path_listdir_df['type'] == 'Folder') &
            (base_path_listdir_df['name'].str.isdigit())
        ]['name'].apply(int).to_list()

        CACHE_YEARS[product] = available_years

    return CACHE_YEARS[product]


def query_chirps_v2_global_daily(
    product:str,
    startdate:datetime.datetime = None,
    enddate:datetime.datetime = None,
    path_ends_with_list:list[str] = ['tif.gz'],
    show_progress:bool = True,
):  
    if path_ends_with_list is None:
        path_ends_with_list = []

    base_path = PRODUCT_TO_BASE_PATH_DICT[product]

    available_years = query_list_of_available_years(product=product)

    query_years = copy.deepcopy(available_years)
    if startdate is not None:
        query_years = [year for year in query_years if year >= startdate.year]
    if enddate is not None:
        query_years = [year for year in query_years if year <= enddate.year]
    
    query_paths = [f'{base_path}{year}' for year in query_years]

    queried_listdir_dfs = []
    if show_progress:
        iter_query_paths = tqdm.tqdm(query_paths)
    else:
        iter_query_paths = query_paths

    for path in iter_query_paths:
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

    if startdate is not None:
        paths_df = paths_df[paths_df['date'] >= startdate]
    if enddate is not None:
        paths_df = paths_df[paths_df['date'] <= enddate]

    or_ends_filtered_indices = None
    for ends_with in path_ends_with_list:
        ends_filtered_indices = paths_df['path'].str.endswith(ends_with)
        if or_ends_filtered_indices is None:
            or_ends_filtered_indices = ends_filtered_indices
        else:
            ends_filtered_indices = or_ends_filtered_indices | ends_filtered_indices
    paths_df = paths_df[or_ends_filtered_indices]

    paths_df = paths_df.sort_values(by='date').reset_index(drop=True)

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
