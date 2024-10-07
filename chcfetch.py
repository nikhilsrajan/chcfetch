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
    
    base_path = PRODUCT_TO_BASE_PATH_DICT[product]

    base_path_listdir_df = ftputils.get_listdir_df(
        ftp_creds = get_ftp_creds(),
        paths = [base_path],
        show_progress = False,
    )

    available_years = base_path_listdir_df[
        (base_path_listdir_df['type'] == 'Folder') &
        (base_path_listdir_df['name'].str.isdigit())
    ]['name'].apply(int).to_list()

    return available_years


def query_chirps_v2_global_daily(
    product:str,
    years:list[int],
    path_ends_with_list:list[str] = ['tif.gz'],
    njobs:int = 8,
):  
    if path_ends_with_list is None:
        path_ends_with_list = []

    base_path = PRODUCT_TO_BASE_PATH_DICT[product]

    available_years = query_list_of_available_years(product=product)

    query_years = list(set(years) & set(available_years))
    
    query_paths = [f'{base_path}{year}' for year in query_years]

    listdir_df = ftputils.get_listdir_df(
        ftp_creds = get_ftp_creds(),
        paths = query_paths,
        njobs = njobs,
    )
    
    paths_df = listdir_df[listdir_df['type'] == 'File'][['path']].reset_index(drop=True)

    or_ends_filtered_indices = None
    for ends_with in path_ends_with_list:
        ends_filtered_indices = paths_df['path'].str.endswith(ends_with)
        if or_ends_filtered_indices is None:
            or_ends_filtered_indices = ends_filtered_indices
        else:
            ends_filtered_indices = or_ends_filtered_indices | ends_filtered_indices
    paths_df = paths_df[or_ends_filtered_indices]

    for index, row in paths_df.iterrows():
        path = row['path']
        date_str = path.split('/')[-1].replace('chirps-v2.0.', '').replace('.tif', '').replace('.gz', '')
        date = datetime.datetime.strptime(date_str, '%Y.%m.%d')
        paths_df.loc[index, 'date'] = date

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
