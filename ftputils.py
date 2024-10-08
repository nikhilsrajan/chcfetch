import ftplib
import datetime
import warnings
import pandas as pd
import os
import tqdm
import multiprocessing as mp
import functools


class FTPCreds(object):
    """
    Class to store variables required to create an FTP object.

    Note: Previously object of ftplib.FTP was passed into the functions
    get_listdir_df and download_file. But that made it difficult to parallelise
    the download_file function as multiprocessing was unable to pickle an IO
    object which was the ftplib.FTP object. Thus the creation of ftplib.FTP object
    now happens in the function and the FTPCreds since it contains only str
    variables could easily be pickled and the function can be parallelised.
    """
    def __init__(self, host:str, user:str, passwd:str):
        self.host = host
        self.user = user
        self.passwd = passwd


def get_ftp(ftp_creds:FTPCreds):
    return ftplib.FTP(
        host = ftp_creds.host,
        user = ftp_creds.user,
        passwd = ftp_creds.passwd,
    )


def get_listdir_df_single(
    path:str,
    ftp_creds:FTPCreds,
) -> pd.DataFrame:
    ftp = get_ftp(ftp_creds=ftp_creds)

    listdir = []
    ftp.cwd(path)
    ftp.retrlines(cmd='LIST', callback=listdir.append)

    cwd = ftp.pwd()
    if cwd == '/':
        cwd = ''

    ftp.quit()

    data = {
        'type': [], 'permission': [], 'n_dirs': [], 'uid': [], 'gid': [],
        'size': [], 'month': [], 'day': [], 'year': [], 'time': [],
        'name': [], 'link_to': [], 'path': [],
    }

    for entry in listdir:
        entry_splits = entry.split()
        _permission = entry_splits[0]
        _type = {
            'd': 'Folder',
            'l': 'Symbolic link',
            '-': 'File',
        }[_permission[0]]
        _n_dirs = int(entry_splits[1])
        _uid = entry_splits[2]
        _gid = entry_splits[3]
        _size = int(entry_splits[4])
        _month = entry_splits[5]
        _day = int(entry_splits[6])
        _year_or_time = entry_splits[7]
        if ':' in _year_or_time: # is time
            _year = datetime.datetime.now().year
            _time = _year_or_time
        else:
            _year = int(_year_or_time)
            _time = None
        _name = entry_splits[8]
        _link_to = None
        if _type == 'Symbolic link':
            if len(entry_splits) <= 9:
                warnings.warn('Expected more entries for symbolic link.')
            elif entry_splits[9] != '->':
                warnings.warn('Expected -> for symbolic link.')
            else:
                _link_to = entry_splits[10]
        
        _path = cwd + '/' + _name

        data['type'].append(_type)
        data['permission'].append(_permission)
        data['n_dirs'].append(_n_dirs)
        data['uid'].append(_uid)
        data['gid'].append(_gid)
        data['size'].append(_size)
        data['month'].append(_month)
        data['day'].append(_day)
        data['year'].append(_year)
        data['time'].append(_time)
        data['name'].append(_name)
        data['link_to'].append(_link_to)
        data['path'].append(_path)

    listdir_df = pd.DataFrame(data=data)
    return listdir_df


def get_listdir_df(
    ftp_creds:FTPCreds,
    paths:list[str],
    njobs:int = 1,
    show_progress:bool = True,
):
    get_listdir_df_single_partial = functools.partial(
        get_listdir_df_single,
        ftp_creds = ftp_creds,
    )

    with mp.Pool(njobs) as p:
        if show_progress:
            listdir_dfs = list(tqdm.tqdm(p.imap(get_listdir_df_single_partial, paths), total=len(paths)))
        else:
            listdir_dfs = list(p.imap(get_listdir_df_single_partial, paths))
    
    listdir_df = pd.concat(listdir_dfs).reset_index(drop=True)

    return listdir_df


def download_file(
    path:str, 
    ftp_creds:FTPCreds,
    download_filepath:str = None,
    download_folderpath:str = None,
    overwrite:bool = False,
) -> str:
    if download_filepath is None and download_folderpath is None:
        raise ValueError('Either download_filepath or download_folderpath should be not None.')
    
    if download_filepath is None:
        filename = os.path.split(path)[1]
        download_filepath = os.path.join(download_folderpath, filename)
    else:
        download_folderpath = os.path.split(download_filepath)[0]

    if not os.path.exists(download_filepath) or overwrite:
        temp_download_filepath = download_filepath + '.temp'

        ftp = get_ftp(ftp_creds=ftp_creds)
        os.makedirs(download_folderpath, exist_ok=True)
        ftp.retrbinary("RETR " + path, open(temp_download_filepath, 'wb').write)
        ftp.quit()

        os.rename(temp_download_filepath, download_filepath)

    return download_filepath


def download_files(
    ftp_creds:FTPCreds,
    paths:list[str],
    download_folderpath:str = None,
    overwrite:bool = False,
    njobs:int = 1,
):
    download_file_partial = functools.partial(
        download_file,
        ftp_creds = ftp_creds,
        download_folderpath = download_folderpath,
        overwrite = overwrite,
    )

    with mp.Pool(njobs) as p:
        local_filepaths = list(tqdm.tqdm(p.imap(download_file_partial, paths), total=len(paths)))

    return local_filepaths
