import ftplib
import datetime
import warnings
import pandas as pd
import os


def get_listdir_df(
    ftp:ftplib.FTP, 
    path:str,
) -> pd.DataFrame:
    listdir = []
    ftp.cwd(path)
    ftp.retrlines(cmd='LIST', callback=listdir.append)

    cwd = ftp.pwd()
    if cwd == '/':
        cwd = ''

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


def download_file(
    ftp:ftplib.FTP, 
    path:str, 
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
        os.makedirs(download_folderpath, exist_ok=True)
        ftp.retrbinary("RETR " + path, open(download_filepath, 'wb').write)

    return download_filepath

