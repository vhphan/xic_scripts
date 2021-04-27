# %%
import os
from glob import iglob
from loguru import logger
from tqdm import tqdm
import pandas as pd
import numpy as np

from private.keys import con_string
from sqlalchemy import create_engine
from MySQLdb import OperationalError

logger.add("pm_merged.txt", rotation="1 MB")
parent_folder = r"C:\Users\vhphan\Downloads\xic"

cic_filepath = r"C:\Users\vhphan\Downloads\xic\20210326\pm_merged\cic_fdd\imageFDD.parquet"
uic_filepath = r"C:\Users\vhphan\Downloads\xic\20210326\pm_merged\uic_fdd\imageFDD.parquet"
engine_mysql = create_engine(con_string,
                             echo=True,
                             pool_recycle=3600,
                             pool_pre_ping=True)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def sublist(lst1, lst2):
    return set(lst1) <= set(lst2)


def get_cols_needed_cic():
    cic = pd.read_parquet(cic_filepath).reset_index()
    uic = pd.read_parquet(uic_filepath).reset_index()
    sublist(list(cic.columns), list(uic.columns))

    set_difference = set(cic.columns) - set(uic.columns)
    cols_needed_cic = list(set_difference)
    return cols_needed_cic


def process_pm():
    df_list_uic = []
    df_list_cic = []
    cols_needed_cic = get_cols_needed_cic()
    for filename in iglob(f'{parent_folder}/**/*.parquet', recursive=True):
        if os.path.isfile(filename):
            if r'pm_merged\uic_fdd\imageFDD.parquet' in filename:
                print(filename)
                df_list_uic.append(pd.read_parquet(filename))
            if r'pm_merged\cic_fdd\imageFDD.parquet' in filename:
                print(filename)
                df_list_cic.append(pd.read_parquet(filename)[cols_needed_cic])
    df_uic = pd.concat(df_list_uic)
    df_cic = pd.concat(df_list_cic)
    print(df_uic.shape, df_cic.shape)
    df_final = pd.concat([df_cic, df_uic], axis=1)
    df_final.reset_index(inplace=True)

    for chunk in tqdm(np.array_split(df_final, 10_000)):
        i = chunk[['EUtranCellFDD']].head(1).index.values[0]
        try:
            chunk.to_sql('pm_merged', engine_mysql, index=False, if_exists='append')
            if i % 100_000 == 0:
                logger.info( f'{i} completed')
        except OperationalError:
            logger.error(f'{i}')
            logger.error(OperationalError.__repr__(), f'at {i}')


if __name__ == '__main__':
    process_pm()
