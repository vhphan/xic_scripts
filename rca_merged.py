# %%
import os
from datetime import datetime
from functools import wraps
from glob import iglob
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from private.keys import con_string

engine_mysql = create_engine(con_string,
                             echo=True,
                             pool_recycle=3600,
                             pool_pre_ping=True)
parent_folder = r"C:\Users\vhphan\Downloads\xic"
cic_path = r"C:\Users\vhphan\Downloads\xic\20210328\rca_transformed\rca_cic_fdd\df_analyse.parquet"
uic_path = r"C:\Users\vhphan\Downloads\xic\20210328\rca_transformed\rca_uic_fdd\df_analyse.parquet"


def timer(func):
    """Print the runtime of the decorated function"""

    @wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = datetime.now()
        value = func(*args, **kwargs)
        end_time = datetime.now()
        run_time = end_time - start_time
        print('run time', run_time, func.__name__)
        return value

    return wrapper_timer


def read_parquet():
    for filename in iglob(f'{parent_folder}/**/*.parquet', recursive=True):
        if os.path.isfile(filename):
            print(filename)
            p = Path(filename)
            print(filename.split('\\')[7])


@timer
def get_data(file_prefix, file_type='parquet'):
    data = {}
    for filename in iglob(f'{parent_folder}/**/*.parquet', recursive=True):
        if os.path.isfile(filename):
            p = Path(filename)
            date_str = filename.split('\\')[7]
            cat = p.parent.name
            basename = Path(filename).stem
            for prefix in file_prefix:
                if prefix in basename and file_type == 'parquet':
                    print(cat, basename, date_str)
                    print(filename)
                    df = pd.read_parquet(filename).reset_index()
                    if f"{cat}_{basename}" in data:
                        data[f"{cat}_{basename}"] = pd.concat([data[f"{cat}_{basename}"], df])
                    else:
                        data[f"{cat}_{basename}"] = df
    return data


@timer
def get_processed_data():
    rca = get_data(['details', 'result'])
    rca_details_df = pd.DataFrame()
    rca_result_df = pd.DataFrame()
    for k, df in rca.items():
        if '_details_' in k:
            rca_details_df = rca_details_df.append(df.drop('index', axis=1), ignore_index=True)
        if '_result_' in k:
            rca_result_df = rca_result_df.append(df.drop('index', axis=1), ignore_index=True)

    rca_result_df = rca_result_df[rca_result_df['result']]
    rca = pd.merge(rca_result_df, rca_details_df, on=['cause', 'issue_class', 'primary_cause', 'secondary_cause'])
    rca.drop(['result', 'MeContext'], axis=1, inplace=True)
    rca['cell_details2'], rca['relation_details2'] = None, None

    for i, row in rca.iterrows():
        if row['cell_details']:
            df_row = pd.read_json(row['cell_details'], orient='table')
            df_row = df_row[df_row['EUtranCellFDD'] == row['EUtranCellFDD']]
            row['cell_details2'] = df_row.to_json(orient='table')

    for i, row in rca.iterrows():
        if row['relation_details']:
            df_row = pd.read_json(row['relation_details'], orient='table')
            df_row = df_row[df_row['s_EUtranCellFDD'] == row['EUtranCellFDD']]
            row['relation_details2'] = df_row.to_json(orient='table')

    return rca


@timer
def get_df_analyze():
    df_analyse_uic = pd.read_parquet(uic_path)
    df_analyse_cic = pd.read_parquet(cic_path)
    df_analyse = pd.concat([df_analyse_uic, df_analyse_cic])
    df_analyse.reset_index(inplace=True)
    return df_analyse


@timer
def rca_merged():
    df_analyze = get_df_analyze()
    df_processed = get_processed_data()
    rca_merged_ = pd.merge(df_processed, df_analyze,
                           left_on=['EUtranCellFDD', 'issue_class'],
                           right_on=['EUtranCellFDD', 'class'],
                           )
    rca_merged_ = rca_merged_[['EUtranCellFDD',
                               'cause',
                               'issue_class',
                               'primary_cause',
                               'secondary_cause',
                               'cell_details2',
                               'relation_details2',
                               'package',
                               'score_final']]
    rca_merged_.to_sql('rca_merged', engine_mysql, if_exists='append', index=False)


if __name__ == '__main__':
    rca_merged()
