import numpy as np

from ep_db import df_to_epdb
from pm_merged_stats import get_pm_merged, engine_mysql


def generate_df_int():
    df_final = get_pm_merged()
    int_cols = [col for col in df_final.columns if 'INTERFERENCE' in col]
    req_cols = ['EUtranCellFDD', 'time'] + int_cols
    df_int = df_final[req_cols].copy()
    df_int['INTERFERENCE_PWR_DBM_MEAN'] = df_int[int_cols].apply(np.mean, axis=1)
    df_to_epdb(df_int,
               engine_mysql,
               name='df_int',
               index=False,
               if_exists='replace',
               chunksize=10_000)


if __name__ == '__main__':
    generate_df_int()
