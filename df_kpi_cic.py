import pandas as pd

from ep_db import df_to_epdb
from pm_merged_stats import engine_mysql

# path = r"C:\Users\vhphan\Downloads\xic\20210328\rca_transformed\rca_cic_fdd\df_kpi.parquet"
path = r"C:\Users\vhphan\Downloads\xic\20210328\pm_merged\rca_cic_fdd\df_kpi.parquet"
if __name__ == '__main__':
    df_kpi_cic = pd.read_parquet(path)
    print(df_kpi_cic.columns)
    print(df_kpi_cic.info)
    df_to_epdb(df_kpi_cic, engine_mysql, name='cic_pm_merge', chunksize=10_000, if_exists='append')