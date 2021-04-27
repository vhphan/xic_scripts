from rca_merged import get_df_analyze, engine_mysql

if __name__ == '__main__':

    df_analyse = get_df_analyze()
    df_analyse.to_sql('df_analyse', engine_mysql, index=False, if_exists='replace')