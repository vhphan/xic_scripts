from retry import retry


@retry(tries=5, delay=1, backoff=2)
def df_to_epdb(df, con, **kwargs):
    df.to_sql(**kwargs, con=con)
