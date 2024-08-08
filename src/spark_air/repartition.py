import pandas as pd
import os

def re_partition(load_dt, from_path='/data/20240808/movie_data/data/extract'):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/{from_path}/load_dt={load_dt}'
    write_base = f'{home_dir}/data/movie/repartition/'

    df = pd.read_parquet(read_path)
    df['load_dt'] = load_dt
    #rm_dir(write_path)
    df.to_parquet(
            write_base,
            partition_cols=['load_dt', 'multiMovieYn', 'repNationCd']
            )

