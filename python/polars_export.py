import polars as pl
import time

if __name__ == '__main__':
    df = pl.scan_csv(source="69M_reddit_accounts.csv", separator=',', has_header=True)

    start_time = time.time()
    df.sink_parquet(
        path='69M_reddit_accounts_polars_snappy.parquet',
        compression='snappy',
        row_group_size=1024 * 1024,
        statistics=True
    )
    print("Elapsed time: %s seconds" % (time.time() - start_time))
