import duckdb
import time

if __name__ == '__main__':
    start_time = time.time()
    rel = duckdb.read_csv('69M_reddit_accounts.csv', header=True, delimiter=',')
    rel.write_parquet(file_name='69M_reddit_accounts_duckdb_snappy.parquet', compression='snappy')
    print("Elapsed time: %s seconds" % (time.time() - start_time))
