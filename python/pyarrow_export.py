from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow as pa
import time

if __name__ == '__main__':
    start_time = time.time()
    table = csv.read_csv(
        input_file='69M_reddit_accounts.csv',
        read_options=csv.ReadOptions(
            use_threads=True,
            block_size=100 * 1024 * 1024,  # 100m
            skip_rows=1,
            column_names=['id', 'name', 'created_utc', 'updated_on', 'comment_karma', 'link_karma']
        ),
        convert_options=csv.ConvertOptions(
            column_types={
                'id': pa.int64(),
                'name': pa.string(),
                'created_utc': pa.int64(),
                'updated_on': pa.int64(),
                'comment_karma': pa.int64(),
                'link_karma': pa.int64()
            }
        )
    )
    pq.write_table(
        table, '69M_reddit_accounts_pyarrow_snappy.parquet',
        compression='snappy',
        row_group_size=1024 * 1024,
        write_statistics=True
    )
    print("Elapsed time: %s seconds" % (time.time() - start_time))
