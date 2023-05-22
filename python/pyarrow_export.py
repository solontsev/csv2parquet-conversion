from pyarrow import csv
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import time

if __name__ == '__main__':
    start_time = time.time()
    schema = pa.schema([
        ('id', pa.int64()),
        ('name', pa.string()),
        ('created_utc', pa.int64()),
        ('updated_on', pa.int64()),
        ('comment_karma', pa.int64()),
        ('link_karma', pa.int64())
    ])
    columns = ['id', 'name', 'created_utc', 'updated_on', 'comment_karma', 'link_karma']
    dtype={
        'id': 'int64',
        'name': 'string[pyarrow]',
        'created_utc': 'int64',
        'updated_on': 'int64',
        'comment_karma': 'int64',
        'link_karma': 'int64'
    }

    df = pd.read_csv('69M_reddit_accounts.csv', low_memory=True, names=columns, dtype=dtype, header=0)
    df.to_parquet('69M_reddit_accounts_pyarrow_snappy.parquet', engine='pyarrow')

    # with pq.ParquetWriter('69M_reddit_accounts_pyarrow_snappy.parquet', schema=schema, compression='snappy') as writer:
    #     with pd.read_csv('69M_reddit_accounts.csv', header=0, names=columns, chunksize=1_000_000, low_memory=True) as reader:
    #         for df in reader:
    #             batch = pa.RecordBatch.from_pandas(df=df, Schema_schema=schema)
    #             writer.write_batch(batch, row_group_size=1024*1024)

    print("Elapsed time: %s seconds" % (time.time() - start_time))
