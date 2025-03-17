-- select 
--     coalesce(
--         maxOrNull(block_number) + 1,
--         {{.DEFAULT_START}}
--     ) as INIT_START
-- from iceberg(
--     '{{.ICEBERG_TABLE_LOCATION}}',
--     '{{.S3_ACCESS_KEY_ID}}',
--     '{{.S3_SECRET_ACCESS_KEY}}'
-- )

-- While the above query is very natural an easy, unfortunately it is very slow.
-- As of today, ClickHouse cannot yet make use of Iceberg & Parquet min/max statitics to answer queries.
-- Thus, the above query scan the full block_number columns of every Parquet file in the last table snapshot.
-- Until these optimizations are implemented, we will resort to a dirty (but cool) trick of reading Parquet files 
-- metadata and doing "manual" max() aggregagtion for the block_number column.

with 
    (
        select 
            groupUniqArray(_file)
        from iceberg('https://s3.rbx.io.cloud.ovh.net/agnostic-data-ice-ethereum-mainnet/decoded_logs')
    ) as files,

    (
        select 
            'https://s3.rbx.io.cloud.ovh.net/agnostic-data-ice-ethereum-mainnet/decoded_logs' ||
            '/data/{' ||
            arrayStringConcat(files, ',') ||
            '}'
    ) as url_pat,

    (
       select
            maxOrNull(
                arrayMax(
                    x -> toUInt64(x.statistics.max),
                    arrayFilter(
                        x -> x.name = 'block_number', 
                        arrayFlatten(row_groups.columns)
                    )
                )
            )
        from s3(url_pat, 'ParquetMetadata')
    ) as max

select
    coalesce(
        max + 1,
        {{.DEFAULT_START}}
    ) as INIT_START