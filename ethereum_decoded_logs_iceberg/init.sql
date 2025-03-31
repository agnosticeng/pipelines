{{define "init_source"}}

create table source as remote(
    '{{.CH_HOST}}', 
    {{.CH_DATABASE | default "default"}}, 
    {{.CH_TABLE}},
    '{{.CH_USER | default "default"}}',
    '{{.CH_PASSWD | default ""}}'
)

{{end}}

{{define "init_evm_abi_decoding"}}

create dictionary evm_abi_decoding (
    selector String,
    fullsigs Array(String)
)
primary key selector
source(http(url '{{.EVM_ABI_DECODING_URL}}' format 'Parquet'))
lifetime(min 3600 max 7200)
layout(hashed())

{{end}}

{{define "init_start"}}

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

-- While the above query is very natural an simple, unfortunately it is very slow.
-- As of today, ClickHouse cannot yet make use of Iceberg & Parquet min/max statitics to answer queries.
-- Thus, the above query scan the full block_number columns of every Parquet file in the last table snapshot.
-- Until these optimizations are implemented, we will resort to a dirty (but cool) trick of reading Parquet files 
-- metadata and doing "manual" max() aggregagtion for the block_number column.

with 
    (
        select 
            groupUniqArray(_file)
        from iceberg('{{.ICEBERG_TABLE_LOCATION}}')
    ) as files,

    (
        if (
            length(files) = 1,
            toString(files[1]),
            '{' || arrayStringConcat(files, ',') || '}'
        )
    ) as files_pat,

    (
        select '{{.ICEBERG_TABLE_LOCATION}}' || '/data/' || files_pat
    ) as url_pat,

    (
       select
            max(
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

select assumeNotNull(max + 1) as INIT_START

{{end}}







with 
    (
        select 
            groupUniqArray(_file)
        from iceberg('https://s3.rbx.io.cloud.ovh.net/agnostic-data-ice-ethereum-mainnet/decoded_logs', '17ecdf911ec344b2978047e1d6cc9794', '11ef5fed7eab46c28c2ee9e2574072c0')
    ) as files,

    (
        if (
            length(files) = 1,
            toString(files[1]),
            '{' || arrayStringConcat(files, ',') || '}'
        )
    ) as files_pat,

    (
        select 'https://s3.rbx.io.cloud.ovh.net/agnostic-data-ice-ethereum-mainnet/decoded_logs' || '/data/' || files_pat
    ) as url_pat,

    (
       select
            max(
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

select assumeNotNull(max + 1) as INIT_START