{{define "init_s3_config"}}
create named collection s3_config as use_environment_credentials=1
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

with 
    (
        select 
            groupUniqArray(_file) 
        from iceberg('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}', settings iceberg_use_version_hint=true)
    ) as dest_files,

    (
        if (
            length(dest_files) = 1,
            toString(dest_files[1]),
            '{' || arrayStringConcat(dest_files, ',') || '}'
        )
    ) as dest_pat,

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
        from s3('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}' || '/data/' || dest_pat, 'ParquetMetadata')
    ) as max

select 
    max + 1 as INIT_START

{{end}}