{{define "source"}}

with
    {{.MAX_BATCH_SIZE | default "1000"}} as max_batch_size,

    (
        select 
            groupUniqArray(_file) 
        from iceberg('{{.ICEBERG_SOURCE_TABLE_LOCATION}}', settings iceberg_use_version_hint=true)
    ) as source_files,

    (
        if (
            length(source_files) = 1,
            toString(source_files[1]),
            '{' || arrayStringConcat(source_files, ',') || '}'
        )
    ) as source_pat,

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
        from s3('{{.ICEBERG_SOURCE_TABLE_LOCATION}}' || '/data/' || source_pat, 'ParquetMetadata')
    ) as tip,

    coalesce(
        {{.RANGE_END | toCH}} + 1,
        {{.INIT_START | toCH}},
        {{.DEFAULT_START | toCH}},
        0
    ) as start

select 
    generate_series as RANGE_START,
    least(
        assumeNotNull(tip), 
        toUInt64(generate_series + max_batch_size - 1)
    ) as RANGE_END
from generate_series(
    toUInt64(assumeNotNull(start)),
    assumeNotNull(tip),
    toUInt64(max_batch_size)
)

{{end}}