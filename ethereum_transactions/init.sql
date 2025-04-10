{{define "init_start"}}

with 
    (
        select 
            groupUniqArray(_file)
        from iceberg('{{.ICEBERG_DESTINATION_TABLE_LOCATION}}')
    ) as files,

    (
        if (
            length(files) = 1,
            toString(files[1]),
            '{' || arrayStringConcat(files, ',') || '}'
        )
    ) as files_pat,

    (
        select '{{.ICEBERG_DESTINATION_TABLE_LOCATION}}' || '/data/' || files_pat
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

select max + 1 as INIT_START

{{end}}