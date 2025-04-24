{{define "init_table_files"}}

create table iceberg_table_files engine=TinyLog
as (
    with 
        '{{.TABLE_LOCATION}}' as table_location,
        '{{.MAX_MERGE_OUTPUT_SIZE | default "10737418240"}}' as max_merge_output_size

    select 
        _file as file,
        any(_size) as size,
        any(_time) as time
    from iceberg(table_location, settings iceberg_use_version_hint=true)
    where _size < max_merge_output_size
    group by file
    order by time
)

{{end}}