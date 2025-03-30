{{define "source"}}

with 
    {{.MAX_INPUT_FILES | default "10"}} as max_input_files,
    {{.MAX_MERGE_OUTPUT_SIZE | default "10737418240"}} as max_output_size, -- default=10GB

    q0 as (
        select
            *
        from iceberg_table_files
        where not has({{.USED_FILES | default list | toCH}}, file)
    ),

    q1 as (
        select 
            groupArray(
                tuple(
                    file as file, 
                    size as size, 
                    time as time
                )
            ) over (order by size desc rows between current row and unbounded following) as input_files 
        from q0
    )

select 
    generateUUIDv7() || '.parquet' as OUTPUT_FILE,
    arrayMap(x -> x.file, input_files) as INPUT_FILES,
    arrayConcat({{.USED_FILES | default list | toCH}}, INPUT_FILES) as USED_FILES
from q1
where length(input_files) > 1
and length(input_files) <= max_input_files
and arraySum(arrayMap(x -> assumeNotNull(x.size), input_files)) <= max_output_size
order by length(input_files) desc, arraySum(arrayMap(x -> assumeNotNull(x.size), input_files)) desc
limit 1

settings enable_named_columns_in_function_tuple=1

{{end}}