{{define "import_parquet_files"}}

create table import_{{.TASK_ID}}
engine=MergeTree
order by {{.ORDER_BY}}
settings old_parts_lifetime=10
as (
    select
        * 
    from s3('{{.TABLE_LOCATION}}/data/{' || arrayStringConcat({{.INPUT_FILES | toClickHouseLiteral}}, ',') || '}') 
)

{{end}}

{{define "export_merged_parquet_file"}}

insert into function s3('{{.TABLE_LOCATION}}/data/{{.OUTPUT_FILE}}')
select * from import_{{.TASK_ID}}
order by {{.ORDER_BY}}

{{end}}

{{define "drop_import_table"}}

drop table import_{{.TASK_ID}} sync 

{{end}}