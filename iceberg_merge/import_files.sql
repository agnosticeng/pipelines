create table import_{{.TASK_ID}}
engine=MergeTree
order by {{.ORDER_BY}}
as (
    select
        * 
    from s3(
        '{{.TABLE_LOCATION}}/data/{' || arrayStringConcat({{.INPUT_FILES | toClickHouseLiteral}}, ',') || '}',
        '{{.S3_ACCESS_KEY_ID}}',
        '{{.S3_SECRET_ACCESS_KEY}}'
    ) 
)