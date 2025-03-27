insert into function s3(
    '{{.TABLE_LOCATION}}/data/{{.OUTPUT_FILE}}',
    '{{.S3_ACCESS_KEY_ID}}',
    '{{.S3_SECRET_ACCESS_KEY}}'
)
select * from import_{{.TASK_ID}}
{{if .ORDER_BY}}
order by {{.ORDER_BY}}
{{end}}