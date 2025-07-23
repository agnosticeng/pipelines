{{define "write_json_file"}}

insert into function s3('{{.S3_DESTINATION_PATH}}/{{.RANGE_START}}-{{.RANGE_END}}.json')
select * from range_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}

{{define "drop_range"}}

drop table range_{{.RANGE_START}}_{{.RANGE_END}} sync

{{end}}