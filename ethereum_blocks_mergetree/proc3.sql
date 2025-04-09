{{define "write_to_sink"}}

insert into sink
select * from buffer_{{.RANGE_START}}_{{.RANGE_END}}

{{end}}

{{define "drop_buffer"}}

drop table buffer_{{.RANGE_START}}_{{.RANGE_END}} sync

{{end}}