{{define "init_sink"}}

create table sink as remote(
    '{{.CH_HOST}}', 
    {{.CH_DATABASE | default "default"}}, 
    {{.CH_TABLE}},
    '{{.CH_USER | default "default"}}',
    '{{.CH_PASSWD | default ""}}'
)

{{end}}

{{define "init_start"}}

select 
    maxOrNull(number) + 1 as INIT_START
from sink

{{end}}