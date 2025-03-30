{{define "iceberg_commit"}}

select icepq_replace('s3:/' || path('{{.TABLE_LOCATION}}'), {{.INPUT_FILES | toCH}}, {{.OUTPUT_FILES | toCH}})

{{end}}