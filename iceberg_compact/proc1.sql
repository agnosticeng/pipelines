{{define "batch_merges"}}

select
    arrayConcat(
        {{.LEFT.INPUT_FILES | default list | toCH}},
        {{.RIGHT.INPUT_FILES | toCH}}
    ) as INPUT_FILES,
    arrayPushBack(
        {{.LEFT.OUTPUT_FILES | default list | toCH}},
        {{.RIGHT.OUTPUT_FILE | toCH}}
    ) as OUTPUT_FILES

{{end}}