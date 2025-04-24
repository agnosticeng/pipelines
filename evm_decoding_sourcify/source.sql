{{define "source"}}

with
    '{{.SOURCE_BASE_URL | default "https://export.sourcify.app"}}' as base_url,

    (
        select 
            files.compiled_contracts
        from url(base_url || '/manifest.json')
    ) as files,

    (
        select base_url || '/' || '{' || arrayStringConcat(files, ',') || '}'
    ) as glob_url

select 
    today() as DATE,
    glob_url as GLOB_URL

{{end}}