{{define "fetch_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    select 
        *
    from url(
        'https://hacker-news.firebaseio.com/v0/item/{' ||
        '{{.RANGE_START}}' ||
        '..' ||
        '{{.RANGE_END}}' ||
        '}.json'
    )
)

{{end}}