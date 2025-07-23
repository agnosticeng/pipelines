{{define "source"}}

with
    {{.MAX_BATCH_SIZE | default "10"}} as max_batch_size,
    {{.MAX_BATCH_PER_RUN | default "100"}} as max_batch_per_run,
    1000000000 as tip,

    coalesce(
        {{.RANGE_END | toCH}} + 1,
        {{.DEFAULT_START | toCH}},
        1
    ) as start

select 
    generate_series as RANGE_START,
    least(
        tip, 
        toUInt64(generate_series + max_batch_size - 1)
    ) as RANGE_END
from generate_series(
    toUInt64(assumeNotNull(start)),
    assumeNotNull(tip),
    toUInt64(max_batch_size)
)
limit max_batch_per_run

{{end}}