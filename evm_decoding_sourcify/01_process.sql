{{define "fetch_abis"}}

create table abis engine=MergeTree order by tuple()
as (
    select
        arrayJoin(JSONExtract(compilation_artifacts::String, 'abi', 'Array(String)')) as abi_field
    from url('{{.GLOB_URL}}')
)

{{end}}

{{define "generate_decoding_dataset"}}

create table evm_abi_decoding engine=MergeTree order by tuple()
as (
    with 
        signatures as (
            select
                JSONExtract(evm_signature_from_descriptor(abi_field::String), 'JSON') as res
            from abis
            having res.error is null
        ),

        ranked as (
            select 
                evm_hex_decode(res.value.selector::String) as selector,
                res.value.fullsig::String as fullsig,
                count(*) as occurences
            from signatures
            group by selector, fullsig
            order by occurences desc
        ),

        grouped as (
            select
                selector,
                groupArray(fullsig) as fullsigs
            from ranked
            group by selector
        )

    select * from grouped
)

{{end}}

{{define "check_decoding_dataset"}}

select 
    throwIf(count(*) <= 1400000),
    throwIf(countIf(length(selector) = 4) <= 1200000),
    throwIf(countIf(length(selector) = 32) <= 250000)
from evm_abi_decoding

{{end}}

{{define "save_as_daily_parquet"}}

insert into function s3('{{.TARGET_BASE_URL}}' || toYYYYMMDD({{.DATE | toCH}}) || '.parquet')
select * from evm_abi_decoding

{{end}}

{{define "save_as_latest_parquet"}}

insert into function s3('{{.TARGET_BASE_URL}}' || 'latest.parquet')
select * from evm_abi_decoding

{{end}}