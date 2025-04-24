{{define "fetch_range"}}

create table range_{{.RANGE_START}}_{{.RANGE_END}} engine=Memory
as (
    with
        q0 as (
            select
                timestamp,
                block_hash,
                block_number,
                transaction_from,
                transaction_status,
                transaction_hash,
                transaction_index,
                log_index,
                address,
                topics,
                data
            from iceberg('{{.ICEBERG_SOURCE_TABLE_LOCATION}}', settings iceberg_use_version_hint=true)
            where block_number >= {{.RANGE_START}} and block_number <= {{.RANGE_END}}
            and length(topics) > 0
        ),

        q1 as (
            select 
                q0.* except (topics, data),
                JSONExtract(
                    evm_decode_event(
                        topics::Array(FixedString(32)),
                        data::String,
                        dictGet(evm_abi_decoding, 'fullsigs', topics[1]::String)
                    ),
                    'JSON'
                ) as evt
            from q0
        )


    select
        * except (evt),
        evt.value.signature::String as signature,
        evt.value.fullsig::String as fullsig,
        toJSONString(evt.^value.inputs) as inputs
    from q1
    where evt.error is null
)

{{end}}