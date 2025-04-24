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
                subtraces,
                trace_address,
                error,
                call_type,
                from,
                gas,
                to,
                value,
                gas_used,
                input,
                output
            from iceberg('{{.ICEBERG_SOURCE_TABLE_LOCATION}}')
            where block_number >= {{.RANGE_START}} and block_number <= {{.RANGE_END}}
            and length(input) >= 4
        ),

        q1 as (
            select 
                q0.* except (input, output),
                JSONExtract(
                    evm_decode_call(
                        input::String,
                        output::String,
                        dictGet(evm_abi_decoding, 'fullsigs', left(input, 4)::String)
                    ),
                    'JSON'
                ) as call
            from q0
        )

    select
        * except (call),
        call.value.signature::String as signature,
        call.value.fullsig::String as fullsig,
        toJSONString(call.^value.inputs) as inputs,
        toJSONString(call.^value.outputs) as outputs
    from q1
    where call.error is null
)

{{end}}