with markup as(
    select * ,
    first_value(customer_id)
    over(partition by company_name, contact_name
    order by company_name
    rows between unbounded preceding and unbounded following) as result
    from {{source('sources','customers')}}
    -- criando uma coluna result com uma função de janela, para identificar os clientes duplicados
), 
removed as(
    select distinct result from markup
    -- retirando os clientes duplicados
), 
final as (
    select * from {{source('sources','customers')}} where customer_id in (select result from removed)
    -- buscando os ids da tabela inicial onde os ids são iguais a tbl removed
)

select * from final