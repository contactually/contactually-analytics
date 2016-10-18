
with cogs_classes as (

    select * from {{ ref('quickbooks_classes') }}
    where id in ('3100000000000547352', '3100000000000547361', '3100000000000559382')

),

ledger as (

    select * from {{ ref('quickbooks_general_ledger') }}

)


select
    ledger.txn_date,
    ledger.amount,
    cogs_classes.fully_qualified_name,
from ledger
join cogs_classes on ledger.class_id = cogs_classes.id
where ledger.transaction_type = 'debit'
