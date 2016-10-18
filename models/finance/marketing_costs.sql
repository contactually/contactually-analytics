
with marketing_classes as (

    select * from {{ ref('quickbooks_parent_class_map') }}
    where top_level_class_id = '3100000000000559846'

),

ledger as (

    select * from {{ ref('quickbooks_general_ledger') }}

)


select
    ledger.txn_date,
    ledger.amount,
    marketing_classes.class_name
from ledger
join marketing_classes on ledger.class_id = marketing_classes.class_id
where ledger.transaction_type = 'debit'
