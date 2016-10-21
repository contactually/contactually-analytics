
with cogs_classes as (

    select * from {{ ref('quickbooks_classes') }}
    where id in ('3100000000000547352', '3100000000000547361', '3100000000000559382')

),

ledger as (

    select * from {{ ref('quickbooks_general_ledger') }}

),

accounts as (

  select * from {{ ref('quickbooks_accounts_xf') }}

)

select
    ledger.txn_date,
    ledger.amount,
    cogs_classes.fully_qualified_name
from ledger
join cogs_classes on ledger.class_id = cogs_classes.id
join accounts on accounts.id = ledger.account_id
where accounts."statement" = 'is'
and accounts.active = TRUE
