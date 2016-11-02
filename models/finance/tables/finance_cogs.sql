
with cogs_classes as (

    select * from {{ ref('quickbooks_parent_class_map') }}
    where top_level_class_id = '3100000000000559847'

),

ledger as (

    select * from {{ ref('quickbooks_general_ledger') }}

),

accounts as (

  select * from {{ ref('quickbooks_accounts_xf') }}

)

select
    ledger.txn_date,
    ledger.adj_amount,
    accounts.fully_qualified_name as account_name,
    cogs_classes.class_name
from ledger
join cogs_classes on ledger.class_id = cogs_classes.class_id
join accounts on accounts.id = ledger.account_id
where accounts."statement" = 'is'
and accounts.active = TRUE
