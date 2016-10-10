
with cogs_accounts as (

    select *
    from {{ ref('quickbooks_accounts') }}
        where classification = 'Expense'
        and "type" = 'Cost of Goods Sold'
        and active = TRUE

),

ledger as (

    select * from {{ ref('quickbooks_general_ledger') }}

),

classes as (

    select * from {{ ref('quickbooks_classes') }}

)

select l.txn_date as "date", c.id, c.name, l.amount, classes.name as class_name
from cogs_accounts c
join ledger l on c.id = l.account_id
join classes on classes.id = l.class_id
where l.transaction_type = 'debit'
