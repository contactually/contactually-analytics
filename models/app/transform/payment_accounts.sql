
with payment_accounts as (

    select * from {{ ref('base_payment_accounts') }}

), partners as (

    select id, coalesce(partner_display_name, code_name) as partner_name from {{ ref('base_partners') }}

),


select
    pa.id,
    plan,
    coupon,
    quantity,
    created_at,
    updated_at,
    last_charged_at,
    partners.partner_name

from payment_accounts pa
    left outer join partners on partners.id = pa.partner_id
