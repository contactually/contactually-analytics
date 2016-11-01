
-- The name of this model is kind of a misnomer... this model
-- represents a USER in Contactually's SF-backed database

with sf_user as (

    select * from {{ ref('base_contactually_team_account') }}

),

sf_team as (

    select * from {{ ref('base_account') }}

),

sf_record_type as (

    select * from {{ ref('base_record_type') }}

),

plan as (

    select * from {{ ref('base_plan') }}

),

discount_code as (

    select * from {{ ref('base_discount_code') }}

),

payment_account as (

    select * from {{ ref('payment_accounts') }}

),

ct_data as (
    select
        sf_team.id as account_id,
        (case when plan.revenue_type_c = 'Monthly Recurring'
            then (nvl(plan.current_price_c,0) * nvl(sf_team.pay__quantity___c,0) * (1 - nvl(discount_code.percent_off_c,0)/100))
            else (nvl(plan.current_price_c,0)/12 * nvl(sf_team.pay__quantity___c,0) * (1 - nvl(discount_code.percent_off_c,0)/100))
        end) * 12 as ARR
    from sf_team
    left join plan on sf_team.plan___c = plan.id
    left join discount_code on sf_team.discount__code__lookup___c = discount_code.id
)


select
    sf_user.user__id___c as user_id,
    sf_team.team__id___c as team_id,

    ct_data.arr::float / 12.0 as current_mrr,

    sf_record_type.name as account_record_type,
    sf_team.payment__status___c as stripe_status,

    sf_user.connected__accounts___c > 0 as connected_email,
    sf_user.connected__exchange__accounts___c as connected_exchange_accounts,
    sf_user.connected__gmail__accounts___c as connected_gmail_accounts,
    sf_user.connected__imap__accounts___c as connected_imap_accounts,

    sf_user.af_2_0_first_follow_up_sent_date_c is not null as activation_funnel_sent_1_follow_up,
    sf_user.af_2_0_tracked_25_relationships_date_c is not null as activation_funnel_track_25_relationship,

    case when (sf_user.af_2_0_first_sign_in_date_c is not null and 
                sf_user.af_2_0_connected_first_email_date_c is null)
        then '25%'
        when (sf_user.af_2_0_first_sign_in_date_c is not null and 
                sf_user.af_2_0_connected_first_email_date_c is not null and
                sf_user.af_2_0_tracked_25_relationships_date_c is null)
        then '50%'
        when (sf_user.af_2_0_first_sign_in_date_c is not null and 
                sf_user.af_2_0_connected_first_email_date_c is not null and
                sf_user.af_2_0_tracked_25_relationships_date_c is not null and
                sf_user.af_2_0_first_follow_up_sent_date_c is null)
        then '75%'
        when (sf_user.af_2_0_first_sign_in_date_c is not null and 
                sf_user.af_2_0_connected_first_email_date_c is not null and
                sf_user.af_2_0_tracked_25_relationships_date_c is not null and
                sf_user.af_2_0_first_follow_up_sent_date_c is not null)
        then '100%'
        else '0%'
    end as activation_funnel_percent,

    sf_record_type.name in ('Team - SMB Unpaid', 'Team - SMB Paid')
        and sf_team.stage___c != 'Rejected MQL') as is_mql,

    sf_record_type.name in ('Team - SMB Unpaid', 'Team - SMB Paid')
        and sf_team.stage___c != 'Rejected MQL'
        and (coalesce(sf_team.no_call_tasks_reason_c, '') = '') as is_sql,

    case when (sf_record_type.name in ('Partner - Enterprise', 'Team - Enterprise') and
                sf_team.stage___c = 'Active')
        then 'Tier 1'
        when (sf_record_type.name = 'Team - SMB Paid' and
                sf_team.stage___c = 'Active' and
                ct_data.arr >= 1000)
        then 'Tier 2'
        when (sf_record_type.name = 'Team - SMB Paid' and
                sf_team.stage___c = 'Active' and
                ct_data.arr < 1000)
        then 'Tier 3'
        when (sf_record_type.name = 'Team - SMB Unpaid' and
                sf_team.team__trial__end__date___c >= getdate())
        then 'Tier 4'
        else 'Tier 5'
    end as customer_tier,

    case when payment_account.first_charged_at is null then FALSE
         when convert_timezone('UTC', 'America/New_York', sf_user.sign__up__date___c) <= payment_account.first_charged_at) then true
         else false
    end as user_added_after_team_paid

from sf_user
    inner join sf_team
        on sf_user.contactually__team__account___c = sf_team.id

    inner join sf_record_type
        on sf_team.record_type_id = sf_record_type.id

    left outer join ct_data
        on sf_team.id = ct_data.account_id

    left outer join payment_account
        on sf_team.team__payment__account__id___c = payment_account.id

