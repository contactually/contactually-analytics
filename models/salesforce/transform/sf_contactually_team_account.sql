
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

    select * from {{ ref('app_payment_accounts') }}

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

    sf_user.contact___c as contact_id,
    sf_team.id          as account_id,

    ct_data.arr::float as current_arr,
    ct_data.arr::float / 12.0 as current_mrr,

    sf_record_type.name as account_record_type,
    sf_team.payment__status___c as stripe_status,

    sf_user.date__last__in__app__message__was__sent___c as last_in_app_message_sent_at,
    sf_user.date__last__contact__was__bucketed___c as last_contact_bucketed_at,
    sf_user.last__modified__user__activity___c as last_activity,
    sf_user.total__contacts___c as total_contacts,
    sf_user.bucket__count___c as bucket_count,
    sf_user.number__of__email__templates__in__account___c as count_email_templates_in_account,
    sf_user.number__of__pipelines___c as number_of_pipelines,
    sf_user.number__of__programs___c as number_of_programs,
    sf_user.total__number__of__times__signed__in___c as number_of_times_signed_in,
    sf_user.time__zone___c as time_zone,
    sf_user.in__app__messages__sent___c as in_app_messages_sent,
    sf_user.invites__sent___c as invites_sent,
    sf_user.invites__redeemed___c as invites_redeemed,
    sf_user.joined__by__invite___c as joined_by_invite,

    sf_user.current__nps__score___c as recent_nps_score,
    sf_user.recent__nps__submission__date___c as recent_nps_submission_date,

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
        and sf_team.stage___c != 'Rejected MQL' as is_mql,

    sf_record_type.name in ('Team - SMB Unpaid', 'Team - SMB Paid')
        and sf_team.stage___c != 'Rejected MQL'
        and (coalesce(sf_team.no_call_tasks_reason_c, '') = '') as is_sql,

    sf_record_type.name in ('Team - SMB Unpaid', 'Team - SMB Paid')
        and sf_team.stage___c != 'Rejected MQL'
        and (coalesce(sf_team.no_call_tasks_reason_c, '') = '')
        and (
            sf_user.connected__accounts___c > 0 or
            sf_user.connected__exchange__accounts___c or
            sf_user.connected__gmail__accounts___c or
            sf_user.connected__imap__accounts___c
        ) as is_sql_with_email_account,

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
         when convert_timezone('UTC', 'America/New_York', sf_user.sign__up__date___c) <= payment_account.first_charged_at then true
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

