with salesforce_contactually_user as (

    select * from salesforce."_contactually__account___c"

),

salesforce_contactually_team as (

    select * from salesforce."_account" 

),

salesforce_record_type as (

    select * from salesforce."_record_type" 

),

plan as (

    select * from salesforce."_product_2"

),

discount_code as (

    select * from salesforce."_coupon___c"

),

ct_data as (
    select
    salesforce_contactually_team.id as account_id,
    (case when plan.revenue_type_c = 'Monthly Recurring'
        then (nvl(plan.current_price_c,0) * nvl(salesforce_contactually_team.pay__quantity___c,0) * (1 - nvl(discount_code.percent_off_c,0)/100))
        else (nvl(plan.current_price_c,0)/12 * nvl(salesforce_contactually_team.pay__quantity___c,0) * (1 - nvl(discount_code.percent_off_c,0)/100))
    end)*12 as ARR
    from salesforce_contactually_team
    left join plan on salesforce_contactually_team.plan___c = plan.id
    left join discount_code on salesforce_contactually_team.discount__code__lookup___c = discount_code.id
)


select
    salesforce_contactually_user.user__id___c as User_ID,
    salesforce_contactually_team.team__id___c as Team_ID,
    salesforce_record_type.name as Account_Record_Type,
    case when salesforce_contactually_user.connected__accounts___c > 0
        then 'Yes'
        else 'No'
        end as Connected_Email,
    salesforce_contactually_user.connected__exchange__accounts___c as Connected_Exchange_Accounts,
    salesforce_contactually_user.connected__gmail__accounts___c as Connected_Gmail_Accounts,
    salesforce_contactually_user.connected__imap__accounts___c as Connected_IMAP_Accounts,
    case when salesforce_contactually_user.af_2_0_first_follow_up_sent_date_c is not null
        then 'Yes'
        else 'No'
        end as Activation_Funnel_Sent_1_Follow_Up,
    case when salesforce_contactually_user.af_2_0_tracked_25_relationships_date_c is not null
        then 'Yes'
        else 'No'
        end as Activation_Funnel_Track_25_Relationship,
    case when (salesforce_contactually_user.af_2_0_first_sign_in_date_c is not null and 
                salesforce_contactually_user.af_2_0_connected_first_email_date_c is null)
        then '25%'
        when (salesforce_contactually_user.af_2_0_first_sign_in_date_c is not null and 
                salesforce_contactually_user.af_2_0_connected_first_email_date_c is not null and
                salesforce_contactually_user.af_2_0_tracked_25_relationships_date_c is null)
        then '50%'
        when (salesforce_contactually_user.af_2_0_first_sign_in_date_c is not null and 
                salesforce_contactually_user.af_2_0_connected_first_email_date_c is not null and
                salesforce_contactually_user.af_2_0_tracked_25_relationships_date_c is not null and
                salesforce_contactually_user.af_2_0_first_follow_up_sent_date_c is null)
        then '75%'
        when (salesforce_contactually_user.af_2_0_first_sign_in_date_c is not null and 
                salesforce_contactually_user.af_2_0_connected_first_email_date_c is not null and
                salesforce_contactually_user.af_2_0_tracked_25_relationships_date_c is not null and
                salesforce_contactually_user.af_2_0_first_follow_up_sent_date_c is not null)
        then '100%'
        else '0%'
        end as Activation_Funnel_Percent,
    case when (salesforce_record_type.name in ('Team - SMB Unpaid', 'Team - SMB Paid') and
                salesforce_contactually_team.stage___c != 'Rejected MQL')
        then 'Yes'
        else 'No'
        end as Is_MQL,
    case when (salesforce_record_type.name in ('Team - SMB Unpaid', 'Team - SMB Paid') and
                salesforce_contactually_team.stage___c != 'Rejected MQL' and 
                salesforce_contactually_team.no_call_tasks_reason_c = '')
        then 'Yes'
        else 'No'
        end as Is_SQL, /*also criteria for "phone has US/Canada area code which is super complicated and basically validates the phone #*/
        /*Is SQL (with email account) just looks to see if Is_SQL = Yes and if one of the connected accounts fields is true*/
    salesforce_contactually_team.payment__status___c as Stripe_Status,
    case when (salesforce_record_type.name in ('Partner - Enterprise', 'Team - Enterprise') and
                salesforce_contactually_team.stage___c = 'Active')
        then 'Tier 1'
        when (salesforce_record_type.name = 'Team - SMB Paid' and
                salesforce_contactually_team.stage___c = 'Active' and
                ct_data.arr >= 1000)
        then 'Tier 2'
        when (salesforce_record_type.name = 'Team - SMB Paid' and
                salesforce_contactually_team.stage___c = 'Active' and
                ct_data.arr < 1000)
        then 'Tier 3'
        when (salesforce_record_type.name = 'Team - SMB Unpaid' and
                salesforce_contactually_team.team__trial__end__date___c >= GETDATE())
        then 'Tier 4'
        else 'Tier 5'
        end as Customer_Tier
    from
    salesforce_contactually_user
    inner join salesforce_contactually_team
        on salesforce_contactually_user.contactually__team__account___c = salesforce_contactually_team.id
    inner join salesforce_record_type
        on salesforce_contactually_team.record_type_id = salesforce_record_type.id
    left join ct_data
        on salesforce_contactually_team.id = ct_data.account_id

