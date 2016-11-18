
-- We'll primarily be joining this to the teams table
{{
    config (
        materialized ='table',
        sort = ['user_created_at'],
        dist = 'user_id'
    )
}}

with users as (

    select * from {{ ref('app_users') }}

),

sf_users as (

    select * from {{ ref('sf_contactually_team_account') }}

),

contacts as (

    select * from {{ ref('sf_contacts') }}

)

select

    -- id fields
    users.id                  as user_id,
    users.team_id             as team_id,

    sf_users.contact_id       as contact_id,
    sf_users.account_id       as account_id,

    -- user fields
    users.created_at          as user_created_at,
    users.updated_at          as user_updated_at,
    users.first_name          as user_first_name,
    users.last_name           as user_last_name,
    users.email               as user_email,
    users.sign_in_count       as user_sign_in_count,
    users.current_sign_in_at  as user_current_sign_in_at,
    users.last_sign_in_at     as user_last_sign_in_at,
    users.last_seen_at        as user_last_seen_at,
    users.admin               as user_admin,
    --users.dead                as user_dead, -- too morbid

    -- sf_contactually_team_account fields
    current_mrr,
    current_arr,
    account_record_type,
    stripe_status,
    connected_email,
    connected_exchange_accounts,
    connected_gmail_accounts,
    connected_imap_accounts,
    activation_funnel_sent_1_follow_up
    activation_funnel_track_25_relationship,
    activation_funnel_percent,

    last_in_app_message_sent_at,
    last_contact_bucketed_at,
    last_activity,
    total_contacts,
    bucket_count,
    count_email_templates_in_account,
    number_of_pipelines,
    number_of_programs,
    number_of_times_signed_in,
    time_zone,
    in_app_messages_sent,
    invites_sent,
    invites_redeemed,
    joined_by_invite,

    recent_nps_score,
    recent_nps_submission_date,

    is_mql,
    is_sql,
    is_sql_with_email_account,
    customer_tier,
    user_added_after_team_paid,

    -- contact fields
    contact_mailing_street             as contact_mailing_street,
    contact_mailing_city               as contact_mailing_city,
    contact_mailing_state              as contact_mailing_state,
    contact_mailing_postal_code        as contact_mailing_postal_code,
    contact_mailing_country            as contact_mailing_country,
    contact_phone                      as contact_phone,
    contact_title                      as contact_title,
    contact_area_code                  as contact_area_code,
    contact_is_outside_usa_and_canada  as contact_is_outside_usa_and_canada,
    contact_phone_has_us_ca_area_code  as contact_phone_has_us_ca_area_code

from users
    join sf_users on users.id = sf_users.user_id
    join contacts on contacts.contact_id = sf_users.contact_id
