with u as
(
  select
    *
  from {{ ref('users_created') }}
)
select sue.id,
  sue.user_id,
  sue.user_session_id,
  sue.event_type,
  sue.eventable_id,
  sue.created_at,
  sue.updated_at,
  sue.published_at,
  sue.eventable_type,
  sue.uri,
  sue.referer,
  ete.event_name,
  case when sue.created_at <= dateadd(hour,24,u.created_at) then 1 else 0 end as first_day,
  case when (sue.created_at between dateadd(hour,24,u.created_at) and dateadd(hour,168,u.created_at)) then 1 else 0 end as first_week,
  case when (sue.created_at between dateadd(hour,168,u.created_at) and dateadd(hour,336,u.created_at)) then 1 else 0 end as second_week,
  json_extract_path_text( extra_data, 'days_since') as days_since,
  json_extract_path_text( extra_data, 'channel_sent') as channel_sent,
  json_extract_path_text( extra_data, 'template_used') as template_used,
  json_extract_path_text( extra_data, 'introduction') as introduction,
  json_extract_path_text( extra_data, 'content') as content,
  json_extract_path_text( extra_data, 'attachment') as attachment,
  json_extract_path_text( extra_data, 'programs') as programs,
  json_extract_path_text( extra_data, 'track_function') as track_function,
  json_extract_path_text( extra_data, 'sent_now') as sent_now,
  json_extract_path_text( extra_data, 'remote_ip') as remote_ip,
  json_extract_path_text( extra_data, 'product') as product,
  json_extract_path_text( extra_data, 'enabled_features') as enabled_features,
  json_extract_path_text( extra_data, 'quantity') as quantity,
  json_extract_path_text( extra_data, 'username') as username,
  json_extract_path_text( extra_data, 'type') as type,
  json_extract_path_text( extra_data, 'author_id') as author_id,
  json_extract_path_text( extra_data, 'author_email') as author_email,
  json_extract_path_text( extra_data, 'via') as via,
  json_extract_path_text( extra_data, 'user_agent') as user_agent,
  json_extract_path_text( extra_data, 'lead_capture_type') as lead_capture_type,
  json_extract_path_text( extra_data, 'name') as name,
  json_extract_path_text( extra_data, 'reminder_days') as reminder_days,
  json_extract_path_text( extra_data, 'program_assigned') as program_assigned,
  json_extract_path_text( extra_data, 'device') as device,
  json_extract_path_text( extra_data, 'template_type') as template_type,
  json_extract_path_text( extra_data, 'assigned_to_bucket') as assigned_to_bucket,
  json_extract_path_text( extra_data, 'number_of_templates') as number_of_templates,
  json_extract_path_text( extra_data, 'plan') as plan,
  json_extract_path_text( extra_data, 'plan_was') as plan_was,
  json_extract_path_text( extra_data, 'revenue') as revenue,
  json_extract_path_text( extra_data, 'payment_method') as payment_method,
  json_extract_path_text( extra_data, 'filter') as filter,
  json_extract_path_text( extra_data, 'included_bucket') as included_bucket,
  json_extract_path_text( extra_data, 'preset') as preset,
  json_extract_path_text( extra_data, 'accessible_to') as accessible_to,
  json_extract_path_text( extra_data, 'team_size') as team_size,
  json_extract_path_text( extra_data, 'shared_with_user') as shared_with_user,
  json_extract_path_text( extra_data, 'bucket_share_count') as bucket_share_count,
  json_extract_path_text( extra_data, 'bucket_id') as bucket_id,
  json_extract_path_text( extra_data, 'permission_level') as permission_level,
  json_extract_path_text( extra_data, 'pipeline_share_count') as pipeline_share_count,
  json_extract_path_text( extra_data, 'pipeline_id') as pipeline_id,
  json_extract_path_text( extra_data, 'physical_message_stationery_id') as physical_message_stationery_id,
  json_extract_path_text( extra_data, 'value') as value,
  json_extract_path_text( extra_data, 'assigned_to_id') as assigned_to_id,
  json_extract_path_text( extra_data, 'tag_quantity') as tag_quantity,
  json_extract_path_text( extra_data, 'tag_name') as tag_name,
  json_extract_path_text( extra_data, 'private') as private,
  json_extract_path_text( extra_data, 'duration') as duration,
  json_extract_path_text( extra_data, 'reminder') as reminder,
  json_extract_path_text( extra_data, 'response') as response,
  json_extract_path_text( extra_data, 'quantity_rejected') as quantity_rejected,
  json_extract_path_text( extra_data, 'contact_program_id') as contact_program_id,
  json_extract_path_text( extra_data, 'contacts_count') as contacts_count
from postgres_public_production_main_public.stats_user_events as sue
  left join event_type_enum as ete
    on ete.enum = sue.event_type
  inner join u
    on u.id = sue.user_id
{% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
  where sue.id not in (select id from {{ this }})
{% endif %}