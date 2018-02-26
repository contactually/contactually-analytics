with subscription_ids as
(
    select
      subscription.id as max_version_id
    from zuora._subscription subscription
      inner join
      (
        select
          subscription.original_id,
          max(subscription.version) as max_version
        from zuora._subscription subscription
        where not subscription.is_deleted
        group by 1
      )max_version
        on subscription.original_id = max_version.original_id
           and subscription.version = max_version.max_version
    where not subscription.is_deleted
  --and subscription.account_id = '2c92a0fe607ce0bf0160b875f27e06c1'
), base_charges as
(
    select distinct
      subscription.account_id,
      subscription.id,
      subscription.version,
      subscription.status,
      subscription.name as subscription_name,
      subscription.original_id,
      subscription.cancelled_date,
      rate_plan_charge.effective_start_date,
      rate_plan_charge.effective_end_date,
      rate_plan_charge.charge_number,
      rate_plan_charge.charge_model,
      case when rate_plan_charge.charge_model not in ('Discount-Fixed Amount', 'Discount-Percentage') then rate_plan_charge.name end as plan_name,
      case when rate_plan_charge.charge_model in ('Discount-Fixed Amount', 'Discount-Percentage') then rate_plan_charge.name end as discount_name,
      case when rate_plan_charge.charge_model not in ('Discount-Fixed Amount', 'Discount-Percentage') then rate_plan_charge.quantity end as quantity,
      case when rate_plan_charge.charge_model not in ('Discount-Fixed Amount', 'Discount-Percentage') then rate_plan_charge.mrr end as mrr,
      tier.discount_percentage,
      case when rate_plan_charge.charge_model = 'Discount-Fixed Amount' and rate_plan_charge.billing_period = 'Annual'
        then tier.discount_amount/12
      when rate_plan_charge.charge_model = 'Discount-Fixed Amount' and rate_plan_charge.billing_period = 'Quarter'
        then tier.discount_amount/3
      else tier.discount_amount end as discount_amount
    from zuora._rate_plan_charge rate_plan_charge
      inner join zuora._rate_plan_charge_tier tier
        on rate_plan_charge.id = tier.rate_plan_charge_id
      inner join zuora._subscription subscription
        on rate_plan_charge.subscription_id = subscription.id
      inner join subscription_ids
        on subscription.id = subscription_ids.max_version_id
    where not rate_plan_charge.is_deleted
          and rate_plan_charge.effective_start_date != coalesce(rate_plan_charge.effective_end_date, '2099-12-31')
          and rate_plan_charge.charge_type = 'Recurring'
          and not subscription.is_deleted
    order by subscription.account_id, rate_plan_charge.effective_start_date, rate_plan_charge.effective_end_date
), subscription_versions as (
    select
      subscription.account_id,
      subscription.original_id,
      subscription.id,
      subscription.version,
      subscription.subscription_version_amendment_id,
      subscription.subscription_start_date,
      subscription.subscription_end_date,
      subscription.cancelled_date,
      subscription.status,
      case when subscription.arrreporting_date_c is not null
        then subscription.arrreporting_date_c
      when amendment.id is not null and amendment.contract_effective_date <= coalesce(subscription.subscription_end_date, '2099-12-31')
        then amendment.contract_effective_date
      when amendment.id is not null and amendment.contract_effective_date > subscription.subscription_end_date
        then subscription.subscription_end_date
      else subscription.subscription_start_date
      end as version_effective_date,
      amendment.contract_effective_date,
      amendment.code,
      amendment.type
    from zuora._subscription subscription
      left join zuora.amendment amendment
        on subscription.subscription_version_amendment_id = amendment.id
    where not subscription.is_deleted
          and (subscription.incorrect_cancellation_date_c != 'True' or subscription.incorrect_cancellation_date_c is null)
    --and subscription.account_id = '2c92a0fc5455af6b0154611af1992418'
    order by subscription.version
), versions_reduced as
(
    select
      subscription_versions.account_id,
      subscription_versions.original_id,
      subscription_versions.id,
      subscription_versions.subscription_start_date,
      subscription_versions.subscription_end_date,
      subscription_versions.cancelled_date,
      subscription_versions.version,
      subscription_versions.version_effective_date,
      subscription_versions.status,
      subscription_versions.code,
      subscription_versions.type,
      sum(base_charges.mrr) as mrr,
      sum(base_charges.quantity) as quantity,
      sum(base_charges.discount_amount) as discount_amount,
      sum(base_charges.discount_percentage) as discount_percentage,
      listagg(distinct base_charges.plan_name, ',') as plans,
      listagg(distinct base_charges.discount_name, ',') as discounts
    from subscription_versions
      inner join base_charges
        on subscription_versions.original_id = base_charges.original_id
           and ((subscription_versions.version_effective_date >= base_charges.effective_start_date and (subscription_versions.version_effective_date < base_charges.effective_end_date or base_charges.effective_end_date is null))
                or (base_charges.status = 'Cancelled' and subscription_versions.type = 'Cancellation' /*and subscription_versions.version_effective_date = base_charges.effective_end_date*/)
             /*or (base_charges.effective_start_date > subscription_versions.version_effective_date and base_charges.effective_start_date < subscription_versions.version_end_date)*/)
    group by 1,2,3,4,5,6,7,8,9,10,11
    order by subscription_versions.account_id, subscription_versions.version_effective_date
), max_versions as
(
    select
      account_id,
      original_id,
      subscription_start_date,
      subscription_end_date,
      cancelled_date,
      version_effective_date,
      mrr,
      quantity,
      discount_amount,
      discount_percentage,
      plans,
      discounts,
      max(version) as version
    from versions_reduced
    group by 1,2,3,4,5,6,7,8,9,10,11,12
), subscription_base as
(
    select
      max_versions.account_id,
      max_versions.original_id,
      max_versions.subscription_start_date,
      max_versions.subscription_end_date,
      max_versions.cancelled_date,
      max_versions.version_effective_date,
      max_versions.mrr as mrr_no_discount,
      max_versions.quantity,
      max_versions.discount_amount,
      max_versions.discount_percentage,
      (max_versions.mrr - coalesce(max_versions.discount_amount,0)) * (1 - coalesce(max_versions.discount_percentage,0)/100) as mrr,
      max_versions.plans,
      max_versions.discounts,
      max_versions.version,
      row_number() over (partition by max_versions.original_id order by max_versions.version_effective_date) as version_index,
      subscription.status,
      subscription.name as subscription_name,
      subscription.id,
      subscription_rank.subscription_rank,
      case when subscription.status = 'Cancelled' and lead(max_versions.version_effective_date, 1) over (partition by max_versions.account_id order by max_versions.subscription_start_date, max_versions.version_effective_date) = max_versions.cancelled_date
        then 'Renewal - Cancellation'
      when lag(max_versions.cancelled_date, 1) over (partition by max_versions.account_id order by max_versions.subscription_start_date, max_versions.version_effective_date) = max_versions.version_effective_date
        then 'Renewal - New Subscription'
      end as probable_renewal,
      subscription.arrreporting_date_c as arr_reporting_date
    from max_versions
      inner join zuora._subscription subscription
        on max_versions.account_id = subscription.account_id
           and max_versions.original_id = subscription.original_id
           and max_versions.version = subscription.version
           and not subscription.is_deleted
      left join
      (
        select
          original_id,
          account_id,
          row_number() over (partition by account_id order by contract_effective_date) as subscription_rank
        from (
          select distinct
            account_id,
            original_id,
            contract_effective_date
          from zuora._subscription
        )
      ) subscription_rank
        on subscription.original_id = subscription_rank.original_id
    order by max_versions.account_id, max_versions.subscription_start_date, max_versions.version_effective_date
), base_final as
(
    select
      id,
      version,
      status,
      subscription_rank,
      version_effective_date as start_date,
      case when status = 'Cancelled' and arr_reporting_date is not null
        then arr_reporting_date
      when status = 'Cancelled' and arr_reporting_date is null
        then subscription_end_date
      else lead(version_effective_date, 1) over (partition by account_id order by subscription_rank, version_index)
      end as end_date,
      subscription_name,
      original_id as subscription_id,
      subscription_start_date as subscription_start,
      subscription_end_date as subscription_end,
      cancelled_date,
      account_id,
      plans,
      case when status = 'Cancelled' then 0
      else quantity
      end as seats,
      discounts,
      case when status = 'Cancelled' or mrr_no_discount < 0 then 0
      else mrr_no_discount
      end as mrr_no_discount,
      case when status = 'Cancelled' or mrr < 0 then 0
      else mrr
      end as mrr,
      discount_amount,
      discount_percentage,
      version_index as index
    from subscription_base
)
select
  teams.id                                                                                  as team_id,
  base_final.mrr/case when base_final.seats = 0 then 1 else base_final.seats end            as zuora_mrr_per_seat,
  base_final.plans                                                                          as zuora_plan_name,
  base_final.seats                                                                          as zuora_seat_quantity,
  z_account.bill_cycle_day                                                                  as zuora_bill_cycle_day,
  z_account.last_invoice_date                                                               as zuora_last_invoice_date,
  z_payment_method.num_consecutive_failures                                                 as zuora_num_consecutive_failures,
  z_payment_method.last_transaction_date_time                                               as zuora_last_transaction_date_time,
  z_payment_method.last_transaction_status                                                  as zuora_last_transaction_status,
  z_payment_method.type                                                                     as zuora_payment_method_type,
  z_contact.first_name + ' ' + z_contact.last_name                                          as zuora_billing_contact_name,
  z_contact.work_email                                                                      as zuora_billing_contact_work_email,
  z_contact.work_phone                                                                      as zuora_billing_contact_work_phone,
  z_contact.personal_email                                                                  as zuora_billing_contact_personal_email
from base_final
  inner join
  (
    select
      account_id,
      max(subscription_rank) as max_subscription_rank,
      max(index) as max_index
    from base_final
    group by 1
  )max_version
    on base_final.account_id = max_version.account_id
       and base_final.subscription_rank = max_version.max_subscription_rank
       and base_final.index = max_version.max_index
  inner join postgres_public_production_main_public.payment_accounts pa
    on base_final.account_id = pa.zuora_account_id
  inner join zuora._account z_account
    on base_final.account_id = z_account.id
  inner join postgres_public_production_main_public.teams
    on pa.id = teams.payment_account_id
  left join zuora._payment_method z_payment_method
    on z_account.default_payment_method_id = z_payment_method.id
  left join zuora._contact z_contact
    on z_account.bill_to_contact_id = z_contact.id