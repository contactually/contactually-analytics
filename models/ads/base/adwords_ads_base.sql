with max_report as
(
    select
      day,
      campaignid,
      max(_sdc_report_datetime) as max_report_datetime
    from adwords_contactually_ads.campaign_performance_report
    group by 1,2
)
select
  'Google Adwords' as ad_source,
  adwords.day as date,
  adwords.campaign as campaign_name,
  sum(adwords.clicks) as clicks,
  sum(adwords.impressions) as impressions,
  sum(adwords.cost/1000000.00) as cost
from adwords_contactually_ads.campaign_performance_report adwords
  inner join max_report
    on adwords.day = max_report.day
       and adwords.campaignid = max_report.campaignid
       and adwords._sdc_report_datetime = max_report.max_report_datetime
group by 1,2,3