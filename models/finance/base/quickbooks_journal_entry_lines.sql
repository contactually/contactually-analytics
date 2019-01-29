select
  index as id,
  journal_entry_id as source_key,
  amount,
  description,
  account_id as journalentrylinedetail__accountref__value,
  class_id as journalentrylinedetail__classref__value,
  posting_type as journalentrylinedetail__postingtype
from quickbooks.journal_entry_line