select
  id,
  journal_entry_id as source_key,
  amount,
  description,
  account as journalentrylinedetail__accountref__value,
  class as journalentrylinedetail__classref__value,
  posting_type as journalentrylinedetail__postingtype
from quickbooks.journal_entry_line