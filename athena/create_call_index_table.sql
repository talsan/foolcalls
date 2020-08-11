CREATE EXTERNAL TABLE IF NOT EXISTS qcdb.fool_call_index (
         cid string,
         call_url string,
         publication_author string,
         publication_time_published string,
         publication_time_updated string,
         call_title string,
         call_subtitle string,
         period_end string,
         ticker string,
         ticker_exchange string,
         company_name string,
         fool_company_id string,
         fiscal_period_year string,
         fiscal_period_qtr string,
         call_short_title string,
         call_date string,
         call_time string,
         duration_minutes string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://fool-calls/state=structured/version=202007.1'