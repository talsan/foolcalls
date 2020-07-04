CREATE EXTERNAL TABLE IF NOT EXISTS qcdb.fool_call_index (
         cid string,
         call_url string,
         publication_author string,
         publication_time timestamp,
         call_title string,
         call_subtitle string,
         period_end date,
         ticker string,
         ticker_exchange string,
         company_name string,
         fool_company_id string,
         fiscal_period_year string,
         fiscal_period_qtr string,
         call_short_title string,
         call_date date,
         call_time timestamp
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://foolcalls/stage=structured/version=202006.1'

CREATE EXTERNAL TABLE IF NOT EXISTS qcdb.fool_call_statements_nested (
  cid string,
  call_transcript array < struct < statement_num:int,
         section:string,
         statement_type:string,
         speaker:string,
         `role`:string,
         affiliation:string,
  text:string
  > >
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://foolcalls/stage=structured/version=202006.1'


--VIEW
CREATE OR REPLACE VIEW fool_call_statements AS
SELECT cid,

statements.statement_num as statement_num,
statements.section as section,
statements.statement_type as statement_type,
statements.speaker as speaker,
statements.role as "role",
statements.affiliation as affiliation

FROM qcdb.fool_call_statements_nested
cross join unnest(call_transcript) as t(statements)


CREATE EXTERNAL TABLE IF NOT EXISTS qcdb.fool_call_speakers_nested (
  cid string,
  participants struct <
  management: array < struct < speaker:string,role:string,affiliation:string>>,
  analysts: array < struct < speaker:string,`role`:string,affiliation:string>>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://foolcalls/stage=structured/version=202006.1'

CREATE OR REPLACE VIEW fool_call_speakers AS
SELECT cid,
management.speaker as speaker,
management.role as "role",
management.affiliation as affiliation

FROM qcdb.fool_call_speakers_nested
cross join unnest(participants.management) as t(management)

union

select cid,
analysts.speaker as speaker,
analysts.role as "role",
analysts.affiliation as affiliation

FROM qcdb.fool_call_speakers_nested
cross join unnest(participants.analysts) as t(analysts)


