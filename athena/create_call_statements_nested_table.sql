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
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://fool-calls/state=structured/version=202007.1'