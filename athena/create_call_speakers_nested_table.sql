CREATE EXTERNAL TABLE IF NOT EXISTS qcdb.fool_call_speakers_nested (
  cid string,
  participants struct <
  management: array < struct < speaker:string,role:string,affiliation:string>>,
  analysts: array < struct < speaker:string,`role`:string,affiliation:string>>>
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' LOCATION 's3://fool-calls/state=structured/version=202007.1'