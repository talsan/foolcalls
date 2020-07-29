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