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