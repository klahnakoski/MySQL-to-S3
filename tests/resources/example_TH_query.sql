describe 
select 
  m.bug_id,
  count(1) intermittent, 
  sum(CASE WHEN n.failure_classification_id = 7  THEN 1 ELSE 0 END) is_auto 
from
  job j 
join
  bug_job_map m on m.job_id = j.id
left join
  job_note n on n.job_id = j.id and n.failure_classification_id = 7 # autoclassify
where
  j.failure_classification_id = 4 AND # intermittent
  j.start_time> CURDATE() - INTERVAL 7 DAY AND
  j.repository_id <> 4 # NOT TRY
group by
  m.bug_id
;
  