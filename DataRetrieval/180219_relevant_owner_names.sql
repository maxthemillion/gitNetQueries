-- reports a list of owners which fulfill certain filter criteria:
-- 1. >= 2000 cumulated comments on all repositories
-- 2. >= 500 commits or >= 100 pull requests   

WITH 
-- counting pull requests per owner
pull_request_count_aggregated as (
  SELECT COUNT(pull_request_count.id) as pr_count, p.owner_id as owner_id
  FROM  
    (SELECT pull.id, pull.base_repo_id as repo_id
    FROM `ghtorrent-bq.ght_2017_09_01.pull_requests` as pull JOIN `ghtorrent-bq.ght_2017_09_01.pull_request_history` as history
    ON pull.id = history.pull_request_id
    WHERE history.action = "opened" 
    and created_at >= TIMESTAMP("2014-01-01 00:00:00")
    and created_at < TIMESTAMP("2017-08-01 00:00:00")) as pull_request_count
  JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p  
  ON pull_request_count.repo_id = p.id
  GROUP BY owner_id),

-- counting commits per owner 
commit_count_aggregated as (
  SELECT COUNT(commit_count.id) as commit_count, p.owner_id as owner_id
  FROM 
  (SELECT commits.id, commits.project_id as repo_id
    FROM `ghtorrent-bq.ght_2017_09_01.commits` as commits 
    WHERE commits.created_at >= TIMESTAMP("2014-01-01 00:00:00")
    and commits.created_at < TIMESTAMP("2017-08-01 00:00:00")) as commit_count
  JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p
  ON commit_count.repo_id = p.id
  GROUP BY owner_id),

-- counting comments per repository
comment_count as (
  SELECT ct, p.owner_id
  FROM
    (SELECT Count(comment_id) as ct, i.repo_id as repo_id
     FROM `ghtorrent-bq.ght_2017_09_01.issue_comments` as ic JOIN `ghtorrent-bq.ght_2017_09_01.issues` as i
     ON ic.issue_id = i.id
     WHERE ic.created_at >= TIMESTAMP("2014-01-01 00:00:00")
     and ic.created_at < TIMESTAMP("2017-08-01 00:00:00")
     GROUP BY repo_id) as b 
  JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p
  ON b.repo_id = p.id
UNION ALL
  SELECT ct, p.owner_id
  FROM
    (SELECT COUNT(comment_id) as ct, pr.base_repo_id as repo_id
     FROM `ghtorrent-bq.ght_2017_09_01.pull_request_comments` as pc JOIN `ghtorrent-bq.ght_2017_09_01.pull_requests` as pr
     ON pc.pull_request_id = pr.id
     WHERE pc.created_at >= TIMESTAMP("2014-01-01 00:00:00")
     and pc.created_at < TIMESTAMP("2017-08-01 00:00:00")
     GROUP BY repo_id) as c
  JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p
  ON c.repo_id = p.id
UNION ALL
  SELECT ct, p.owner_id
  FROM   
    (SELECT COUNT(comment_id) as ct, com.project_id as repo_id
     FROM `ghtorrent-bq.ght_2017_09_01.commit_comments` as cc JOIN `ghtorrent-bq.ght_2017_09_01.commits` as com
     ON cc.commit_id = com.id
     WHERE cc.created_at >= TIMESTAMP("2014-01-01 00:00:00")
     and cc.created_at < TIMESTAMP("2017-08-01 00:00:00")
     GROUP BY repo_id) as d
   JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p
   ON d.repo_id = p.id),

-- aggregating comment counts
comment_count_aggregated as (
  SELECT SUM(comment_count.ct) as comment_count, owner_id
  FROM comment_count 
  GROUP BY owner_id
),

-- joining all subtables on owner_id   
joined_res as (
SELECT * 
FROM 
comment_count_aggregated FULL JOIN pull_request_count_aggregated
USING (owner_id)
FULL JOIN commit_count_aggregated
USING (owner_id)
WHERE comment_count_aggregated.comment_count > 2000 AND 
  (commit_count_aggregated.commit_count > 500 
  OR pull_request_count_aggregated.pr_count > 100))
  
-- joining with users to retrieve owner names
SELECT joined_res.owner_id as ght_owner_id, 
	u.login as owner_login, 
	comment_count, 
	pr_count,
	commit_count
FROM `ghtorrent-bq.ght_2017_09_01.users` u JOIN joined_res
ON u.id = joined_res.owner_id