-- counts 


WITH comment_count as (
SELECT ct, p.owner_id
FROM
 (SELECT Count(comment_id) as ct, i.repo_id as repo_id
    From `ghtorrent-bq.ght_2017_09_01.issue_comments` ic JOIN `ghtorrent-bq.ght_2017_09_01.issues` i
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
    FROM `ghtorrent-bq.ght_2017_09_01.pull_request_comments` pc JOIN `ghtorrent-bq.ght_2017_09_01.pull_requests` pr
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
    FROM `ghtorrent-bq.ght_2017_09_01.commit_comments` cc JOIN `ghtorrent-bq.ght_2017_09_01.commits` com
    ON cc.commit_id = com.id
    WHERE cc.created_at >= TIMESTAMP("2014-01-01 00:00:00")
    and cc.created_at < TIMESTAMP("2017-08-01 00:00:00")
    GROUP BY repo_id) as d
   JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p
    ON d.repo_id = p.id)
    x
SELECT * 
FROM 
  (SELECT SUM(comment_count.ct) as comment_count, owner_id
  FROM comment_count 
  GROUP BY owner_id)
FULL JOIN `github-181509.GitArchive_Queries.no_pullreq_p_o`
USING (owner_id)





