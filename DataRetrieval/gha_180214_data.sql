-- retrieves required event data from GHA

WITH tabRange as (
  SELECT data.*, ght_repo_id, forked_from as ght_forked_from
  FROM
    (SELECT *
    FROM `githubarchive.month.2016*`
    WHERE _TABLE_SUFFIX IN ("01")
    and type IN ('CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent', 'IssuesEvent',
    'PullRequestEvent', 'MemberEvent', 'CreateEvent', 'ReleaseEvent')) as data
    JOIN `github-181509.GitArchive_Queries.selected_repos` selected_repos
    ON data.repo.name = selected_repos.full_name
),



standard_info as(
SELECT 
  id as event_id,
  type, 
  (SELECT owner_name FROM (SELECT SUBSTR(repo.name, 1, (SELECT STRPOS(repo.name, '/') - 1)) as owner_name)) as owner_name, 
  repo.name as repo_name,
  repo.id as repo_id,
  actor.id as actor_id, 
  actor.login as actor_login, 
  org.id as org_id, 
  org.login as org_login, 
  created_at as event_time,
  ght_repo_id,
  ght_forked_from

FROM tabRange  
), 

extra_info_cce as (
SELECT 
  event_id, action, TO_JSON_STRING(cce) as other
  FROM (
    SELECT id as event_id,
           "created" as action,
           (SELECT AS STRUCT
           JSON_EXTRACT_SCALAR(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT_SCALAR(payload, '$.comment.commit_id') as commit_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.body') as comment_body) as cce
           FROM tabRange
           WHERE type = 'CommitCommentEvent'
    )
), 


extra_info_prrce as (
SELECT 
  event_id, action, TO_JSON_STRING(prrce) as other
  FROM 
    (SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           (SELECT AS STRUCT
           JSON_EXTRACT_SCALAR(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT_SCALAR(payload, '$.comment.pull_request_review_id') as pull_request_review_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.pull_request.id') as pull_request_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.body') as comment_body) as prrce
           FROM tabRange
           WHERE type = 'PullRequestReviewCommentEvent')
WHERE action = "created"  
),   


extra_info_ice as (
SELECT 
  event_id, action, TO_JSON_STRING(ice) as other
  FROM
    (SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           (SELECT AS STRUCT  
           JSON_EXTRACT_SCALAR(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT_SCALAR(payload, '$.issue.id') as issue_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.body') as comment_body) as ice
           FROM tabRange
           WHERE type = 'IssueCommentEvent'
           )
 WHERE action = "created"  
), 

extra_info_issue_e as (
SELECT 
  event_id, action, TO_JSON_STRING(ie) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           (SELECT AS STRUCT 
           JSON_EXTRACT_SCALAR(payload, '$.issue.id') as issue_id,
           JSON_EXTRACT_SCALAR(payload, '$.issue.pull_request.id') as pull_request_id) as ie
           FROM tabRange
           WHERE type = 'IssuesEvent'
    )
 WHERE action = "opened"  
),

extra_info_pullrequest_e as (
SELECT 
  event_id, action, TO_JSON_STRING(pre) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           (SELECT AS STRUCT
           JSON_EXTRACT_SCALAR(payload, '$.pull_request.base.sha') as base_sha,
           JSON_EXTRACT_SCALAR(payload, '$.pull_request.base.repo.id') as base_repo_id,
           JSON_EXTRACT_SCALAR(payload, '$.pull_request.id') as pull_request_id
           ) as pre
           FROM tabRange
           WHERE type = 'PullRequestEvent'
    )
 WHERE action = "opened"  
),

extra_info_member_e as (
SELECT 
  event_id, action, TO_JSON_STRING(me) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           (SELECT AS STRUCT
           JSON_EXTRACT_SCALAR(payload, '$.member.id') as member_id,
           JSON_EXTRACT_SCALAR(payload, '$.member.login') as member_login) as me
           FROM tabRange
           WHERE type = 'MemberEvent'
    )
  WHERE action = "added"  
),

extra_info_release_e as (
SELECT 
  event_id, action, TO_JSON_STRING(re) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           (SELECT AS STRUCT
           JSON_EXTRACT_SCALAR(payload, '$.release.id') as release_id
           ) as re
           FROM tabRange
           WHERE type = 'ReleaseEvent'
    )
  WHERE action = "published"  
),

extra_info_create_e as (
SELECT 
  event_id, action, other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.ref_type') as action,
           "" as other
    FROM tabRange
    WHERE type = 'CreateEvent'
    )
  WHERE action = "repository"  
),

release_filtered as (
  SELECT JSON_EXTRACT_SCALAR(other, '$.release_id') as release_id, MIN(tabRange.id) as min_event_id
  FROM tabRange
  WHERE type = 'ReleaseEvent'
  GROUP BY release_id
), 

standard_info_filtered as (
  SELECT *
  FROM standard_info as si
  WHERE
    si.type != 'ReleaseEvent' or (
      EXISTS (
      SELECT * FROM release_filtered 
      WHERE si.event_id = release_filtered.min_event_id))
)


Select DISTINCT * 
From standard_info_filtered JOIN 
  (SELECT * FROM extra_info_cce UNION ALL 
  SELECT * FROM extra_info_prrce UNION ALL
  SELECT * FROM extra_info_ice UNION ALL 
  SELECT * FROM extra_info_issue_e UNION ALL
  SELECT * FROM extra_info_pullrequest_e UNION ALL
  SELECT * FROM extra_info_member_e UNION ALL
  SELECT * FROM extra_info_create_e UNION ALL
  SELECT * FROM extra_info_release_e)
  USING(event_id)
