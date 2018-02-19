WITH tabRange as (
  SELECT *
  FROM `githubarchive.day.2017*`
  WHERE _TABLE_SUFFIX IN ("0214")
  and type IN ('CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent', 'IssuesEvent',
  'PullRequestEvent', 'RepositoryEvent', 'MemberEvent', 'WatchEvent', 'ForkEvent', 'ReleaseEvent')
),

standard_info as(
SELECT 
  id as event_id,
  type, 
  repo.name as repo_name, 
  repo.id as repo_id,
  actor.id as actor_id, 
  actor.login as actor_login, 
  org.id as org_id, 
  org.login as org_login, 
  created_at as event_time,
  JSON_EXTRACT(payload, '$.action') as action,
  JSON_EXTRACT(payload, '$.sender.type') as actor_type
  
FROM tabRange  

), 

extra_info_cce as (
SELECT 
  event_id, TO_JSON_STRING(cce) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT(payload, '$.comment.commit_id') as commit_id,
           JSON_EXTRACT(payload, '$.comment.body') as comment_body
    FROM tabRange
    WHERE type = 'CommitCommentEvent'
  ) as cce
), 


extra_info_prrce as (
SELECT 
  event_id, TO_JSON_STRING(prrce) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT(payload, '$.comment.pull_request_review_id') as pull_request_review_id,
           JSON_EXTRACT(payload, '$.comment.pull_request.id') as pull_request_id,
           JSON_EXTRACT(payload, '$.comment.body') as comment_body
    FROM tabRange
    WHERE type = 'PullRequestReviewCommentEvent'
  ) as prrce
), 


extra_info_ice as (
SELECT 
  event_id, TO_JSON_STRING(ice) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT(payload, '$.issue.id') as issue_id,
           JSON_EXTRACT(payload, '$.comment.body') as comment_body,
           CASE WHEN JSON_EXTRACT(payload, '$.issue.pull_request.url') IS NOT NULL THEN TRUE ELSE FALSE END as is_pull_request
    FROM tabRange
    WHERE type = 'IssueCommentEvent'
  ) as ice
), 

extra_info_ie as (
SELECT 
  event_id, TO_JSON_STRING(ie) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.issue.id') as issue_id,
           JSON_EXTRACT(payload, '$.issue.pull_request.id') as pull_request_id,
           JSON_EXTRACT(payload, '$.issue.pull_request.url') as pull_request_url
    FROM tabRange
    WHERE type = 'IssueEvent'
  ) as ie
),

extra_info_fe as (
SELECT 
  event_id, TO_JSON_STRING(fe) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.forkee.id') as forkee_id,
           JSON_EXTRACT(payload, '$.forkee.name') as forkee_name
    FROM tabRange
    WHERE type = 'ForkEvent'
  ) as fe
),

extra_info_pre as (
SELECT 
  event_id, TO_JSON_STRING(pre) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.pull_request.base.sha') as base_sha,
           JSON_EXTRACT(payload, '$.pull_request.base.repo.id') as base_repo_id,
           JSON_EXTRACT(payload, '$.pull_request.base.created_at') as base_created_at,
           JSON_EXTRACT(payload, '$.pull_request.base.user.id') as base_user_id,
           JSON_EXTRACT(payload, '$.pull_request.base.user.login') as base_user_login
    FROM tabRange
    WHERE type = 'PullRequestEvent'
  ) as pre
),

extra_info_me as (
SELECT 
  event_id, TO_JSON_STRING(me) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.member.id') as member_id,
           JSON_EXTRACT(payload, '$.member.login') as member_login
    FROM tabRange
    WHERE type = 'MemberEvent'
  ) as me
),

extra_info_re as (
SELECT 
  event_id, TO_JSON_STRING(re) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.release.id') as release_id,
           JSON_EXTRACT(payload, '$.release.author.id') as author_id,
           JSON_EXTRACT(payload, '$.release.published_at') as published_at
    FROM tabRange
    WHERE type = 'ReleaseEvent'
  ) as re
)


Select * 
From standard_info LEFT JOIN 
  (SELECT * FROM extra_info_cce UNION ALL 
  SELECT * FROM extra_info_prrce UNION ALL
  SELECT * FROM extra_info_ice UNION ALL 
  SELECT * FROM extra_info_ie UNION ALL
  SELECT * FROM extra_info_fe UNION ALL
  SELECT * FROM extra_info_pre UNION ALL
  SELECT * FROM extra_info_me UNION ALL
  SELECT * FROM extra_info_re)
  USING(event_id)
