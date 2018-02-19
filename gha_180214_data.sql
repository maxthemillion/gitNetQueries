-- retrieves required event data from GHA

WITH tabRange as (
  SELECT data.*, ght_repo_id, forked_from as ght_forked_from
  FROM
    (SELECT *
    FROM `githubarchive.day.2018*`
    WHERE _TABLE_SUFFIX IN ("0219")
    and type IN ('CommitCommentEvent', 'IssueCommentEvent', 'PullRequestReviewCommentEvent', 'IssuesEvent',
    'PullRequestEvent', 'RepositoryEvent', 'MemberEvent', 'CreateEvent', 'ReleaseEvent')) as data
    JOIN `github-181509.GitArchive_Queries.selected_repos` selected_repos
    ON data.repo.name = selected_repos.full_name
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
  ght_repo_id,
  ght_forked_from

FROM tabRange  
), 

extra_info_cce as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(cce) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT(payload, '$.comment.commit_id') as commit_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.body') as comment_body,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           JSON_EXTRACT_SCALAR(payload, '$.sender.type') as actor_type
    FROM tabRange
    WHERE type = 'CommitCommentEvent'
    ) as cce
WHERE action = "created"  
), 


extra_info_prrce as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(prrce) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT(payload, '$.comment.pull_request_review_id') as pull_request_review_id,
           JSON_EXTRACT(payload, '$.comment.pull_request.id') as pull_request_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.body') as comment_body,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           JSON_EXTRACT_SCALAR(payload, '$.sender.type') as actor_type
           
    FROM tabRange
    WHERE type = 'PullRequestReviewCommentEvent' 
    ) as prrce
WHERE action = "created"  
),   


extra_info_ice as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(ice) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.comment.id') as comment_id,
           JSON_EXTRACT(payload, '$.comment.position') as comment_position,
           JSON_EXTRACT(payload, '$.issue.id') as issue_id,
           JSON_EXTRACT_SCALAR(payload, '$.comment.body') as comment_body,
           CASE WHEN JSON_EXTRACT(payload, '$.issue.pull_request.url') IS NOT NULL THEN TRUE ELSE FALSE END as is_pull_request,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           JSON_EXTRACT_SCALAR(payload, '$.sender.type') as actor_type

           FROM tabRange
    WHERE type = 'IssueCommentEvent'
    ) as ice
 WHERE action = "created"  
), 

extra_info_issue_e as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(ie) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.issue.id') as issue_id,
           JSON_EXTRACT(payload, '$.issue.pull_request.id') as pull_request_id,
           JSON_EXTRACT(payload, '$.issue.pull_request.url') as pull_request_url,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           JSON_EXTRACT_SCALAR(payload, '$.sender.type') as actor_type
           
    FROM tabRange
    WHERE type = 'IssuesEvent'
    ) as ie
 WHERE action = "opened"  
),

extra_info_pullrequest_e as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(pre) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.pull_request.base.sha') as base_sha,
           JSON_EXTRACT(payload, '$.pull_request.base.repo.id') as base_repo_id,
           JSON_EXTRACT(payload, '$.pull_request.base.created_at') as base_created_at,
           JSON_EXTRACT(payload, '$.pull_request.base.user.id') as base_user_id,
           JSON_EXTRACT_SCALAR(payload, '$.pull_request.base.user.login') as base_user_login,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           JSON_EXTRACT_SCALAR(payload, '$.sender.type') as actor_type
    FROM tabRange
    WHERE type = 'PullRequestEvent'
    ) as pre
 WHERE action = "opened"  
),

extra_info_member_e as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(me) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT(payload, '$.member.id') as member_id,
           JSON_EXTRACT_SCALAR(payload, '$.member.login') as member_login,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           JSON_EXTRACT_SCALAR(payload, '$.sender.type') as actor_type
    FROM tabRange
    WHERE type = 'MemberEvent'
    ) as me
  WHERE action = "added"  
),

extra_info_release_e as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(me) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.action') as action,
           JSON_EXTRACT_SCALAR(payload, '$.sender.type') as actor_type,
           JSON_EXTRACT(payload, '$.release.id') as release_id,
           JSON_EXTRACT(payload, '$.release.published_at') as published_at
    FROM tabRange
    WHERE type = 'ReleaseEvent'
    ) as me
  WHERE action = "published"  
),

extra_info_create_e as (
SELECT 
  event_id, action, actor_type, TO_JSON_STRING(ce) as other
  FROM (
    SELECT id as event_id,
           JSON_EXTRACT_SCALAR(payload, '$.ref_type') as action,
           JSON_EXTRACT_SCALAR(payload, '$.pusher_type') as actor_type
    FROM tabRange
    WHERE type = 'CreateEvent'
    ) as ce
  WHERE action = "repository"  
)

Select * 
From standard_info JOIN 
  (SELECT * FROM extra_info_cce UNION ALL 
  SELECT * FROM extra_info_prrce UNION ALL
  SELECT * FROM extra_info_ice UNION ALL 
  SELECT * FROM extra_info_issue_e UNION ALL
  SELECT * FROM extra_info_pullrequest_e UNION ALL
  SELECT * FROM extra_info_member_e UNION ALL
  SELECT * FROM extra_info_create_e UNION ALL
  SELECT * FROM extra_info_release_e)
  USING(event_id)
