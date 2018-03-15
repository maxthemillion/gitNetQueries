-- concatenates owner login and username to retrieve the full repository name 
-- in GHA compatible format for all repositories which were selected to the sample.
-- format: owner/repository

SELECT 
  p.name, 
  p.name as repo_name, 
  CONCAT(se.owner_login, "/", p.name) as full_name, 
  p.id as ght_repo_id, 
  p.forked_from
FROM `github-181509.180305_MA_01.ght_owners_sample` as se
JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p
ON se.ght_owner_id = p.owner_id

