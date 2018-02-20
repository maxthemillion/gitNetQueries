-- concatenates owner login and username to retrieve the full name in GHA compatible format

SELECT se.owner_login, p.name as repo_name, CONCAT(se.owner_login, "/", p.name) as full_name, p.id as ght_repo_id, p.forked_from
FROM `github-181509.GitArchive_Queries.selected_owners` as se
JOIN `ghtorrent-bq.ght_2017_09_01.projects` as p
ON se.ght_owner_id = p.owner_id
