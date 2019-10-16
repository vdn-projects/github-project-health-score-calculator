SELECT 	h.org, 
		h.repo_name,
		printf("%.2f", (ifnull(cc.metric,0) + ifnull(io.metric,0) + ifnull(pr.metric,0) + ifnull(cd.metric,0))) health_score,
		
		cc.num_commits,
		cc.max_num_commits,
		cc.metric commit_count_metric,
		
		io.avg_opened_duration,
		io.min_opened_duration,
		io.metric opened_issued_metric,
		
		pr.avg_merged_duration,
		pr.min_merged_duration,
		pr.metric merged_pr_metric,
		
		cd.ratio,
		cd.max_ratio,
		cd.metric commit_developer_metric
FROM org_repo h
LEFT JOIN commit_count_metric cc ON cc.org = h.org AND cc.repo_name = h.repo_name
LEFT JOIN issue_opened_metric io ON io.org = h.org AND io.repo_name = h.repo_name
LEFT JOIN pr_merged_metric pr ON pr.org = h.org AND pr.repo_name = h.repo_name
LEFT JOIN commit_developer_ratio_metric cd ON cd.org = h.org AND cd.repo_name = h.repo_name
ORDER BY health_score DESC
LIMIT 1000;