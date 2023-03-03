select 
	dev.developer as Developer_Name,
	avg(score.user_score) as Average_User_Score
from dm.fact_video_games_score score
inner join dm.dim_video_games games
	on games.id = score.game_id 
inner join dm.dim_developer dev
	on dev.id = games.developer_id
where score.user_score is not null
group by Developer_Name 
order by Average_User_Score desc nulls last
limit 10
;