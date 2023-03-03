select 
	floor(games.year_of_release / 10) as Decade,
	round(avg(sales.global_sales), 0) as Average_Global_Sales,
	round(avg(score.critic_score), 2) as Average_Critic_Score
from dm.dim_video_games games
left join dm.fact_video_games_score score
	on score.game_id = games.id 
left join dm.fact_video_games_sales sales
	on sales.game_id = games.id 
where 
	games.year_of_release is not null
	and sales.global_sales is not null
	and score.critic_score is not null
group by Decade
order by Decade
;