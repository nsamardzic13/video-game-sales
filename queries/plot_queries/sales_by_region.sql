select 
	sum(na_sales) as North_America,
	sum(eu_sales) as Europe,
	sum(jp_sales) as Japan,
	sum(other_sales) as Other
from dm.fact_video_games_sales
;