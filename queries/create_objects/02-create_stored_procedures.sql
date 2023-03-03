create or replace procedure staging.sp_update_dim_platform()
language plpgsql
as $$
begin 
	
	-- update statement would be added if dim table would have more columns
	-- insert new platform records
	insert into dm.dim_platform (platform)
	select distinct 
		platform 
	from staging.stg_video_games stg
	where platform is not null
	and not exists (
		select 1
		from dm.dim_platform dim
		where dim.platform = stg.platform
	)
	;
	
end; $$


create or replace procedure staging.sp_update_dim_publisher()
language plpgsql
as $$
begin 
	
	-- update statement would be added if dim table would have more columns
	-- insert new publisher records
	insert into dm.dim_publisher (publisher)
	select distinct 
		publisher 
	from staging.stg_video_games stg
	where publisher is not null
	and not exists (
		select 1
		from dm.dim_publisher dim
		where dim.publisher = stg.publisher
	)
	;
	
end; $$


create or replace procedure staging.sp_update_dim_developer()
language plpgsql
as $$
begin 
	
	-- update statement would be added if dim table would have more columns
	-- insert new developer records
	insert into dm.dim_developer (developer)
	select distinct 
		developer 
	from staging.stg_video_games stg
	where developer is not null
	and not exists (
		select 1
		from dm.dim_developer dim
		where dim.developer = stg.developer
	)
	;
	
end; $$


create or replace procedure staging.sp_update_dim_video_games()
language plpgsql
as $$
begin 
	
	-- update values
	update dm.dim_video_games dim
	set 
		genre = stg.genre,
		publisher_id = pu.id,
		developer_id = dev.id,
		rating = stg.rating 
	from staging.stg_video_games stg
	left join dm.dim_platform pl
		on stg.platform = pl.platform
	left join dm.dim_publisher pu
		on stg.publisher = pu.publisher
	left join dm.dim_developer dev
		on stg.developer = dev.developer	
	where stg."name" is not null
	and dim."name" = stg."name"
	and coalesce(dim.platform_id, -13) =  coalesce(pl.id, -13)
	and coalesce(dim.year_of_release, -13) =  coalesce(stg.year_of_release, -13)
	and 
		(
			coalesce(dim.genre, 'default') != coalesce(stg.genre, 'default')
			or coalesce(dim.publisher_id, -13) != coalesce(pu.id, -13)
			or coalesce(dim.developer_id, -13) != coalesce(dev.id, -13)
			or coalesce(dim.rating, 'default') != coalesce(stg.rating, 'default') 
		)
	;
	-- insert new developer records
	insert into dm.dim_video_games ("name", platform_id, year_of_release, genre, publisher_id, developer_id, rating)
	select distinct 
		stg."name",
		pl.id as platform_id,
		stg.year_of_release,
		stg.genre,
		pu.id as publisher_id,
		dev.id as developer_id,
		stg.rating 
	from staging.stg_video_games stg
	left join dm.dim_platform pl
		on stg.platform = pl.platform
	left join dm.dim_publisher pu
		on stg.publisher = pu.publisher
	left join dm.dim_developer dev
		on stg.developer = dev.developer	
	where stg."name" is not null
	and not exists (
		select 1
		from dm.dim_video_games dim
		where 
			coalesce(dim."name", 'default') = coalesce(stg."name", 'default')
			and coalesce(dim.platform_id, -13) = coalesce(pl.id, -13)
			and coalesce(dim.year_of_release, -13) = coalesce(stg.year_of_release, -13)
			and coalesce(dim.genre, 'default') = coalesce(stg.genre, 'default')
			and coalesce(dim.publisher_id, -13) = coalesce(pu.id, -13)
			and coalesce(dim.developer_id, -13) = coalesce(dev.id, -13)
			and coalesce(dim.rating, 'default') = coalesce(stg.rating, 'default')
	)
	;
	
end; $$


create or replace procedure staging.sp_update_fact_video_games_sales()
language plpgsql
as $$
begin 
	
	-- set value to inactive
	update dm.fact_video_games_sales fact
	set 
		record_end_timestamp = fact.record_start_timestamp - interval '1 day'
	from dm.dim_video_games vg
	left join dm.dim_platform dpl
		on vg.platform_id = dpl.id 
	left join dm.dim_publisher dpu
		on dpu.id = vg.publisher_id
	left join dm.dim_developer dd
		on dd.id = vg.developer_id
	inner join staging.stg_video_games stg
		on coalesce(stg."name", 'default') = coalesce(vg."name", 'default')
		and coalesce(stg.year_of_release, -13) = coalesce(vg.year_of_release , -13)
		and coalesce(stg.platform, 'default') = coalesce(dpl.platform, 'default')
		and coalesce(stg.publisher, 'default') = coalesce(dpu.publisher, 'default')
		and coalesce(stg.developer, 'default') = coalesce(dd.developer, 'default')
	where 
		fact.game_id = vg.id
		and (
			fact.na_sales != stg.na_sales
			or fact.eu_sales != stg.eu_sales
			or fact.jp_sales != stg.jp_sales
			or fact.other_sales != stg.other_sales
			or fact.global_sales != stg.global_sales
		)
	;
	-- insert new developer records
	insert into dm.fact_video_games_sales (game_id, na_sales, eu_sales, jp_sales, other_sales, global_sales, record_start_timestamp)
	select distinct 
		vg.id as game_id,
		stg.na_sales,
		stg.eu_sales,
		stg.jp_sales,
		stg.other_sales,
		stg.global_sales,
		stg.record_timestamp as record_start_timestamp
	from dm.dim_video_games vg
	left join dm.dim_platform dpl
		on vg.platform_id = dpl.id 
	left join dm.dim_publisher dpu
		on dpu.id = vg.publisher_id
	left join dm.dim_developer dd
		on dd.id = vg.developer_id
	inner join staging.stg_video_games stg
		on coalesce(stg."name", 'default') = coalesce(vg."name", 'default')
		and coalesce(stg.year_of_release, -13) = coalesce(vg.year_of_release , -13)
		and coalesce(stg.platform, 'default') = coalesce(dpl.platform, 'default')
		and coalesce(stg.publisher, 'default') = coalesce(dpu.publisher, 'default')
		and coalesce(stg.developer, 'default') = coalesce(dd.developer, 'default') 
	where not exists (
		select 1 
		from dm.fact_video_games_sales fact 
		where 
			fact.game_id = vg.id
			and fact.na_sales = stg.na_sales
			and fact.eu_sales = stg.eu_sales
			and fact.jp_sales = stg.jp_sales
			and fact.other_sales = stg.other_sales
			and fact.global_sales = stg.global_sales
	)
	;
	
end; $$


create or replace procedure staging.sp_update_fact_video_games_score()
language plpgsql
as $$
begin 
	
	-- set value to inactive
	update dm.fact_video_games_score fact
	set 
		record_end_timestamp = fact.record_start_timestamp - interval '1 day'
	from dm.dim_video_games vg
	left join dm.dim_platform dpl
		on vg.platform_id = dpl.id 
	left join dm.dim_publisher dpu
		on dpu.id = vg.publisher_id
	left join dm.dim_developer dd
		on dd.id = vg.developer_id
	inner join staging.stg_video_games stg
		on coalesce(stg."name", 'default') = coalesce(vg."name", 'default')
		and coalesce(stg.year_of_release, -13) = coalesce(vg.year_of_release , -13)
		and coalesce(stg.platform, 'default') = coalesce(dpl.platform, 'default')
		and coalesce(stg.publisher, 'default') = coalesce(dpu.publisher, 'default')
		and coalesce(stg.developer, 'default') = coalesce(dd.developer, 'default') 
	where 
		fact.game_id = vg.id
		and (
			coalesce(fact.critic_score, -13) != coalesce(stg.critic_score, -13)
			or coalesce(fact.critic_count, -13) != coalesce(stg.critic_count, -13)
			or coalesce(fact.user_score, -13) != coalesce(stg.user_score, -13)
			or coalesce(fact.user_count, -13) != coalesce(stg.user_count, -13)
		)
	;
	-- insert new developer records
	insert into dm.fact_video_games_score (game_id, critic_score, critic_count, user_score, user_count, record_start_timestamp)
	select distinct 
		vg.id as game_id,
		stg.critic_score,
		stg.critic_count,
		stg.user_score,
		stg.user_count,
		stg.record_timestamp as record_start_timestamp
	from dm.dim_video_games vg
	left join dm.dim_platform dpl
		on vg.platform_id = dpl.id 
	left join dm.dim_publisher dpu
		on dpu.id = vg.publisher_id
	left join dm.dim_developer dd
		on dd.id = vg.developer_id
	inner join staging.stg_video_games stg
		on coalesce(stg."name", 'default') = coalesce(vg."name", 'default')
		and coalesce(stg.year_of_release, -13) = coalesce(vg.year_of_release , -13)
		and coalesce(stg.platform, 'default') = coalesce(dpl.platform, 'default')
		and coalesce(stg.publisher, 'default') = coalesce(dpu.publisher, 'default')
		and coalesce(stg.developer, 'default') = coalesce(dd.developer, 'default') 
	where not exists (
		select 1 
		from dm.fact_video_games_score fact 
		where 
			coalesce(fact.game_id, -13) = coalesce(vg.id, -13)
			and coalesce(fact.critic_score, -13) = coalesce(stg.critic_score, -13)
			and coalesce(fact.critic_count, -13) = coalesce(stg.critic_count, -13)
			and coalesce(fact.user_score, -13) = coalesce(stg.user_score, -13)
			and coalesce(fact.user_count, -13) = coalesce(stg.user_count, -13)
	)
	-- many null values in score columns.. we can skip them if every column is null
	and (
		stg.critic_score is not null
		or stg.critic_count is not null
		or stg.user_score is not null
		or stg.user_count is not null
	)
	;
	
end; $$