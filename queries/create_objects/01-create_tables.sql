create table if not exists staging.stg_video_games (
	"name"					varchar(150),
	platform				varchar(10),
	year_of_release			integer,
	genre					varchar(20),
	publisher				varchar(100),
	na_sales				integer,
	eu_sales				integer,
	jp_sales				integer,
	other_sales				integer,
	global_sales			integer,
	critic_score			integer,
	critic_count			integer,
	user_score				float,
	user_count				integer,
	developer				varchar(100),
	rating 					varchar(5),
	record_timestamp		timestamp default now()
);

create table if not exists dm.dim_platform (
	id 						serial primary key,
	platform				varchar(10)
);

create table if not exists dm.dim_publisher (
	id 						serial primary key,
	publisher				varchar(100)
);

create table if not exists dm.dim_developer (
	id 						serial primary key,
	developer				varchar(100)
);

create table if not exists dm.dim_video_games (
	id 						serial primary key,
	"name"					varchar(150),
	platform_id				integer,
	year_of_release			integer,
	genre					varchar(20),
	publisher_id			integer,
	developer_id			integer,
	rating 					varchar(5)
);

create table if not exists dm.fact_video_games_sales (
	id 						serial primary key,
	game_id					integer,
	na_sales				integer,
	eu_sales				integer,
	jp_sales				integer,
	other_sales				integer,
	global_sales			integer,
	record_start_timestamp	timestamp,
	record_end_timestamp	timestamp default null
);

create table if not exists dm.fact_video_games_score (
	id 						serial primary key,
	game_id					integer,
	critic_score			integer,
	critic_count			integer,
	user_score				float,
	user_count				integer,
	record_start_timestamp	timestamp,
	record_end_timestamp	timestamp default null
);