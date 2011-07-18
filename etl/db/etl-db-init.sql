-- These tables are used by the ETL process to get movies, reviewers, and reviews into the normal db

-- rt_queue: List of movies to be querried about in rottentomatoes.com
-- item_queue: Movies, Critics, and Reviews which have information and are ready to be transfered to the normal db
-- data: Attribute name, value pair table for the queue objects
create table rt_queue (
        rt_id integer not null,
        link text not null,
        date_last_queried datetime null default null,
        date_last_found datetime null default null,
        date_last_scraped datetime null default null,
        date_lock datetime null default null,
        --
        primary key(rt_id)
);

create table item_queue (
        item_id integer not null auto_increment,
        date_created timestamp not null,
        date_pushed datetime,        
        --
        primary key(item_id)
);

create table data (
        item_id int not null,
        attr_name char(255) not null,
        attr_value text,
        date_entered timestamp,
        --
        foreign key (item_id) references item_queue(item_id)
);

create table rt_activity (
	rt_id int not null,
	ts timestamp not null,
	status text not null,
	estimated_api_calls int not null,
	etl_duration_seconds int not null,
	--
        foreign key (rt_id) references rt_queue(rt_id)
);

create table rt_scrape_activity (
	rt_id int not null,
	ts timestamp not null,
	status text not null,
	estimated_web_calls int not null,
	etl_duration_seconds int not null,
	--
        foreign key (rt_id) references rt_queue(rt_id)
);
