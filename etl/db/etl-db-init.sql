-- These tables are used by the ETL process to get movies, reviewers, and reviews into the normal db

-- rt_queue: List of movies to be querried about in rottentomatoes.com
-- item_queue: Movies, Critics, and Reviews which have information and are ready to be transfered to the normal db
-- data: Attribute name, value pair table for the queue objects
-- rt_activity: Record of rottentomatoes api calls
-- etl_controller: Simple record of etl runs, a run will remember the run id of when it started, and quits
 	when that has changed. So by inserting a new row old etls will stop gracefully.
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
        hash integer not null default value 0,
        --
        primary key(item_id)
);

create index item_queue_hash on item_queue(hash) using hash;

create table data (
        item_id int not null,
        attr_name char(255) not null,
        attr_value text,
        date_entered timestamp,
        --
        foreign key (item_id) references item_queue(item_id)
);

create index data_value on data(attr_value(20)) using btree;
create index data_name on data(attr_name(20)) using btree;

create table rt_activity (
	rt_id int not null,
	ts timestamp not null,
	status text not null,
	estimated_api_calls int not null,
	etl_duration_seconds int not null,
	--
        foreign key (rt_id) references rt_queue(rt_id)
);

create table etl_controller (
	name int not null auto_increment,
	ts timestamp not null,
	--
	primary key(name)
);
