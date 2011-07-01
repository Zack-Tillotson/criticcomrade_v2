-- These tables are used by the ETL process to get movies, reviewers, and reviews into the normal db

-- rt_queue: List of movies to be querried about in rottentomatoes.com
-- item_queue: Movies, Critics, and Reviews which have information and are ready to be transfered to the normal db
-- data: Attribute name, value pair table for the queue objects
create table rt_queue (
        rt_id integer not null,
        link text not null,
        last_queried date,
        last_found date,
        --
        primary key(rt_id)
);

create table item_queue (
        item_id integer not null auto_increment,
        date_pushed date,
        --
        primary key(item_id)
);

create table data (
        item_id int not null,
        attr_name char(255) not null,
        attr_value text,
        --
        foreign key (item_id) references item_queue(item_id)
);

create table rt_activity (
	date activity_time not null default value now(),
	link text not null,
	action text
);