create table cc_movie (
	mid int primary key not null
	,title char(255) not null
	,imdb_id char(128)
	,year char(4)
	,url_title char(255) not null
	,synopsis text
	,runtime char(32)
	,release_date datetime
	,poster_link text
	,mpaa_rating text
);

create table cc_critic (
	cid int primary key not null
	,name text not null
	,publication text not null
);

create table cc_review (
	rid int primary key not null
	,is_positive bool not null
	,link text
	,original_score text
	,date datetime not null
	,quote text
	,mid int not null
	,cid int not null
	---------------------------
	,foreign key(mid) references cc_movie(mid)
	,foreign key(rid) references cc_critic(cid)
);
