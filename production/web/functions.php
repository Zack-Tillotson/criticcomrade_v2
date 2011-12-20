<?php

session_start();

include_once("db.properties");

function get_movies() {

	$query = 'select * from (select m.mid, m.title, m.poster_link, count(*) cnt FROM cc_movie m, cc_review r where m.mid = r.mid group by m.mid, m.title, m.poster_link) a order by cnt desc limit 50';
	$result = mysql_query($query) or die('Query failed: ' . mysql_error());
	 
	$ret = array();

	while ($line = mysql_fetch_array($result, MYSQL_ASSOC)) {
		$ret[] = new Movie($line['mid'], $line['title'], $line['poster_link']);
	}

	return $ret;

}

class Movie {

	private $id;
	private $title;
	private $poster;
	
	public function __construct($id, $title, $poster) {
		$poster = preg_replace('/_ori/', '_mob', $poster);

		$this->id = $id;
		$this->title = $title;
		$this->poster = $poster;
	}

	public function get_id() {
		return $this->id;
	}

	public function get_title() {
		return $this->title;
	}
	public function get_poster() {
		return $this->poster;
	}
}	


function get_critics() {

	
	$query = "select c.name, c.publication, same_count, tot_count from cc_critic c, (select r.cid, sum(if(r.is_positive = u.is_positive, 1, 0)) same_count, count(*) tot_count from cc_review r, cc_user_review u where r.mid = u.mid and u.sid = '?' group by r.cid) a where a.cid = c.cid order by same_count desc, tot_count asc limit 100";

	$query = str_replace_once("?", mysql_real_escape_string(session_id()), $query);
	$result = mysql_query($query) or die('Query failed: ' . mysql_error());
	 
	$ret = array();

	$i = 0;

	while ($line = mysql_fetch_array($result, MYSQL_ASSOC)) {
		$ret[] = new Critic(++$i, $line['name'], $line['publication'], $line['same_count'], $line['tot_count']);
	}

	return $ret;

}

class Critic {

	public $rank;
	public $name;
	public $publisher;
	public $same_count;
	public $tot_count;

	public function __construct($rank, $name, $publisher, $same_count, $tot_count) {
		$this->rank = $rank;
		$this->name = $name;
		$this->publisher = $publisher;
		$this->same_count= $same_count;
		$this->tot_count= $tot_count;
	}

}

function setup_db() {
	$link = mysql_connect($GLOBALS['dbHost'], $GLOBALS['dbUser'], $GLOBALS['dbPass']) or die('Could not connect: ' . mysql_error());
        mysql_select_db($GLOBALS['dbName']) or die('Could not select database');
}

function add_user_reviews($reviews = array()) {

	$query_delete = "delete from cc_user_review where sid = '?' and mid = '?'";
	$query_insert = "insert into cc_user_review (mid, sid, is_positive) values ('?', '?', ?)";
	$queres = array();

	foreach($reviews as $review) {
		$q = $query_delete;
		$q = str_replace_once("?", mysql_real_escape_string($review[1]), $q);
		$q = str_replace_once("?", mysql_real_escape_string($review[0]), $q);
		$queries[] = $q;

		$q = $query_insert;
		$q = str_replace_once("?", mysql_real_escape_string($review[0]), $q);
		$q = str_replace_once("?", mysql_real_escape_string($review[1]), $q);
		$q = str_replace_once("?", mysql_real_escape_string($review[2]), $q);
		$queries[] = $q;
	}

	foreach($queries as $query) {
		$result = mysql_query($query) or die('Query failed: ' . mysql_error());
	}

}

function str_replace_once($str_pattern, $str_replacement, $string){
       
	if (strpos($string, $str_pattern) !== false){
		$occurrence = strpos($string, $str_pattern);
		return substr_replace($string, $str_replacement, strpos($string, $str_pattern), strlen($str_pattern));
	}

	return $string;
} 

setup_db();

?>
