<?php

session_start();

include_once("db.properties");

function get_movies() {

	$query = 'select * from (select m.mid, m.title, m.poster_link, count(*) cnt FROM cc_movie m, cc_review r where m.mid = r.mid group by m.mid, m.title, m.poster_link) a order by cnt desc limit 25';
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

}

function setup_db() {
	$link = mysql_connect($GLOBALS['dbHost'], $GLOBALS['dbUser'], $GLOBALS['dbPass']) or die('Could not connect: ' . mysql_error());
        mysql_select_db($GLOBALS['dbName']) or die('Could not select database');
}

setup_db();

?>
