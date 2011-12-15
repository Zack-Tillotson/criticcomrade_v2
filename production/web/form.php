<?php

include_once('functions.php');

$reviews = array();
for($i = 0 ; $i < 25; $i++) {

	if(!isset($_POST["is_positive-$i"])) {
		continue;
	}

	$found_one = true;

	$mid = $_POST["mid-$i"];
	$sid = session_id();
	$is_positive = strcmp($_POST["is_positive-$i"], "true") == 0 ? "true" : "false";

	$reviews[] = array($mid, $sid, $is_positive);

}

if(count($reviews) > 0) {

	add_user_reviews($reviews);

}

?>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<html>
	<head>
		<title>Your Page Title</title>
		<meta http-equiv="REFRESH" content="0;url=index.php">
	</HEAD>
	<BODY>
		We're working to calculate which critics agree with you most! You will be sent along to your results (or click <a href="/">here</a>).
	</BODY>
</HTML>
