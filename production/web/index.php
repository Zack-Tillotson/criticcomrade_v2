<?php

include_once('functions.php');

// Iteration 1

	// Goal
		// 1. Get _something_ up and running
		// 2. Don't spend a lot of time
		// 3. Conversation seed

	// Risks
		// 1. Bite off too much
		// 2. Not professional looking

	// Plan
		// 1. Start with most basic atomic functionality - rate movies and see what critics agree
		// 2. Don't get fancy. Make basic now, will tweak look until professional.

	// Results

// Get the list of movies to rate
$movies = get_movies();
$critics = get_critics();

?>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<title>Heart Critics - Discover Your Perfect Critic Match</title>
		<style type="text/css">
			body {
				background: #0000cc;
				margin: 0px;
			}

			body div {
				background: white;
			}

			#section-title {
				background: #a1cd63;
				padding: 10px;
				margin: 10px 0px;
			}

			#section-title span {
				display: block;
				font-size: 30px; 
				margin-left: 150px;
			}

			#section-title h1 {
				display: inline;
				font-size: 72px;
			}

			#section-rate {
				width: 370px;
				float: left;
				margin-left: 10px;
				padding: 10px;
				margin-bottom:10px;
			}

			h2 {
				color: #426115;
			}

			#section-critics {
				float: left;
				margin-left: 10px;
				padding: 10px;
				margin-bottom:10px;
			}

			#section-footer {
				background: #a1cd63;
				padding: 10px;
				font-size: 20px;
				clear: both;
			}

		</style>
	</head>
	<body>
		<div id="section-title">
			<h1>&hearts; Critics</h1>
			<span>Every critic has a different opinion, who agrees with YOUR opinion?</span>
		</div>
		<div id="section-rate">
			<h2>1. Rate Movies You've Seen</h2>
			<form name="input" action="form.php" method="post">
				<input type="submit" value="Submit" />
				<table>
<?php
$i=0;
foreach($movies as $movie) {
?>
					<tr>
						<td>
							<input type="hidden" name="mid-<?php print $i; ?>" value="<?php print $movie->get_id(); ?>" />
							<?php print $movie->get_title(); ?>
						</td>
						<td><img src="<?php print $movie->get_poster(); ?>" width="50" /></td>
						<td><input type="radio" name="is_positive-<?php print $i; ?>" value="true">+</input></td>
						<td><input type="radio" name="is_positive-<?php print $i; ?>" value="false">-</input></td>
					</tr>
<?php
	$i++;
}
?>
				</table>
				<input type="submit" value="Submit" />
			</form>
		</div>
		<div id="section-critics">
			<h2>2. These Critics Agree</h2>
			<table>
				<tr>
					<td>Rank</td>
					<td>Name</td>
					<td>Publisher</td>
					<td>Agree On</td>
					<td>Of A Possible</td>
				</tr>	
<?php
foreach($critics as $critic) {
?>
				<tr>
					<td><?php print $critic->rank; ?></td>
					<td><?php print $critic->name; ?></td>
					<td><?php print $critic->publisher; ?></td>
					<td><?php print $critic->same_count; ?></td>
					<td><?php print $critic->tot_count; ?></td>
				</tr>	
<?php
}
?>
			</table>
		</div>
		<div id="section-footer">
			&copy; Zack Tillotson, 2012. Thanks <a href="http://www.rottentomatoes.com">RottenTomatoes</a> for having a nice API!
		</div>
	</body>
</html>
