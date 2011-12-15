<?php

include 'functions.php';

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
//$movies = getMovies();
//$critics = getCritics();

?>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
	<head>
		<title>Critic Critic - Critics have lots of opinions, who agrees with YOUR opinion?</title>
	</head>
	<body>
		<div id="section-title">
			<h1>Critic Critic</h1>
			<span>Critics have lots of opinions, who agrees with YOUR opinion?</span>
		</div>
		<div id="section-rate">
			<form name="input" action="index.php" method="post">
				<table>
<?php
foreach($movies as $movie) {
?>
					<tr>
						<td><?php print $movie->title; ?></td>
						<td><?php print $movie->poster; ?></td>
						<td><input type="radio" name="plus-minus" value="plus">+</input></td>
						<td><input type="radio" name="plus-minus" value="minus">-</input></td>
					</tr>
<?php
}
?>
				</table>
				<input type="submit" value="Submit" />
			</form>
		</div>
		<div id="section-critics">
		</div>
	</body>
</html>
