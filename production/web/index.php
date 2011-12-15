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
		<title>Critic Critic - Every critics has a different opinion, who agrees with YOUR opinion?</title>
	</head>
	<body>
		<div id="section-title">
			<h1>Critic Critic</h1>
			<span>Every critics has a different opinion, who agrees with YOUR opinion?</span>
		</div>
		<div id="section-rate">
			<form name="input" action="form.php" method="post">
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
	</body>
</html>
