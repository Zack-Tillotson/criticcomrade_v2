package com.criticcomrade.etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Scanner;

import com.criticcomrade.api.data.MovieShort;
import com.criticcomrade.api.main.RottenTomatoesApi;

public class LoadHistoricalMovieTitles extends Thread {

	public static void main(String[] args) throws FileNotFoundException, InterruptedException {

		PrintWriter out = new PrintWriter(new File("rt_queue.csv"));

		int i = 0;
		Scanner in = new Scanner(new File(args[0]));
		while (in.hasNext()) {
			i++;
			String title = in.nextLine();
			try {
				System.out.println("\n\t" + title);
				List<MovieShort> results = RottenTomatoesApi.searchMovies(title);
				for (MovieShort m : results) {
					System.out.println(m.title + "(" + m.year + ") = " + m.links.self);
					out.println(m.id + "," + m.links.self);
				}
				sleep(100);
			} catch (Exception e) {
				System.err.println("Call failed: " + i + " " + title);
			}
		}
		out.close();

	}

}
