package com.criticcomrade.etl.query;

import java.io.IOException;
import java.util.*;

import com.criticcomrade.api.data.Movie;
import com.criticcomrade.api.main.RottenTomatoesApi;
import com.criticcomrade.etl.query.data.*;
import com.google.gson.JsonSyntaxException;

public class RottenTomatoesMovieQuery extends DataItem {
    
    private final String id;
    private final String link;
    private final Movie movie;
    
    public RottenTomatoesMovieQuery(String id, String link) throws JsonSyntaxException, IOException {
	super(AttributeConstants.MOVIE);
	this.id = id;
	this.link = link;
	movie = RottenTomatoesApi.getMovie(id);
    }
    
    public String getId() {
	return id;
    }
    
    public String getLink() {
	return link;
    }
    
    public Movie getMovie() {
	return movie;
    }
    
    @Override
    public Collection<Attribute> getDirectAttributes() {
	
	List<Attribute> attrs = new ArrayList<Attribute>();
	
	// Basic attrs
	attrs.add(new Attribute(AttributeConstants.MOVIE_ID, movie.id));
	attrs.add(new Attribute(AttributeConstants.MOVIE_TITLE, movie.title));
	attrs.add(new Attribute(AttributeConstants.MOVIE_YEAR, movie.year));
	attrs.add(new Attribute(AttributeConstants.MOVIE_MPAA_RATING, movie.mpaa_rating));
	attrs.add(new Attribute(AttributeConstants.MOVIE_RUNTIME, movie.runtime));
	attrs.add(new Attribute(AttributeConstants.MOVIE_SYNOPSIS, movie.synopsis));
	if (movie.alternate_ids != null) {
	    attrs.add(new Attribute(AttributeConstants.MOVIE_IMDB_ID, movie.alternate_ids.imdb));
	}
	
	return attrs;
	
    }
    
    @Override
    public Collection<DataItem> getSubItems() {
	
	List<DataItem> dataItems = new ArrayList<DataItem>();
	
	// Release Dates
	dataItems.add(new DataItem(AttributeConstants.RELEASEDATES) {
	    
	    @Override
	    public Collection<Attribute> getDirectAttributes() {
		List<Attribute> attrs = new ArrayList<Attribute>();
		if (movie.release_dates != null) {
		    attrs.add(new Attribute(AttributeConstants.RELEASEDATES_THEATER, movie.release_dates.theater));
		    attrs.add(new Attribute(AttributeConstants.RELEASEDATES_DVD, movie.release_dates.dvd));
		}
		return attrs;
	    }
	    
	    @Override
	    public Collection<DataItem> getSubItems() {
		return new ArrayList<DataItem>();
	    }
	    
	});
	
	// Posters
	dataItems.add(new DataItem(AttributeConstants.POSTERS) {
	    
	    @Override
	    public Collection<Attribute> getDirectAttributes() {
		List<Attribute> attrs = new ArrayList<Attribute>();
		if (movie.posters != null) {
		    attrs.add(new Attribute(AttributeConstants.POSTERS_THUMBNAIL, movie.posters.thumbnail));
		    attrs.add(new Attribute(AttributeConstants.POSTERS_PROFILE, movie.posters.profile));
		    attrs.add(new Attribute(AttributeConstants.POSTERS_DETAILED, movie.posters.detailed));
		    attrs.add(new Attribute(AttributeConstants.POSTERS_ORIGINAL, movie.posters.original));
		}
		return attrs;
	    }
	    
	    @Override
	    public Collection<DataItem> getSubItems() {
		return new ArrayList<DataItem>();
	    }
	    
	});
	
	// TODO Cast
	// TODO Directors
	
	return dataItems;
	
    }
    
}