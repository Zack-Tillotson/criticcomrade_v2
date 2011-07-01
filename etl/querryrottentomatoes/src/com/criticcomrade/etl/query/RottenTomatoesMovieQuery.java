package com.criticcomrade.etl.query;

import java.io.IOException;
import java.util.*;

import com.criticcomrade.api.data.*;
import com.criticcomrade.api.main.RottenTomatoesApi;
import com.criticcomrade.etl.query.data.*;
import com.google.gson.JsonSyntaxException;

public class RottenTomatoesMovieQuery extends DataItem {
    
    private final String rtid;
    private final Movie movie;
    private final List<Review> reviews;
    
    public RottenTomatoesMovieQuery(String rtid) throws JsonSyntaxException, IOException {
	super(AttributeConstants.MOVIE);
	this.rtid = rtid;
	movie = RottenTomatoesApi.getMovie(rtid);
	reviews = RottenTomatoesApi.getReviews(movie);
    }
    
    public String getRtId() {
	return rtid;
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
	if ((movie.genres != null)) {
	    for (String genre : movie.genres) {
		attrs.add(new Attribute(AttributeConstants.MOVIE_GENRE, genre));
	    }
	}
	if (movie.alternate_ids != null) {
	    attrs.add(new Attribute(AttributeConstants.MOVIE_IMDB_ID, movie.alternate_ids.imdb));
	}
	
	return attrs;
	
    }
    
    @Override
    protected Collection<DataItem> buildSubItems() {
	
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
	    protected Collection<DataItem> buildSubItems() {
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
	    protected Collection<DataItem> buildSubItems() {
		return new ArrayList<DataItem>();
	    }
	    
	});
	
	// Cast
	if (movie.abridged_cast != null) {
	    for (final MoviePerson person : movie.abridged_cast) {
		dataItems.add(new DataItem(AttributeConstants.CAST) {
		    
		    @Override
		    public Collection<Attribute> getDirectAttributes() {
			List<Attribute> attrs = new ArrayList<Attribute>();
			attrs.add(new Attribute(AttributeConstants.CAST_NAME, person.name));
			return attrs;
		    }
		    
		    @Override
		    protected Collection<DataItem> buildSubItems() {
			return new ArrayList<DataItem>();
		    }
		    
		});
	    }
	}
	
	// Directors
	if (movie.abridged_directors != null) {
	    for (final MoviePerson person : movie.abridged_directors) {
		dataItems.add(new DataItem(AttributeConstants.DIRECTOR) {
		    
		    @Override
		    public Collection<Attribute> getDirectAttributes() {
			List<Attribute> attrs = new ArrayList<Attribute>();
			attrs.add(new Attribute(AttributeConstants.DIRECTOR_NAME, person.name));
			return attrs;
		    }
		    
		    @Override
		    protected Collection<DataItem> buildSubItems() {
			return new ArrayList<DataItem>();
		    }
		    
		});
	    }
	}
	
	// Reviews
	for (final Review review : reviews) {
	    
	    dataItems.add(new DataItem(AttributeConstants.REVIEW) {
		
		@Override
		public Collection<Attribute> getDirectAttributes() {
		    List<Attribute> attrs = new ArrayList<Attribute>();
		    attrs.add(new Attribute(AttributeConstants.REVIEW_DATE, review.date));
		    attrs.add(new Attribute(AttributeConstants.REVIEW_ORIGINAL_SCORE, review.original_score));
		    attrs.add(new Attribute(AttributeConstants.REVIEW_QUOTE, review.quote));
		    attrs.add(new Attribute(AttributeConstants.REVIEW_LINK, review.links.review));
		    return attrs;
		}
		
		@Override
		protected Collection<DataItem> buildSubItems() {
		    ArrayList<DataItem> items = new ArrayList<DataItem>();
		    items.add(new DataItem(AttributeConstants.REVIEWER) {
			
			@Override
			protected Collection<Attribute> getDirectAttributes() {
			    List<Attribute> attrs = new ArrayList<Attribute>();
			    attrs.add(new Attribute(AttributeConstants.REVIEWER_NAME, review.critic));
			    attrs.add(new Attribute(AttributeConstants.REVIEWER_PUBLICATION, review.publication));
			    return attrs;
			}
			
			@Override
			protected Collection<DataItem> buildSubItems() {
			    return new ArrayList<DataItem>();
			}
		    });
		    return items;
		}
		
	    });
	    
	}
	
	return dataItems;
	
    }
}