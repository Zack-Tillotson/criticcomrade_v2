package com.criticcomrade.etl.query;

import java.util.*;

public class AttributeConstants {
    
    public static final String TYPE = "TYPE";
    
    public static final String MOVIE = "MOVIE";
    public static final String MOVIE_ID = "ID";
    public static final String MOVIE_TITLE = "TITLE";
    public static final String MOVIE_YEAR = "YEAR";
    public static final String MOVIE_MPAA_RATING = "MPAA_RATING";
    public static final String MOVIE_RUNTIME = "RUNTIME";
    public static final String MOVIE_SYNOPSIS = "SYNOPSIS";
    public static final String MOVIE_IMDB_ID = "IMDB_ID";
    public static final String MOVIE_GENRE = "GENRE";
    public static final String MOVIE_URL_TITLE = "URL_TITLE";
    
    public static final String CAST = "CAST";
    public static final String CAST_NAME = "NAME";
    
    public static final String DIRECTOR = "DIRECTOR";
    public static final String DIRECTOR_NAME = "NAME";
    
    public static final String RELEASEDATES = "RELEASEDATES";
    public static final String RELEASEDATES_THEATER = "THEATER";
    public static final String RELEASEDATES_DVD = "DVD";
    
    public static final String POSTERS = "POSTERS";
    public static final String POSTERS_THUMBNAIL = "THUMBNAIL";
    public static final String POSTERS_PROFILE = "PROFILE";
    public static final String POSTERS_DETAILED = "DETAILED";
    public static final String POSTERS_ORIGINAL = "ORIGINAL";
    
    public static final String REVIEW = "REVIEW";
    public static final String REVIEW_ORIGINAL_SCORE = "ORIGINAL_SCORE";
    public static final String REVIEW_DATE = "DATE";
    public static final String REVIEW_LINK = "LINK";
    public static final String REVIEW_QUOTE = "QUOTE";
    public static final String REVIEW_IS_POSITIVE = "IS_POSITIVE";
    
    public static final String REVIEWER = "REVIEWER";
    public static final String REVIEWER_NAME = "NAME";
    public static final String REVIEWER_PUBLICATION = "PUBLICATION";
    
    private static Map<String, List<String>> keyAttributes = null;
    
    public static Map<String, List<String>> getKeyAttributes() {
	
	if (keyAttributes == null) {
	    keyAttributes = new HashMap<String, List<String>>();
	    
	    ArrayList<String> l;
	    
	    // MOVIE
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.MOVIE_ID);
	    keyAttributes.put(AttributeConstants.MOVIE, l);
	    
	    // CAST
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.CAST_NAME);
	    keyAttributes.put(AttributeConstants.CAST, l);
	    
	    // DIRECTOR
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.DIRECTOR_NAME);
	    keyAttributes.put(AttributeConstants.DIRECTOR, l);
	    
	    // RELEASEDATES
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.RELEASEDATES_THEATER);
	    l.add(AttributeConstants.RELEASEDATES_DVD);
	    keyAttributes.put(AttributeConstants.RELEASEDATES, l);
	    
	    // POSTERS
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.POSTERS_DETAILED);
	    l.add(AttributeConstants.POSTERS_ORIGINAL);
	    l.add(AttributeConstants.POSTERS_PROFILE);
	    l.add(AttributeConstants.POSTERS_THUMBNAIL);
	    keyAttributes.put(AttributeConstants.POSTERS, l);
	    
	    // REVIEW
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.REVIEW_LINK);
	    l.add(AttributeConstants.REVIEW_QUOTE); // Some people are dumb and don't link to the actual review, just a generic site
	    keyAttributes.put(AttributeConstants.REVIEW, l);
	    
	    // REVIEWER
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.REVIEWER_NAME);
	    l.add(AttributeConstants.REVIEWER_PUBLICATION);
	    keyAttributes.put(AttributeConstants.REVIEWER, l);
	    
	}
	
	return keyAttributes;
	
    }
    
    private static List<String> typeNames = null;
    
    public static List<String> getObjectTypeAttributeNames() {
	
	if (typeNames == null) {
	    typeNames = new ArrayList<String>();
	    typeNames.add(MOVIE);
	    typeNames.add(CAST);
	    typeNames.add(DIRECTOR);
	    typeNames.add(RELEASEDATES);
	    typeNames.add(POSTERS);
	    typeNames.add(REVIEW);
	    typeNames.add(REVIEWER);
	}
	
	return typeNames;
	
    }
    
}
