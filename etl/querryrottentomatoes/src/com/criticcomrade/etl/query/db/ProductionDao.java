package com.criticcomrade.etl.query.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.criticcomrade.etl.data.DataItem;
import com.criticcomrade.etl.query.AttributeConstants;

public class ProductionDao extends AbstractDao {

    public ProductionDao(Connection conn) {
        super(conn);
    }

    public List<Integer> pushMovie(DataItem movie) {

        String releaseDate = null;
        String posterUrl = null;
        for (DataItem item : movie.getSubItems()) {
            if (item.getType().equals(AttributeConstants.RELEASEDATES)) {
                releaseDate = item.getAttributeValue(AttributeConstants.RELEASEDATES_THEATER);
            } else if (item.getType().equals(AttributeConstants.POSTERS)) {
                posterUrl = item.getAttributeValue(AttributeConstants.POSTERS_ORIGINAL);
            }
        }

        // If movie already exists, update it. Otherwise add it.
        if (movieExists(movie.getId())) {

            updateMovie(movie.getId(), movie.getAttributeValue(AttributeConstants.MOVIE_TITLE), movie
                    .getAttributeValue(AttributeConstants.MOVIE_IMDB_ID), movie
                    .getAttributeValue(AttributeConstants.MOVIE_YEAR), movie
                    .getAttributeValue(AttributeConstants.MOVIE_URL_TITLE), movie
                    .getAttributeValue(AttributeConstants.MOVIE_SYNOPSIS), movie
                    .getAttributeValue(AttributeConstants.MOVIE_RUNTIME), movie
                    .getAttributeValue(AttributeConstants.MOVIE_MPAA_RATING), releaseDate, posterUrl);

        } else {

            addMovie(movie.getId(), movie.getAttributeValue(AttributeConstants.MOVIE_TITLE), movie
                    .getAttributeValue(AttributeConstants.MOVIE_IMDB_ID), movie
                    .getAttributeValue(AttributeConstants.MOVIE_YEAR), movie
                    .getAttributeValue(AttributeConstants.MOVIE_URL_TITLE), movie
                    .getAttributeValue(AttributeConstants.MOVIE_SYNOPSIS), movie
                    .getAttributeValue(AttributeConstants.MOVIE_RUNTIME), movie
                    .getAttributeValue(AttributeConstants.MOVIE_MPAA_RATING), releaseDate, posterUrl);
        }

        for (DataItem item : movie.getSubItems()) {
            if (item.getType().equals(AttributeConstants.REVIEW)) {
                DataItem critic = item.getSubItems().iterator().next();

                if (criticExists(critic.getId())) {
                    updateCritic(critic.getId(), critic.getAttributeValue(AttributeConstants.REVIEWER_NAME), critic
                            .getAttributeValue(AttributeConstants.REVIEWER_PUBLICATION));
                } else {
                    addCritic(critic.getId(), critic.getAttributeValue(AttributeConstants.REVIEWER_NAME), critic
                            .getAttributeValue(AttributeConstants.REVIEWER_PUBLICATION));
                }

                if (reviewExists(item.getId())) {
                    updateReview(item.getId(), item.getAttributeValue(AttributeConstants.REVIEW_IS_POSITIVE), item
                            .getAttributeValue(AttributeConstants.REVIEW_ORIGINAL_SCORE), item
                            .getAttributeValue(AttributeConstants.REVIEW_LINK), item
                            .getAttributeValue(AttributeConstants.REVIEW_DATE), item
                            .getAttributeValue(AttributeConstants.REVIEW_QUOTE), movie.getId(), critic.getId());
                } else {
                    addReview(item.getId(), item.getAttributeValue(AttributeConstants.REVIEW_IS_POSITIVE), item
                            .getAttributeValue(AttributeConstants.REVIEW_ORIGINAL_SCORE), item
                            .getAttributeValue(AttributeConstants.REVIEW_LINK), item
                            .getAttributeValue(AttributeConstants.REVIEW_DATE), item
                            .getAttributeValue(AttributeConstants.REVIEW_QUOTE), movie.getId(), critic.getId());
                }
            }

        }

        return movie.getAllItemIds();

    }

    private boolean criticExists(int id) {

        try {
            String sql = "select 1 from cc_critic where cid = ?";
            PreparedStatement statement = conn.prepareStatement(sql);

            try {

                statement.setInt(1, id);

                return statement.executeQuery().next();

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean reviewExists(int id) {

        try {
            String sql = "select 1 from cc_review where rid = ?";
            PreparedStatement statement = conn.prepareStatement(sql);

            try {

                statement.setInt(1, id);

                return statement.executeQuery().next();

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean movieExists(int id) {

        try {
            String sql = "select 1 from cc_movie where mid = ?";
            PreparedStatement statement = conn.prepareStatement(sql);

            try {

                statement.setInt(1, id);

                return statement.executeQuery().next();

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void addCritic(int id, String name, String publication) {

        try {

            String sql = "insert into cc_critic(cid, name, publication) values (?,?,?)";
            PreparedStatement statement = conn.prepareStatement(sql);

            try {
                statement.setInt(1, id);
                statement.setString(2, name);
                statement.setString(3, publication);

                statement.execute();

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean addReview(int id, String isPositive, String originalScore, String link, String date, String quote,
            int mid, int cid) {

        boolean isPos = isPositive != null && isPositive.equals("TRUE");

        try {

            String sql = "insert into cc_review(rid, is_positive, link, original_score, quote, date, mid, cid) values (?,?,?,?,?,?,?,?)";

            PreparedStatement statement = conn.prepareStatement(sql);

            try {
                statement.setInt(1, id);
                statement.setBoolean(2, isPos);
                statement.setString(3, link);
                statement.setString(4, originalScore);
                statement.setString(5, quote);
                statement.setString(6, date);
                statement.setInt(7, mid);
                statement.setInt(8, cid);

                statement.execute();

                return false;

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private boolean addMovie(int id, String title, String imdbId, String year, String urlTitle, String synopsis,
            String runtime, String mpassRating, String releaseDate, String posterUrl) {

        try {

            final String sql = "insert into cc_movie(mid, title, imdb_id, year, url_title, synopsis, runtime, mpaa_rating, release_date, poster_link) values (?,?,?,?,?,?,?,?,?,?)";

            PreparedStatement statement = conn.prepareStatement(sql);

            try {
                statement.setInt(1, id);
                statement.setString(2, title);
                statement.setString(3, imdbId);
                statement.setString(4, year);
                statement.setString(5, urlTitle);
                statement.setString(6, synopsis);
                statement.setString(7, runtime);
                statement.setString(8, mpassRating);
                statement.setString(9, releaseDate);
                statement.setString(10, posterUrl);

                boolean success = statement.execute();

                return success;

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void updateCritic(int id, String name, String publication) {

        try {

            String sql = "update cc_critic set name = ?, publication = ? where cid = ?";
            PreparedStatement statement = conn.prepareStatement(sql);

            try {
                statement.setString(1, name);
                statement.setString(2, publication);
                statement.setInt(3, id);

                statement.execute();

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void updateReview(int id, String isPositive, String originalScore, String link, String date, String quote,
            int mid, int cid) {

        boolean isPos = isPositive != null && isPositive.equals("TRUE");

        try {

            String sql = "update cc_review set is_positive = ?, link = ?, original_score = ?, quote = ?, date = ?, mid = ?, cid = ? where rid = ?";

            PreparedStatement statement = conn.prepareStatement(sql);

            try {

                statement.setBoolean(1, isPos);
                statement.setString(2, link);
                statement.setString(3, originalScore);
                statement.setString(4, quote);
                statement.setString(5, date);
                statement.setInt(6, mid);
                statement.setInt(7, cid);
                statement.setInt(8, id);

                statement.execute();

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private void updateMovie(int id, String title, String imdbId, String year, String urlTitle, String synopsis,
            String runtime, String mpassRating, String releaseDate, String posterUrl) {

        try {

            final String sql = "update cc_movie set title = ?, imdb_id = ?, year = ?, url_title = ?, synopsis = ?, runtime = ?, mpaa_rating = ?, "
                    + "release_date = ?, poster_link = ? where mid = ?";

            PreparedStatement statement = conn.prepareStatement(sql);

            try {
                statement.setString(1, title);
                statement.setString(2, imdbId);
                statement.setString(3, year);
                statement.setString(4, urlTitle);
                statement.setString(5, synopsis);
                statement.setString(6, runtime);
                statement.setString(7, mpassRating);
                statement.setString(8, releaseDate);
                statement.setString(9, posterUrl);
                statement.setInt(10, id);

                statement.execute();

            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
}
