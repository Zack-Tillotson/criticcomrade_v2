package com.criticcomrade.etl.query;

import java.sql.*;
import java.util.*;

import com.criticcomrade.etl.query.data.*;

public class DataItemDao {
    
    private static Map<String, List<String>> keyAttributes = null;
    
    private static Map<String, List<String>> getKeyAttributes() {
	
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
	    keyAttributes.put(AttributeConstants.REVIEW, l);
	    
	    // REVIEWER
	    l = new ArrayList<String>();
	    l.add(AttributeConstants.REVIEWER_NAME);
	    l.add(AttributeConstants.REVIEWER_PUBLICATION);
	    keyAttributes.put(AttributeConstants.REVIEWER, l);
	    
	}
	
	return keyAttributes;
	
    }
    
    public static boolean putDataItem(DataItem item) {
	
	boolean changed = false;
	
	// Put all subitems first
	for (DataItem subitem : item.getSubItems()) {
	    if (putDataItem(subitem)) {
		changed = true;
	    }
	}
	
	// Put the item
	if (writeAttributes(item)) {
	    changed = true;
	}
	
	return changed;
	
    }
    
    private static boolean writeAttributes(DataItem item) {
	
	// First ensure the item is backed
	boolean changed = ensureItemIsBacked(item);
	
	// Get the subitems link attributes (all should be backed)
	Collection<Attribute> attrs = new ArrayList<Attribute>();
	for (DataItem subitem : item.getSubItems()) {
	    attrs.add(new Attribute(subitem.getType(), String.format("%d", subitem.getId())));
	}
	
	// Write all the attributes
	attrs.addAll(item.getAttributes());
	putItemAttributes(item.getId(), attrs);
	
	return changed;
	
    }
    
    private static boolean ensureItemIsBacked(DataItem item) {
	
	// Look for data attributes by data type
	Collection<Attribute> keys = new ArrayList<Attribute>();
	for (Attribute attr : item.getAttributes()) {
	    if (getKeyAttributes().get(item.getType()).contains(attr.attribute)) {
		keys.add(attr);
	    }
	}
	
	// Find which item we're looking at
	int id = findItemByAttributes(item.getType(), keys);
	
	if (id == 0) {
	    id = addNewDataItem();
	}
	
	item.setId(id);
	
	// See if the db attributes are different from the current ones
	boolean changed = false;
	
	Collection<Attribute> dbAttrs = getItemAttrs(item.getId());
	for (Attribute dbAttr : dbAttrs) {
	    if (!item.getAttributes().contains(dbAttr)) {
		changed = true;
	    }
	}
	for (Attribute attr : item.getAttributes()) {
	    if (!dbAttrs.contains(attr)) {
		changed = true;
	    }
	}
	
	return changed;
	
    }
    
    private static int findItemByAttributes(String type, Collection<Attribute> keys) {
	
	keys.add(new Attribute(AttributeConstants.TYPE, type));
	
	try {
	    
	    List<String> values = new ArrayList<String>();
	    String sql = "select item_id from (select item_id, count(*) cnt from (select item_id, attr_name from data where ";
	    
	    for (Attribute key : keys) {
		sql = sql + "(attr_name = ? and attr_value = ?) or ";
		values.add(key.attribute);
		values.add(key.value);
	    }
	    
	    sql = sql + " false) a group by item_id) b where cnt = ?";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    for (int i = 0; i < values.size(); i++) {
		statement.setString(i + 1, values.get(i));
	    }
	    statement.setInt(values.size() + 1, keys.size());
	    
	    int id;
	    
	    ResultSet res = statement.executeQuery();
	    if (res.next()) {
		id = res.getInt(1); // Found the item
	    } else {
		id = 0; // Didn't find item with all the attributes
	    }
	    
	    res.close();
	    statement.close();
	    
	    return id;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
	
    }
    
    private static Collection<Attribute> getItemAttrs(int id) {
	
	try {
	    
	    String sql = "select attr_name, attr_value from data where item_id = ?";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.setInt(1, id);
	    
	    Collection<Attribute> attrs = new ArrayList<Attribute>();
	    
	    ResultSet res = statement.executeQuery();
	    while (res.next()) {
		attrs.add(new Attribute(res.getString(1), res.getString(2)));
	    }
	    
	    res.close();
	    statement.close();
	    
	    return attrs;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
	
    }
    
    private static int addNewDataItem() {
	
	try {
	    
	    String sql = "insert into item_queue() values ()";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.executeUpdate();
	    
	    sql = "select LAST_INSERT_ID()";
	    statement = DaoUtility.getConnection().prepareStatement(sql);
	    ResultSet rs = statement.executeQuery();
	    
	    int id;
	    if (rs.next()) {
		id = rs.getInt(1);
	    } else {
		throw new SQLException("Did not correctly find the last inserted id");
	    }
	    
	    rs.close();
	    statement.close();
	    
	    return id;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    private static void putItemAttributes(int id, Collection<Attribute> attrs) {
	
	try {
	    
	    String sql = "delete from data where item_id = ?";
	    
	    PreparedStatement statement = DaoUtility.getConnection().prepareStatement(sql);
	    statement.setInt(1, id);
	    statement.executeUpdate();
	    
	    for (Attribute attr : attrs) {
		
		sql = "insert into data(item_id, attr_name, attr_value) values (?, ?, ?)";
		statement = DaoUtility.getConnection().prepareStatement(sql);
		statement.setInt(1, id);
		statement.setString(2, attr.attribute);
		statement.setString(3, attr.value);
		int rowsInserted = statement.executeUpdate();
		
		if (rowsInserted != 1) {
		    throw new SQLException(String.format("Failed to insert attribute [%d, %s, %s]", id, attr.attribute, attr.value));
		}
		
	    }
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
	
    }
}
