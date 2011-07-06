package com.criticcomrade.etl.query.db;

import java.sql.*;
import java.util.*;

import com.criticcomrade.etl.query.AttributeConstants;
import com.criticcomrade.etl.query.data.*;

public class DataItemDao extends EtlDao {
    
    public DataItemDao(Connection conn) {
	super(conn);
    }
    
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
    
    public boolean putDataItem(DataItem item) {
	
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
    
    private boolean writeAttributes(DataItem item) {
	
	// First ensure the item is backed
	boolean changed = ensureItemIsBacked(item);
	
	// Get the subitems link attributes (all should be backed)
	Collection<Attribute> attrs = new ArrayList<Attribute>();
	for (DataItem subitem : item.getSubItems()) {
	    attrs.add(new Attribute(subitem.getType(), String.format("%d", subitem.getId())));
	}
	
	// Write all the attributes
	attrs.addAll(item.getAttributes());
	if (putItemAttributes(item.getId(), attrs)) {
	    changed = true;
	}
	
	return changed;
	
    }
    
    private boolean ensureItemIsBacked(DataItem item) {
	
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
    
    private int findItemByAttributes(String type, Collection<Attribute> keys) {
	
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
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
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
    
    private Collection<Attribute> getItemAttrs(int id) {
	
	try {
	    
	    String sql = "select attr_name, attr_value from data where item_id = ?";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
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
    
    private int addNewDataItem() {
	
	try {
	    
	    String sql = "insert into item_queue() values ()";
	    
	    PreparedStatement statement = conn.prepareStatement(sql);
	    int rowsAdded = statement.executeUpdate();
	    
	    if (rowsAdded != 1) {
		throw new SQLException("Incorrect number of rows added to item_queue [" + rowsAdded + "]");
	    }
	    
	    sql = "select LAST_INSERT_ID()";
	    statement = conn.prepareStatement(sql);
	    ResultSet rs = statement.executeQuery();
	    
	    int id = 0;
	    if (rs.next()) {
		id = rs.getInt(1);
	    }
	    
	    if (id == 0) {
		throw new SQLException("Could not find last id [" + id + "]");
	    }
	    
	    rs.close();
	    statement.close();
	    
	    return id;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    private boolean putItemAttributes(int id, Collection<Attribute> attrs) {
	
	List<Attribute> toAdd = new ArrayList<Attribute>();
	List<Attribute> toRemove = new ArrayList<Attribute>();
	buildDbDifferencesLists(id, attrs, toAdd, toRemove);
	
	if (toAdd.isEmpty() && toRemove.isEmpty()) {
	    
	    return false; // No changes
	    
	} else {
	    
	    try {
		
		// Remove the old attrs
		StringBuilder sql;
		List<String> args;
		PreparedStatement statement;
		
		if (!toRemove.isEmpty()) {
		    sql = new StringBuilder("delete from data where item_id = ? and (");
		    args = new ArrayList<String>();
		    for (int i = 0; i < toRemove.size(); i++) {
			if (i > 0) {
			    sql.append(" or ");
			}
			sql.append(" (attr_name = ? and attr_value = ?)");
			args.add(toRemove.get(i).attribute);
			args.add(toRemove.get(i).value);
		    }
		    sql.append(")");
		    
		    statement = conn.prepareStatement(sql.toString());
		    statement.setInt(1, id);
		    int i = 2;
		    for (String arg : args) {
			statement.setString(i++, arg);
		    }
		    statement.executeUpdate();
		}
		// Add the new attrs
		sql = new StringBuilder("insert into data(item_id, attr_name, attr_value) values");
		
		if (!toAdd.isEmpty()) {
		    args = new ArrayList<String>();
		    boolean firstAttr = true;
		    for (Attribute attr : toAdd) {
			
			if (firstAttr) {
			    firstAttr = false;
			} else {
			    sql.append(", ");
			}
			
			sql.append("(?, ?, ?)");
			args.add(attr.attribute);
			args.add(attr.value);
			
		    }
		    
		    statement = conn.prepareStatement(sql.toString());
		    
		    for (int i = 0; i < args.size(); i += 2) {
			statement.setInt(i / 2 * 3 + 1, id);
			statement.setString(i / 2 * 3 + 2, args.get(i));
			statement.setString(i / 2 * 3 + 3, args.get(i + 1));
		    }
		    
		    int rowsInserted = statement.executeUpdate();
		    if (rowsInserted != toAdd.size()) {
			throw new SQLException(String.format("Failed to insert attributes [%d, %s]", id, toRemove.toString()));
		    }
		}
		
	    } catch (SQLException e) {
		throw new RuntimeException(e);
	    }
	    
	    return true;
	    
	}
	
    }
    
    private void buildDbDifferencesLists(int id, Collection<Attribute> attrs, Collection<Attribute> toAdd, Collection<Attribute> toRemove) {
	
	Collection<Attribute> dbAttrs = (new DataItemDao(conn)).getItemAttrs(id);
	
	for (Attribute attr : attrs) {
	    if (!dbAttrs.contains(attr)) {
		toAdd.add(attr);
	    }
	}
	
	for (Attribute attr : dbAttrs) {
	    if (!attrs.contains(attr)) {
		toRemove.add(attr);
	    }
	}
	
    }
}
