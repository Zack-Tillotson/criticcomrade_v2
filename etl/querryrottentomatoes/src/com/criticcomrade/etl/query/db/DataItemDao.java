package com.criticcomrade.etl.query.db;

import java.sql.*;
import java.util.*;

import com.criticcomrade.etl.data.*;
import com.criticcomrade.etl.query.AttributeConstants;

public class DataItemDao extends AbstractDao {
    
    public DataItemDao(Connection conn) {
	super(conn);
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
	
	// Find which item we're looking at
	int id;
	try {
	    id = findItemIdByAttributes(item);
	} catch (AmbiguousQueryException e) {
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
    
    private int findItemIdByAttributes(DataItem item) throws AmbiguousQueryException {
	
	Collection<Attribute> keys = new ArrayList<Attribute>();
	for (Attribute attr : item.getAttributes()) {
	    if (AttributeConstants.getKeyAttributes().get(item.getType()).contains(attr.attribute)) {
		keys.add(attr);
	    }
	}
	
	keys.add(new Attribute(AttributeConstants.TYPE, item.getType()));
	
	List<Integer> ids = findItemIdsByAttributes(keys);
	
	if (ids.size() == 1) {
	    return ids.get(0).intValue();
	}
	
	throw new AmbiguousQueryException();
	
    }
    
    public DataItem findItemByAttributes(List<Attribute> keys) throws BadDataItemException {
	Collection<DataItem> ret = findItemsByAttributes(keys);
	if (ret.size() > 0) {
	    return ret.iterator().next();
	} else {
	    return null;
	}
    }
    
    public Collection<DataItem> findItemsByAttributes(List<Attribute> keys) throws BadDataItemException {
	
	findItemIdsByAttributes(keys);
	
	List<Integer> ids = findItemIdsByAttributes(keys);
	
	// Load the data item for each item id
	Collection<DataItem> ret = new ArrayList<DataItem>();
	for (Integer id : ids) {
	    ret.add(loadDataItem(id));
	}
	
	return ret;
	
    }
    
    private List<Integer> findItemIdsByAttributes(Collection<Attribute> keys) {
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
	    
	    List<Integer> ids = new ArrayList<Integer>();
	    
	    ResultSet res = statement.executeQuery();
	    while (res.next()) {
		ids.add(res.getInt(1)); // Found the item
	    }
	    
	    res.close();
	    statement.close();
	    
	    return ids;
	    
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
    
    private DataItem loadDataItem(int id) throws BadDataItemException {
	
	// Get attributes
	Collection<Attribute> attrs = loadAttributes(id);
	
	// Get subitems
	Collection<DataItem> subItems = new ArrayList<DataItem>();
	for (Attribute attr : attrs) {
	    if (AttributeConstants.getObjectTypeAttributeNames().contains(attr.attribute)) {
		subItems.add(loadDataItem(id));
	    }
	}
	
	// Find the type
	String type = null;
	for (Attribute attr : attrs) {
	    if (attr.attribute.equals(AttributeConstants.TYPE)) {
		type = attr.value;
	    }
	}
	if (type == null) {
	    throw new BadDataItemException();
	}
	
	return new LoadedDataItem(type, subItems, attrs);
	
    }
    
    private Collection<Attribute> loadAttributes(int id) {
	
	try {
	    
	    // Remove the old attrs
	    StringBuilder sql = new StringBuilder("select attr_name, attr_value from data where item_id = ?");
	    PreparedStatement statement = conn.prepareStatement(sql.toString());
	    statement.setInt(1, id);
	    
	    ResultSet rs = statement.executeQuery();
	    
	    List<Attribute> ret = new ArrayList<Attribute>();
	    while (rs.next()) {
		final String attrName = rs.getString(1);
		final String attrValue = rs.getString(2);
		ret.add(new Attribute(attrName, attrValue));
	    }
	    
	    return ret;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
	
    }
    
    public boolean setAttribute(int id, String name, String value) {
	
	try {
	    
	    // Remove the old attr
	    StringBuilder sql = new StringBuilder("delete from data where item_id = ? and attr_name = ?");
	    PreparedStatement statement = conn.prepareStatement(sql.toString());
	    statement.setInt(1, id);
	    statement.setString(2, name);
	    
	    int changed = statement.executeUpdate();
	    
	    sql = new StringBuilder("insert into data (item_id, attr_name, attr_value) values (?, ?, ?)");
	    statement = conn.prepareStatement(sql.toString());
	    statement.setInt(1, id);
	    statement.setString(2, name);
	    statement.setString(3, value);
	    
	    statement.executeUpdate();
	    
	    return changed > 0;
	    
	} catch (SQLException e) {
	    throw new RuntimeException(e);
	}
    }
    
    private class LoadedDataItem extends DataItem {
	
	private final Collection<DataItem> subItems;
	private final Collection<Attribute> attrs;
	
	public LoadedDataItem(String type, Collection<DataItem> subItems, Collection<Attribute> attrs) {
	    super(type);
	    this.subItems = subItems;
	    this.attrs = attrs;
	}
	
	@Override
	protected Collection<DataItem> buildSubItems() {
	    return subItems;
	}
	
	@Override
	protected Collection<Attribute> getDirectAttributes() {
	    return attrs;
	}
    }
    
}
