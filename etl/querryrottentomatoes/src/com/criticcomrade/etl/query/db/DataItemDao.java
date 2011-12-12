package com.criticcomrade.etl.query.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import com.criticcomrade.etl.data.Attribute;
import com.criticcomrade.etl.data.DataItem;
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

        synchronized (conn) {

            // First ensure the item is backed
            boolean changed = ensureItemIsBacked(item);

            // Write all the attributes (by this point all subitems are backed)
            Collection<Attribute> attrs = item.getAttributes();
            if (putItemAttributes(item.getId(), attrs)) {
                changed = true;
            }

            return changed;

        }

    }

    private boolean ensureItemIsBacked(DataItem item) {

        // See if the db attributes are different from the current ones
        boolean changed = false;

        // Find which item we're looking at
        Integer id;
        try {
            id = findItemIdByAttributes(item);
            if (id == null) {
                id = addNewDataItem();
                changed = true;
            }
        } catch (AmbiguousQueryException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        item.setId(id);

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

    public DataItem findItemById(String id) throws AmbiguousQueryException, NumberFormatException, BadDataItemException {
        return loadDataItem(Integer.parseInt(id));
    }

    private Integer findItemIdByAttributes(DataItem item) throws AmbiguousQueryException {

        List<Integer> ids = findItemIdsByAttributes(item.getAttributes());

        if (ids.size() == 1) {
            return ids.get(0).intValue();
        } else if (ids.size() == 0) {
            return null;
        } else {
            // TODO Manually figure out which is correct since multiple items can have same hash
            throw new AmbiguousQueryException(item.getAttributes().toString() + " => " + ids.toString());
        }

    }

    private Collection<Attribute> buildKeysForDataItem(Collection<Attribute> attrs) throws AmbiguousQueryException {

        String type = null;
        for (Attribute attr : attrs) {
            if (attr.attribute.equals(AttributeConstants.TYPE)) {
                type = attr.value;
            }
        }
        if (type == null) {
            throw new AmbiguousQueryException("No type attribute specified for data item [" + attrs.toString() + "]");
        }

        Collection<Attribute> keys = new ArrayList<Attribute>();
        for (String keyName : AttributeConstants.getKeyAttributes().get(type)) {
            boolean found = false;
            for (Attribute attr : attrs) {
                if (keyName.equals(attr.attribute)) {
                    keys.add(attr);
                    found = true;
                    break;
                }
            }
            if (!found) {
                keys.add(new Attribute(keyName, null));
            }
        }

        keys.add(new Attribute(AttributeConstants.TYPE, type));
        return keys;
    }

    public DataItem findItemByAttributes(List<Attribute> attrs) throws BadDataItemException {
        Collection<DataItem> ret = findItemsByAttributes(attrs);
        if (ret.size() > 0) {
            return ret.iterator().next();
        } else {
            return null;
        }
    }

    public Collection<DataItem> findItemsByAttributes(List<Attribute> attrs) throws BadDataItemException {

        List<Integer> ids = findItemIdsByAttributes(attrs);

        // Load the data item for each item id
        Collection<DataItem> ret = new ArrayList<DataItem>();
        for (Integer id : ids) {
            ret.add(loadDataItem(id));
        }

        return ret;

    }

    private List<Integer> findItemIdsByAttributes(Collection<Attribute> attrs) {

        try {

            Collection<Attribute> keys = buildKeysForDataItem(attrs);

            String sql = "select item_id from item_queue where hash = ? order by 1 asc";

            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setInt(1, keys.hashCode());

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
        } catch (AmbiguousQueryException e) {
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

    /**
     * Synchronized so that we return the correct item_id for each item
     * 
     * @return
     */
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
                    statement.close();
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
                    statement.close();

                    if (rowsInserted != toAdd.size()) {
                        throw new SQLException(String.format("Failed to insert attributes [%d, %s]", id, toRemove
                                .toString()));
                    }
                }

                try {
                    refreshItemHash(id, attrs);
                } catch (AmbiguousQueryException e) {
                    throw new SQLException("Unable to build keys", e);
                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            return true;

        }

    }

    private void refreshItemHash(int id, Collection<Attribute> attrs) throws AmbiguousQueryException {

        Collection<Attribute> keys = buildKeysForDataItem(attrs);

        try {

            StringBuilder sql;
            PreparedStatement statement;

            sql = new StringBuilder("update item_queue set hash = ? where item_id = ?");

            statement = conn.prepareStatement(sql.toString());
            statement.setInt(1, keys.hashCode());
            statement.setInt(2, id);

            statement.executeUpdate();
            statement.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildDbDifferencesLists(int id, Collection<Attribute> attrs, Collection<Attribute> toAdd,
            Collection<Attribute> toRemove) {

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
        Collection<Attribute> attrs = loadAttributes(id, true);

        // Get subitems
        Collection<DataItem> subItems = new ArrayList<DataItem>();
        for (Attribute attr : attrs) {
            if (AttributeConstants.getObjectTypeAttributeNames().contains(attr.attribute)) {
                subItems.add(loadDataItem(Integer.parseInt(attr.value)));
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

        return new LoadedDataItem(id, type, subItems, attrs);

    }

    private Collection<Attribute> loadAttributes(int id, boolean sync) {

        if (sync) {
            synchronized (conn) {
                return loadAttributesImpl(id);
            }
        } else {
            return loadAttributesImpl(id);
        }

    }

    private Collection<Attribute> loadAttributesImpl(int id) {
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

            rs.close();
            statement.close();

            return ret;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean setAttribute(int id, String name, String value) {

        synchronized (conn) {

            try {

                // If the old attribute doesn't exist or isn't the same
                StringBuilder sql = new StringBuilder(
                        "select * from data where item_id = ? and attr_name = ? and attr_value = ?");
                PreparedStatement statement = conn.prepareStatement(sql.toString());
                statement.setInt(1, id);
                statement.setString(2, name);
                statement.setString(3, value);

                ResultSet rs = statement.executeQuery();
                boolean found = rs.next();

                rs.close();
                statement.close();

                if (found) {
                    return false;
                } else {

                    // Remove the old attr
                    sql = new StringBuilder("delete from data where item_id = ? and attr_name = ?");
                    statement = conn.prepareStatement(sql.toString());
                    statement.setInt(1, id);
                    statement.setString(2, name);

                    statement.executeUpdate();
                    statement.close();

                    // Add the attr
                    sql = new StringBuilder("insert into data (item_id, attr_name, attr_value) values (?, ?, ?)");
                    statement = conn.prepareStatement(sql.toString());
                    statement.setInt(1, id);
                    statement.setString(2, name);
                    statement.setString(3, value);

                    statement.executeUpdate();
                    statement.close();

                    try {
                        refreshItemHash(id, loadAttributes(id, false));
                    } catch (AmbiguousQueryException e) {
                        throw new RuntimeException("Unable to refresh hash");
                    }

                    return true;

                }

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }

    }

    private class LoadedDataItem extends DataItem {

        private final Collection<DataItem> subItems;
        private final Collection<Attribute> attrs;

        public LoadedDataItem(int id, String type, Collection<DataItem> subItems, Collection<Attribute> attrs) {
            super(type);
            this.id = id;
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

    public boolean getHasChangedSincePush(DataItem item) {

        // If attributes have been changed, it's changed
        if (getChangedAttributes(item.getId()).size() > 0) {
            return true;
        }

        // Check sub items
        for (DataItem subItem : item.getSubItems()) {
            if (getHasChangedSincePush(subItem)) {
                return true;
            }
        }

        // If it hasn't been pushed yet, it's changed
        if (getHasObjectBeenPushed(item.getId())) {
            return true;
        } else {
            return false;
        }

    }

    private List<Attribute> getChangedAttributes(int id) {
        try {

            StringBuilder sql = new StringBuilder(
                    "select attr_name, attr_value from data where item_id = ? and (date_pushed is null or date_pushed < date_entered)");
            PreparedStatement statement = conn.prepareStatement(sql.toString());
            statement.setInt(1, id);

            ResultSet rs = statement.executeQuery();

            List<Attribute> ret = new ArrayList<Attribute>();
            while (rs.next()) {
                final String attrName = rs.getString(1);
                final String attrValue = rs.getString(2);
                ret.add(new Attribute(attrName, attrValue));
            }

            rs.close();
            statement.close();

            return ret;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean getHasObjectBeenPushed(int id) {
        try {

            StringBuilder sql = new StringBuilder("select 1 from item_queue where item_id = ? and date_pushed is null");
            PreparedStatement statement = conn.prepareStatement(sql.toString());
            statement.setInt(1, id);

            ResultSet rs = statement.executeQuery();

            try {
                if (rs.next()) {
                    return true;
                } else {
                    return false;
                }
            } finally {
                rs.close();
                statement.close();
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getPushedMoviesWithChanges() {
        try {

            // TODO FIX
            final String sql = "select * from (select item_id from data where attr_name = 'TYPE' and attr_value = 'MOVIE' and date_pushed is not null) pushed_movies, (select item_id, attr_name, attr_value from data where date_pushed is not null and date_pushed < date_entered) changed_attrs, data items where pushed_movies.item_id = changed_attrs.item_id and items.item_id = changed_attrs.item_id";

            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();

            List<String> ret = new ArrayList<String>();
            while (rs.next()) {
                // TODO ret.add(rs.getString(1));
            }

            rs.close();
            statement.close();

            return ret;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getNonPushedMovies() {
        try {

            final String sql = "select item_id from data where attr_name = 'TYPE' and attr_value = 'MOVIE' and date_pushed is null";

            PreparedStatement statement = conn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();

            List<String> ret = new ArrayList<String>();
            while (rs.next()) {
                ret.add(rs.getString(1));
            }

            rs.close();
            statement.close();

            return ret;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updatePushDate(List<Integer> idList, Date when) {

        StringBuilder ids = new StringBuilder();
        for (Integer id : idList) {
            ids = ids.append(id + ",");
        }
        ids.setLength(ids.length() - 1);

        try {

            String sql = "update data set date_pushed = ? where item_id in (" + ids + ")";

            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setTimestamp(1, new java.sql.Timestamp(new Date().getTime() - 1000 * 60 * 60 * 24));
            int changedCount = statement.executeUpdate();

            statement.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try {

            String sql = "update item_queue set date_pushed = ? where item_id in (" + ids + ")";

            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setTimestamp(1, new java.sql.Timestamp(new Date().getTime() - 1000 * 60 * 60 * 24));
            int changedCount = statement.executeUpdate();

            statement.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int cleanOldData() {
        try {

            String sql = "delete from data where date_entered < subdate(now(), interval 3 month) and date_pushed is not null";

            PreparedStatement statement = conn.prepareStatement(sql);
            int changedCount = statement.executeUpdate();

            statement.close();

            return changedCount;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
