package io.entity;

import com.google.gson.annotations.Expose;
import io.api.nodes.CentralNode;
import io.entity.type.Property;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Date;
import java.util.*;

/**
 * Will be used to create, and upload data in csv parsed to json format
 */
public class Table implements Serializable {


    @Expose
    private String name;
    @Expose
    private List<HeaderColumn> columnList;
    private HashMap<String, Integer> columnIndiceMap;

    private HashMap<Identifier, String> rows;
    private List<Index> indexes;
    private static Logger logger = Logger.getLogger(Table.class);

    public Table() {
        this.name = null;
        this.columnList = null;
        this.rows = new HashMap<>();
        this.columnIndiceMap = new HashMap<>();
        this.indexes = new ArrayList<Index>();
    }


    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    // Add an index to the list of indexes
    public void addIndex(Index index) {
        this.indexes.add(index);
    }

    public List<Index> getIndexes() {
        return indexes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Table table = (Table) o;
        return Objects.equals(name, table.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", columnList=" + columnList +
                ", rows=" + rows +
                ", indexes=" + indexes +
                '}';
    }

    public boolean hasColumn(String columnName) {
        return columnList.stream().anyMatch(column -> column.getName().equals(columnName));
    }

    public boolean loadRow(String row)  {
        // Test the size of the right size
        long numberOfElement = row.chars().filter(ch -> ch == ',').count() + 1;
        if (numberOfElement != columnList.size()) {
            logger.debug("The row length (" + numberOfElement + ") is not equals to the number of column (" + columnList.size() + ")");
            return false;
        }

        // Generate an ID
        Identifier rowIdentifier = new Identifier(UUID.randomUUID());

        //List<Property> rowStored = new ArrayList<>();
        String[] tokens = row.split(",");
        for (int i = 0; i < tokens.length; i++) {
            String rowElement = tokens[i];
            HeaderColumn column = columnList.get(i);
            // TODO: Better use ?
            Property property = null;
            Constructor<? extends Property> cons = null;
            try {
                cons = column.getType().getType().getConstructor();
                property = cons.newInstance();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            if (property == null) {
                logger.debug("Error while parsing " + rowElement);
                return false;
            }
            property.setValue(rowElement);
            //rowStored.add(property);
        }

        // TODO: PutAll ?
        //rows.put(rowIdentifier, row);

        // Compute Index
        for (Index index: indexes) {
            List<String> identifierArray = new ArrayList<>();
            for(String columnName: index.getColumnNames()) {
                identifierArray.add(tokens[columnIndiceMap.get(columnName)]);
            }
            String identifier = String.join(",", identifierArray);

            index.getValues().computeIfAbsent(identifier, k -> new ArrayList<>()).add(row);
        }

        return true;
    }

    public List<HeaderColumn> getColumnList() {
        return columnList;
    }

    public HashMap<String, Integer> getColumnIndiceMap() {
        return columnIndiceMap;
    }

    public HashMap<Identifier, String> getRows() {
        return rows;
    }
}
