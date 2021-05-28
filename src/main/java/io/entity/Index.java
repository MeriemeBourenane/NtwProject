package io.entity;

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Will be used to create and search into an index
 */
public class Index implements Serializable {

    @Expose
    private String name;
    @Expose
    private List<String> columnNames;
    /**
     * String is the value of the columns concatenated with ,
     * List<Identifier> is the list of rows with this value
     */
    private HashMap<String, List<Identifier>> values;

    public Index() {
        this.name = null;
        this.columnNames = null;
        this.values = new HashMap<>();
    }

    public Index(String name, List<String> columnNames) {
        this.name = name;
        this.columnNames = columnNames;
        this.values = null;
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }


    @Override
    public String toString() {
        return "Index{" +
                "name='" + name + '\'' +
                ", columnNames=" + columnNames +
                ", index=" + values +
                '}';
    }

}
