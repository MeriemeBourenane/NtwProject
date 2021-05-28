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
    private HashMap<Integer, Integer> index;

    public Index() {
        this.name = null;
        this.columnNames = null;
        this.index = new HashMap<>();
    }

    public Index(String name, List<String> columnNames) {
        this.name = name;
        this.columnNames = columnNames;
        this.index = null;
    }

    public int generateId(String element) {
        return element.hashCode();
    }

    public String getName() {
        return name;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public HashMap<Integer, Integer> getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "Index{" +
                "name='" + name + '\'' +
                ", columnNames=" + columnNames +
                ", index=" + index +
                '}';
    }
}
