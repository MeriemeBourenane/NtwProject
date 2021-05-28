package io.entity;

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Will be used to create, and upload data in csv parsed to json format
 */
public class Table implements Serializable {

    @Expose
    private String name;
    @Expose
    private TableHeaderColumns tableHeaderColumns;
    private List<TableRow> listRows;
    private List<Index> indexes;

    public Table() {
        this.name = null;
        this.tableHeaderColumns = null;
        this.listRows = new ArrayList<>();
        this.indexes = new ArrayList<Index>();
    }


    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public TableHeaderColumns getTableHeaderColumns() {

        return tableHeaderColumns;
    }

    public void setTableHeaderColumns(TableHeaderColumns tableHeaderColumns) {

        this.tableHeaderColumns = tableHeaderColumns;
    }

    public void setListRows(List<TableRow> listRows) {

        if (this.tableHeaderColumns != null && !this.tableHeaderColumns.getHeaderColumns().isEmpty()) {
            this.listRows = listRows;
        }
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
                ", tableHeaderColumns=" + tableHeaderColumns +
                ", listRows=" + listRows +
                ", indexes=" + indexes +
                '}';
    }
}
