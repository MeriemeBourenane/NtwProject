package io.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

public class TableHeaderColumns implements Serializable {

    private HashMap<String, DataTypes> headerColumns;

    public TableHeaderColumns() {

    }

    public void setHeaderColumns(HashMap<String, DataTypes> headerColumn) {

        this.headerColumns = headerColumn;
    }

    public HashMap<String, DataTypes> getHeaderColumns() {

        return headerColumns;
    }

    @Override
    public String toString() {
        return "TableHeaderColumns{" +
                "headerColumns=" + headerColumns +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableHeaderColumns that = (TableHeaderColumns) o;
        return Objects.equals(headerColumns, that.headerColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headerColumns);
    }
}
