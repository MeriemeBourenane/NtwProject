package io.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

public class TableRow implements Serializable {

    private HashMap<Integer, EntryTable> row;

    public TableRow() {
        this.row = new HashMap<Integer, EntryTable>();
    }

    public TableRow(Integer id, EntryTable entry) {
        this.row = new HashMap<Integer, EntryTable>();
        this.row.put(id, entry);
    }

    public void setRow(Integer id, EntryTable entry) {
        if (row.isEmpty()) {
            this.row.put(id, entry);
        }
    }

    public HashMap<Integer, EntryTable> getRow() {
        return row;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableRow tableRow = (TableRow) o;
        return Objects.equals(row, tableRow.row);
    }

    @Override
    public int hashCode() {
        return Objects.hash(row);
    }

    @Override
    public String toString() {
        return "TableRow{" +
                "row=" + row +
                '}';
    }
}
