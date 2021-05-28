package io.app;

import io.entity.Index;
import io.entity.Table;

import java.util.ArrayList;
import java.util.List;

public class App {

    private static App INSTANCE_APP = null;
    private Table aTable;
    private List<Table> tables;

    private App() {
        this.aTable = new Table();
        this.tables = new ArrayList<Table>();
    }

    public static App getApp() {
        if (INSTANCE_APP == null) {
            INSTANCE_APP = new App();
        }
        return INSTANCE_APP;
    }

    // Add a table to the list of tables
    public void addTable (Table table) {
        this.tables.add(table);
    }

    // Get all the tables - get the list of tables
    public List<Table> getTables() {
        return this.tables;
    }

    // Get a table by name
    public Table getTableByName(String name) {

        return this.tables.stream()
                .filter(table -> name.equals(table.getName()))
                .findAny()
                .orElse(null);
    }

    private boolean hasIndex(String tableName, String indexName) {
        return getTableByName(tableName).getIndexes().stream().anyMatch(index -> index.getName().equals(indexName));
    }

    private boolean hasTable(String name) {
        return tables.stream().anyMatch(table -> table.getName().equals(name));
    }

    public boolean isValidTable(Table table) {
        return table != null
                && table.getName() != null
                && table.getTableHeaderColumns() != null
                && ! hasTable(table.getName())
                && ! table.getTableHeaderColumns().getHeaderColumns().isEmpty();
    }

    public boolean isValidIndex(String tableName, Index index) {
        return index != null
                && index.getName() != null
                && index.getColumnNames() != null
                && ! hasIndex(tableName, index.getName())
                && ! index.getColumnNames().isEmpty()
                && index.getColumnNames().stream().allMatch(getTableByName(tableName)::hasColumn);
    }
}
