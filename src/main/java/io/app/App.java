package io.app;

import io.api.nodes.CentralNode;
import io.entity.Index;
import io.entity.Table;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class App {

    private static App INSTANCE_APP = null;
    private Table aTable;
    private List<Table> tables;

    private List<InetSocketAddress> peers;
    private final static String networkPath = "/var/ntw/config/network.txt";
    private static Logger logger = Logger.getLogger(App.class);



    private App() {
        this.aTable = new Table();
        this.tables = new ArrayList<Table>();
        this.peers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(networkPath))) {
            String line = br.readLine();

            while (line != null) {
                String[] tokens = line.split(":");
                peers.add(new InetSocketAddress(tokens[0], Integer.valueOf(tokens[1])));
                logger.info("Adding new peer (" + tokens[0] + ","+ Integer.valueOf(tokens[1])+")");
                line = br.readLine();
            }
        } catch (IOException e) {
            logger.error("Error while reading network files");
            return;
        }

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
                && table.getColumnList() != null
                && ! hasTable(table.getName())
                && ! table.getColumnList().isEmpty();
    }

    public boolean isValidIndex(String tableName, Index index) {
        return index != null
                && index.getName() != null
                && index.getColumnNames() != null
                && ! hasIndex(tableName, index.getName())
                && ! index.getColumnNames().isEmpty()
                && index.getColumnNames().stream().allMatch(getTableByName(tableName)::hasColumn);
    }

    public boolean isValidSearch(String tableName, List<String> columns, List<String> values) {
        // TODO: Add type check
        return columns != null
                && values != null
                && columns.size() == values.size()
                && columns.stream().allMatch(getTableByName(tableName)::hasColumn);
    }



}
