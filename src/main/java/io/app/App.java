package io.app;

import io.entity.Index;
import io.entity.Table;
import okhttp3.*;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    public boolean sendToPeers(Request.Builder requestBuilder, String method) {
        List<Thread> threadList = new ArrayList<>();
        for (InetSocketAddress address :
                peers) {
            Request request = requestBuilder.url("http://" + address.getAddress().getHostAddress() + ":" + address.getPort() + "/api/central-node/forward/" + method).build();
            Thread th = new Thread(() -> {
                OkHttpClient client = new OkHttpClient().newBuilder().build();
                try {
                    Response response = client.newCall(request).execute();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            threadList.add(th);
            th.start();
        }

        for (Thread th: threadList) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return true;

    }

    public boolean sendToPeers(Request.Builder requestBuilder, String method, List<StringBuilder> data) {
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < peers.size(); i++) {
            InetSocketAddress address = peers.get(i);
            StringBuilder peerData = data.get(i);
            // Check that there is any data to send
            if (peerData.length() == 0) {
                continue;
            }
            RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
                    .addFormDataPart("attachment", "forward",
                            RequestBody.create(
                                    peerData.toString(), MediaType.parse("application/octet-stream")))
                    .build();
            Request request = requestBuilder
                    .url("http://" + address.getAddress().getHostAddress() + ":" + address.getPort() + "/api/central-node/forward/" + method)
                    .method("POST", body)
                    .build();
            Thread th = new Thread(() -> {
                OkHttpClient client = new OkHttpClient().newBuilder()
                        .writeTimeout(0, TimeUnit.MILLISECONDS)
                        .callTimeout(0, TimeUnit.MILLISECONDS)
                        .readTimeout(0, TimeUnit.MILLISECONDS)
                        .connectTimeout(0, TimeUnit.MILLISECONDS)
                        .build();
                try {
                    Response response = client.newCall(request).execute();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            threadList.add(th);
            th.start();
        }

        for (Thread th : threadList) {
            try {
                th.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        data.forEach(s -> s.setLength(0));
        return true;

    }

    public int getNumberOfPeers() {
        return peers.size();
    }



}
