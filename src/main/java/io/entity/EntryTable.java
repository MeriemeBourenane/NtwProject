package io.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class EntryTable implements Serializable {

    private List<String> entries;

    public EntryTable() {
        this.entries = new ArrayList<String>();
    }

    public EntryTable(List<String> entries) {
        this.entries = entries;
    }

    public void addEntry(String newEntry) {
        this.entries.add(newEntry);
    }

    public void addEntries(List<String> newEntries) {
        this.entries.addAll(newEntries);
    }

    @Override
    public String toString() {
        return "EntryTable{" +
                "entries=" + entries +
                '}';
    }
}
