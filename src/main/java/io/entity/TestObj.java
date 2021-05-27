package io.entity;

import java.io.Serializable;

public class TestObj implements Serializable {

    private String name;

    public TestObj(String name) {
        this.name = name;
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
