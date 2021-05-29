package io.entity;

import com.google.gson.annotations.Expose;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

public class HeaderColumn implements Serializable {

    @Expose
    private String name;
    @Expose
    private DataTypes type;

    public HeaderColumn() {
        this.name = null;
        this.type = null;
    }

    public String getName() {
        return name;
    }

    public DataTypes getType() {
        return type;
    }

    @Override
    public String toString() {
        return "TableHeaderColumns{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }

}
