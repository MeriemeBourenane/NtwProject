package io.entity;

public enum DataTypes {

    STRING("String"),
    INTEGER("Integer"),
    DATE("Date"),
    FLOAT("Float");

    public final String value;

    private DataTypes(String value) {
        this.value = value;
    }
}
