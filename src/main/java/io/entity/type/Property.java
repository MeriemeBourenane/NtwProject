package io.entity.type;

public abstract class Property<T>{

    protected T value;

    // Convert a String into a type T
    public abstract void setValue(String input);

    @Override
    public String toString() {
        return "" + value;
    }
}
