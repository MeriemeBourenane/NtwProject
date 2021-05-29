package io.entity.type;

public abstract class Property<T>{

    protected T value;

    public abstract void setValue(String input);

    @Override
    public String toString() {
        return "" + value;
    }
}
