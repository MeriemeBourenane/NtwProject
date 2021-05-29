package io.entity.type;

public class FloatProperty extends Property<Float> {
    @Override
    public void setValue(String input) {
        this.value = Float.valueOf(input);
    }
}
