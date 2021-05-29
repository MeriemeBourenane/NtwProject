package io.entity;

import io.entity.type.*;

import java.util.Date;

public enum DataTypes {

    STRING(StringProperty.class),
    INTEGER(IntegerProperty.class),
    DATE(DateProperty.class),
    FLOAT(FloatProperty.class);

    public Class<? extends Property> type;

    private DataTypes(Class<? extends Property> value) {
        this.type = value;
    }

    public Class<? extends Property> getType() {
        return type;
    }
}
