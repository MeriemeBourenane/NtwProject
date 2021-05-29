package io.entity;

import java.util.UUID;

public class Identifier {

    private String id;

    public Identifier() {
        this.id = null;
    }

    public Identifier(UUID id) {
        this.id = id.toString();
    }

    public String getId() {
        return id;
    }
}
