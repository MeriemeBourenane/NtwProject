package io.api.nodes;

import io.entity.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/api/node")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Node {

    @POST
    @Path("/create-table")
    public Table createTable(Table table) {
        // the central node will receive the first request
        // processing the request create table

        System.out.println(table);

        return table;
    }

    @POST
    @Path("/create-index")
    public List<String> createIndex(@QueryParam("fields") List<String> fields) {
        // the central node will receive the first request
        // processing the request create index

        System.out.println(fields);

        return fields;
    }
}
