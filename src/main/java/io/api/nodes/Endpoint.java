package io.api.nodes;

import io.entity.Table;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class Endpoint {

    @POST
    @Path("/table/create-table")
    public Table createTable(Table table) {
        System.out.println(table);

        return table;
    }

    @GET
    @Path("/table/get-table")
    public Table getTable () {


        return new Table();
    }

}
