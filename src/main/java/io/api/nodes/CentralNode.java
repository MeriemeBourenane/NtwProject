package io.api.nodes;

import io.app.App;
import io.entity.Index;
import io.entity.Table;
import io.entity.TestObj;
import org.apache.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;

/**
 * The central node will receive request and will give instructions to the slave nodes
 */
@Path("/api/central-node")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CentralNode {

    private App app = App.getApp();
    private static Logger logger = Logger.getLogger(CentralNode.class);


    public String formatErrorMessage(String message) {
        return "{ \"error\": \"" + message + "\"}";
    }

    @POST
    @Path("/tables")
    public Response createTable(Table table) {
        // the central node will receive the first request
        // processing the request create table
        // forwarding the 2 others : by a request
        logger.debug("Entering /create-table");
        // TODO : verify type od columns
        if (table != null
                && table.getName() != null
                && table.getTableHeaderColumns() != null) {
            logger.info("The object is valid");
            this.app.addTable(table);
            logger.debug(table);
            return Response.status(Response.Status.CREATED).build();
        }

        return Response.status(Response.Status.BAD_REQUEST).entity("The object is invalid").build();
    }

    @POST
    @Path("/tables/{tableName}/indexes")
    public Response createIndex(Index index,
                                @PathParam("tableName") String tableName) {
        // the central node will receive the first request
        // processing the request create index
        // forwarding the 2 others : by a request
        logger.debug("Entering /tables/{tableName}/indexes");

        if (this.app.getTableByName(tableName) == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The table does not exist")).build();
        }
        if (index.getName() != null && index.getColumnNames() != null) {
            if (index.getColumnNames().isEmpty()) {
                return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The index must have at least one column")).build();
            }

            for (String field : index.getColumnNames()) {
                if (!this.app.getTableByName(tableName).getTableHeaderColumns().getHeaderColumns().containsKey(field)) {
                    return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("This table doesn't contains the field : " + field)).build();
                }
            }
            logger.debug("Adding index...");
            this.app.getTableByName(tableName).addIndex(index);

            logger.debug(this.app.getTables());

            return Response.status(Response.Status.CREATED).build();
        }

        return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The object is incorrect")).build();
    }

    @POST
    @Path("/load-csv")
    //@Consumes(MediaType.)
    public TestObj loadCSV(TestObj t) {

        System.out.println(t);
        return t;
    }

    @GET
    @Path("/get-table-by-name/{name}")
    public Table getTable(@PathParam("name") String name) {
        System.out.println(this.app.getTableByName(name));
        return this.app.getTableByName(name);
    }

    /*
    @POST
    @Path("/get-table/{aTest}")
    public Table setTestTable(@PathParam("aTest") String aTest) {
        this.app.getaTable().setTest(aTest);
        System.out.println(this.app.getaTable());
        return this.app.getaTable();
    }
    */



}
