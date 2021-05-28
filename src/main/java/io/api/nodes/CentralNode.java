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
    @Path("/create-table")
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
    @Path("/create-index")
    public Response.ResponseBuilder createIndex(@QueryParam("nameIndex") String nameIndex,
                                                @QueryParam("nameTable") String nameTable,
                                                @QueryParam("fields") List<String> fields) {
        // the central node will receive the first request
        // processing the request create index
        // forwarding the 2 others : by a request
        System.out.println(nameIndex);
        System.out.println(nameTable);
        System.out.println(fields);

        if (nameIndex != null && nameTable != null && fields != null && this.app.getTableByName(nameTable) != null) {
            for (String field : fields) {
                if (!this.app.getTableByName(nameTable).getTableHeaderColumns().getHeaderColumns().containsKey(field)) {
                    System.out.println("[CentralNode->createIndex] This table doesn't contains the field : " + field);
                    return Response.status(400);
                }
            }
            System.out.println("CentralNode->createIndex] Adding index...");
            this.app.getTableByName(nameTable).addIndex(new Index(nameIndex, fields));
            System.out.println(this.app.getTables());

            return Response.status(200);
        }

        return Response.status(400);
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
