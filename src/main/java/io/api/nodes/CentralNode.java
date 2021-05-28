package io.api.nodes;

import io.app.App;
import io.entity.Index;
import io.entity.Table;
import org.apache.log4j.Logger;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.List;
import java.util.Map;

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
        if (app.isValidTable(table)) {
            logger.info("The object is valid");
            this.app.addTable(table);
            logger.debug(table);
            return Response.status(Response.Status.CREATED).build();
        }

        return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The object is invalid")).build();
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
        if (app.isValidIndex(tableName, index)) {
            logger.debug("Adding index...");
            this.app.getTableByName(tableName).addIndex(index);

            logger.debug(this.app.getTables());

            return Response.status(Response.Status.CREATED).build();
        }

        return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The object is incorrect")).build();
    }

    @POST
    @Path("/tables/{tableName}/csv")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response loadCSV(@PathParam("tableName") String tableName,
                            MultipartFormDataInput input) {
        logger.debug("Loaded CSV");

        //Get API input data
        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
        logger.debug(uploadForm);
        //Get file name
        String fileName = "";
        try {
            fileName = uploadForm.get("fileName").get(0).getBodyAsString();
            logger.info("Get the file " + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Get file data to save
        List<InputPart> inputParts = uploadForm.get("attachment");

        for (InputPart inputPart : inputParts)
        {
            try
            {

                // convert the uploaded file to inputstream
                InputStream inputStream = inputPart.getBody(InputStream.class, null);
                BufferedReader bufferReader = new BufferedReader(new InputStreamReader(inputStream));
                while (bufferReader.ready()) {
                    bufferReader.readLine();
                }
                System.out.println("Success !!!!!");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        return Response.status(Response.Status.OK).build();
    }

    @GET
    @Path("/tables/{tableName}/indexes")
    public Response getIndex(@PathParam("tableName") String tableName,
                             @QueryParam("column") List<String> columnsName,
                             @QueryParam("value") List<String> values) {
        logger.debug("Entering /tables/{tableName}/indexes");

        Table table = this.app.getTableByName(tableName);
        if (table == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The table does not exist")).build();
        }
        logger.debug("Column: " + columnsName + " Values: " + values);
        if (app.isValidSearch(tableName, columnsName, values)) {
            return Response.status(Response.Status.OK).build();

        }

        return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The object is incorrect")).build();
    }




}
