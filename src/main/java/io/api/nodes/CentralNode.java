package io.api.nodes;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.app.App;
import io.entity.Index;
import io.entity.Table;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.log4j.Logger;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import javax.ws.rs.*;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.util.*;

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


    public Response createTable(Table table, boolean forwarded) {
        // the central node will receive the first request
        // processing the request create table
        // forwarding the 2 others : by a request
        logger.debug("Entering /create-table");
        // TODO : verify type od columns
        if (app.isValidTable(table)) {
            logger.info("The object is valid");
            this.app.addTable(table);

            // Create ColumnName -> index Map
            for (int i = 0; i < table.getColumnList().size(); i++) {
                String columnName = table.getColumnList().get(i).getName();
                table.getColumnIndiceMap().put(columnName, i);
            }

            logger.debug(table);

            // Send the table to peers
            if (!forwarded) {
                Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().serializeNulls().create();
                okhttp3.MediaType mediaType = okhttp3.MediaType.parse("application/json");
                RequestBody body = RequestBody.create(gson.toJson(table, Table.class), mediaType);
                Request.Builder request = new Request.Builder()
                        .method("POST", body)
                        .addHeader("Content-Type", "application/json");
                app.sendToPeers(request, "tables");
            }
            return Response.status(Response.Status.CREATED).build();
        }

        return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The object is invalid")).build();
    }

    @POST
    @Path("/tables")
    public Response createTable(Table table) {
        return createTable(table, false);
    }

    @POST
    @Path("/forward/tables")
    public Response createTableForward(Table table) {
        return createTable(table, true);
    }

    public Response createIndex(Index index, String tableName, boolean forwarded) {
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

            // Send the table to peers
            if (!forwarded) {
                Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().serializeNulls().create();
                okhttp3.MediaType mediaType = okhttp3.MediaType.parse("application/json");
                RequestBody body = RequestBody.create(gson.toJson(index, Index.class), mediaType);
                Request.Builder request = new Request.Builder()
                        .method("POST", body)
                        .addHeader("Content-Type", "application/json");
                app.sendToPeers(request, "tables/" + tableName + "/indexes");
            }

            return Response.status(Response.Status.CREATED).build();
        }

        return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The object is incorrect")).build();
    }

    @POST
    @Path("/tables/{tableName}/indexes")
    public Response createIndex(Index index,
                                @PathParam("tableName") String tableName) {
        return createIndex(index, tableName, false);
    }

    @POST
    @Path("/forward/tables/{tableName}/indexes")
    public Response createIndexForward(Index index,
                                       @PathParam("tableName") String tableName) {
        return createIndex(index, tableName, true);
    }

    @POST
    @Path("/tables/{tableName}/csv")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response loadCSV(@PathParam("tableName") String tableName,
                            MultipartFormDataInput input) {
        logger.debug("Loaded CSV");

        Table table = this.app.getTableByName(tableName);
        if (table == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The table does not exist")).build();
        }

        //Get API input data
        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();

        //Get file data to save
        List<InputPart> inputParts = uploadForm.get("attachment");

        int numberOfPeers = app.getNumberOfPeers();
        int batch = 1000;
        int currentLines = 0;
        int currentPeer = 0;
        List<StringBuilder> peersBuffer = new ArrayList<>();
        for (int i = 0; i < numberOfPeers; i++) {
            peersBuffer.add(new StringBuilder());
        }

        for (InputPart inputPart : inputParts) {
            logger.debug("Reading a InputPart");
            boolean firstLine = true;
            try {

                // convert the uploaded file to inputstream
                InputStream inputStream = inputPart.getBody(InputStream.class, null);
                BufferedReader bufferReader = new BufferedReader(new InputStreamReader(inputStream));
                while (bufferReader.ready()) {
                    String line = bufferReader.readLine();
                    if (firstLine) {
                        firstLine = false;
                        continue;
                    }
                    if (currentPeer == numberOfPeers) {
                        // Load the data into the table
                        if (!table.loadRow(line)) {
                            return Response.status(Response.Status.BAD_REQUEST).build();
                        }
                        currentLines++;
                    } else {
                        if (currentLines == batch) {
                            Request.Builder request = new Request.Builder()
                                    .addHeader("Content-Type", "multipart/form-data");
                            app.sendToPeers(request, "tables/" + tableName + "/csv", peersBuffer);
                            currentLines = 0;
                        }
                        // Concat to the StringBuilder
                        if (currentLines != 0) {
                            peersBuffer.get(currentPeer).append("\n");
                        }
                        peersBuffer.get(currentPeer).append(line);

                    }
                    currentPeer = (currentPeer + 1) % (numberOfPeers + 1);

                }
            } catch (Exception e) {
                e.printStackTrace();
                return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
            }
        }

        // Send remaining
        Request.Builder request = new Request.Builder()
                .addHeader("Content-Type", "multipart/form-data");
        app.sendToPeers(request, "tables/" + tableName + "/csv", peersBuffer);

        logger.debug("The size of the table is " + table.getIndexes().get(0).getValues().values().size());

        return Response.status(Response.Status.OK).build();
    }

    @POST
    @Path("/forward/tables/{tableName}/csv")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response loadCSVForward(@PathParam("tableName") String tableName,
                            MultipartFormDataInput input) {
        logger.debug("Forward Loaded CSV");

        Table table = this.app.getTableByName(tableName);
        if (table == null) {
            return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The table does not exist")).build();
        }

        //Get API input data
        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();

        //Get file data to save
        List<InputPart> inputParts = uploadForm.get("attachment");

        for (InputPart inputPart : inputParts) {
            logger.debug("Reading a InputPart");
            boolean firstLine = true;
            try {
                // convert the uploaded file to inputstream
                InputStream inputStream = inputPart.getBody(InputStream.class, null);
                BufferedReader bufferReader = new BufferedReader(new InputStreamReader(inputStream));
                while (bufferReader.ready()) {
                    String line = bufferReader.readLine();
                    // Load the data into the table
                    if (!table.loadRow(line)) {
                        return Response.status(Response.Status.BAD_REQUEST).build();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
            }
        }

        logger.debug("The size of the table is " + table.getIndexes().get(0).getValues().values().size());
        return Response.status(Response.Status.OK).build();
    }

    // WARNING: We must have an index with these columns
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
            // Check that there is an index with these columns
            Optional<Index> appropriateIndex = table.getIndexes().stream().filter(i -> i.getColumnNames().equals(columnsName)).findFirst();

            // TODO: Improve without allocating new memory
            Map<String, String> valueMap = new HashMap<>();
            for (int i = 0; i < columnsName.size(); i++) {
                valueMap.put(columnsName.get(i), values.get(i));
            }

            if (appropriateIndex.isPresent()) {
                // Reorder the values
                List<String> identifierArray = new ArrayList<>();
                for (String columnName : appropriateIndex.get().getColumnNames()) {
                    identifierArray.add(valueMap.get(columnName));
                }
                String targetValue = String.join(",", identifierArray);

                List<String> result = appropriateIndex.get().getValues().getOrDefault(targetValue, new ArrayList<>());

                CacheControl cacheControl = new CacheControl();
                cacheControl.setNoCache(true);
                cacheControl.setMaxAge(-1);
                cacheControl.setMustRevalidate(true);
                return Response.status(Response.Status.OK).cacheControl(cacheControl).entity((StreamingOutput) outputStream -> {
                    // Start streaming the data
                    try (PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outputStream)))) {

                        writer.print("[");
                        boolean firstElement = true;
                        for (String res : result) {
                            if (firstElement) {
                                firstElement = false;
                            } else {
                                writer.write(",");
                            }
                            writer.write("\"" + res + "\"");
                        }

                        // Done!
                        writer.print("]");
                    }
                }).build();
            } else {
                return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("No index is matching your research")).build();

            }
        }

        return Response.status(Response.Status.BAD_REQUEST).entity(formatErrorMessage("The object is incorrect")).build();
    }




}
