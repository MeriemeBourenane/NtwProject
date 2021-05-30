import okhttp3.*;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RESTAPITest {


    @Test
    @Order(1)
    public void createTable() throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder().build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create( "{\n    \"name\": \"yellow-2009\",\n    \"columnList\": [\n        {\n            \"name\": \"vendor_name\",\n            \"type\" : \"STRING\"\n        },\n                {\n            \"name\": \"Trip_Pickup_DateTime\",\n            \"type\" : \"DATE\"\n        },\n                {\n            \"name\": \"Trip_Dropoff_DateTime\",\n            \"type\" : \"DATE\"\n        },\n                {\n            \"name\": \"Passenger_Count\",\n            \"type\" : \"INTEGER\"\n        },\n                {\n            \"name\": \"Trip_Distance\",\n            \"type\" : \"FLOAT\"\n        },\n                {\n            \"name\": \"Start_Lon\",\n            \"type\" : \"FLOAT\"\n        },\n                {\n            \"name\": \"Start_Lat\",\n            \"type\" : \"FLOAT\"\n        },\n                {\n            \"name\": \"Rate_Code\",\n            \"type\" : \"STRING\"\n        },\n                {\n            \"name\": \"store_and_forward\",\n            \"type\" : \"STRING\"\n        },\n                        {\n            \"name\": \"End_Lon\",\n            \"type\" : \"FLOAT\"\n        },\n                        {\n            \"name\": \"End_Lat\",\n            \"type\" : \"FLOAT\"\n        },\n                        {\n            \"name\": \"Payment_Type\",\n            \"type\" : \"STRING\"\n        },\n                        {\n            \"name\": \"Fare_Amt\",\n            \"type\" : \"STRING\"\n        },\n                        {\n            \"name\": \"surcharge\",\n            \"type\" : \"STRING\"\n        },\n                {\n            \"name\": \"mta_tax\",\n            \"type\" : \"STRING\"\n        },\n                {\n            \"name\": \"Tip_Amt\",\n            \"type\" : \"STRING\"\n        },\n                {\n            \"name\": \"Tolls_Amt\",\n            \"type\" : \"STRING\"\n        },\n                {\n            \"name\": \"Total_Amt\",\n            \"type\" : \"STRING\"\n        }\n    ]\n}",mediaType);
        Request request = new Request.Builder()
                .url("http://localhost:8080/api/central-node/tables")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();

        assertEquals(javax.ws.rs.core.Response.Status.CREATED.getStatusCode(), response.code());
    }

    @Test
    @Order(2)
    public void createIndex() throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder().build();
        MediaType mediaType = MediaType.parse("application/json");
        RequestBody body = RequestBody.create( "{\n    \"name\": \"VendorID\",\n    \"columnNames\": [ \"vendor_name\"]\n}",mediaType);
        Request request = new Request.Builder()
                .url("http://localhost:8080/api/central-node/tables/yellow-2009/indexes")
                .method("POST", body)
                .addHeader("Content-Type", "application/json")
                .build();
        Response response = client.newCall(request).execute();

        assertEquals(javax.ws.rs.core.Response.Status.CREATED.getStatusCode(), response.code());
    }

    @Test
    @Order(3)
    public void loadCSV() throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
                                                .writeTimeout(0, TimeUnit.MILLISECONDS)
                                                .callTimeout(0, TimeUnit.MILLISECONDS)
                                                .readTimeout(0, TimeUnit.MILLISECONDS)
                                                .connectTimeout(0, TimeUnit.MILLISECONDS)
                                                .build();
        MediaType mediaType = MediaType.parse("multipart/form-data");
        RequestBody body = new MultipartBody.Builder().setType(MultipartBody.FORM)
                .addFormDataPart("attachment", "yellow_tripdata_2009-01.csv",
                        RequestBody.create(
                                new File("/tmp/yellow_tripdata_2009-01.csv"),MediaType.parse("application/octet-stream")))
                .addFormDataPart("fileName", "test1.csv")
                .build();
        Request request = new Request.Builder()
                .url("http://localhost:8080/api/central-node/tables/yellow-2009/csv")
                .method("POST", body)
                .addHeader("Content-Type", "multipart/form-data")
                .build();
        Response response = client.newCall(request).execute();

        assertEquals(javax.ws.rs.core.Response.Status.OK.getStatusCode(), response.code());
    }

    @Test
    @Order(4)
    public void getIndex() throws IOException {
        OkHttpClient client = new OkHttpClient().newBuilder()
                .build();
        Request request = new Request.Builder()
                .url("http://localhost:8080/api/central-node/tables/yellow-2009/indexes?column=vendor_name&value=VTS")
                .method("GET", null)
                .build();
        Response response = client.newCall(request).execute();
        assertEquals(javax.ws.rs.core.Response.Status.OK.getStatusCode(), response.code());

    }

}
