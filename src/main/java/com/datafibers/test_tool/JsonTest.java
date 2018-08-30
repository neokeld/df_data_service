package com.datafibers.test_tool;

import com.datafibers.util.HelpFunc;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.codec.DecoderException;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by DUW3 on 11/11/2016.
 */
public class JsonTest {

    public static String arrayToString(JsonArray a) {
        String result = "";
        for (int i = 0; i < a.size(); ++i)
			result += a.getValue(i).toString() + ",";
        return result.substring(0, result.length() - 1);
    }

    public static JsonArray livyTableResultToArray(JsonObject livyStatementResult) {
        JsonObject output = livyStatementResult
                .getJsonObject("output")
                .getJsonObject("data")
                .getJsonObject("application/vnd.livy.table.v1+json");

        JsonArray header = output.getJsonArray("headers"), data = output.getJsonArray("data"), result = new JsonArray();
        JsonObject headerRowJson = new JsonObject();
        String headerRow = "";

        if(header.isEmpty()) return new JsonArray().add(new JsonObject().put("row", ""));

        for(int i = 0; i < header.size(); ++i)
			headerRow += header.getJsonObject(i).getString("name") + ",";

        result.add(headerRowJson.put("row", headerRow));

        for(int i = 0; i < data.size(); ++i)
			result.add(new JsonObject().put("row", arrayToString(data.getJsonArray(i))));

        return result;
    }

    public static void main(String[] args) throws IOException, DecoderException {
        Arrays.asList(HelpFunc.sqlCleaner("--comments \nselect * from test;")).forEach(System.out::println);

    }
}
