package com.datafibers.util;

import com.datafibers.model.DFJobPOPJ;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * List of help functions to be used for all DF classes.
 */
public class HelpFunc {

    static final int max = 1000;
    static final int min = 1;
    static final String underscore = "_";
    static final String period = ".";
    static final String space = " ";
    static final String colon = ":";

    /**
     * Loop the enum of ConnectType to add all connects to the list by l
     */
    public static void addSpecifiedConnectTypetoList(List<String> list, String type_regx) {

        for (ConstantApp.DF_CONNECT_TYPE item : ConstantApp.DF_CONNECT_TYPE.values())
			if (item.name().matches(type_regx))
				list.add(item.name());
    }

    /**
     * Return the first not null objects in the list of arguments
     * @param a
     * @param b
     * @param <T>
     * @return object
     */
    public static <T> T coalesce(T a, T b) {
        return a == null ? b : a;
    }

    /**
     * This function will search JSONSTRING to find patterned keys_1..n. If it has key_ignored_mark subkey, the element
     * will be removed. For example {"connectorConfig_1":{"config_ignored":"test"}, "connectorConfig_2":{"test":"test"}}
     * will be cleaned as {"connectorConfig":{"test":"test"}}
     *
     * This will also remove any comments in "\/* *\/"
     * This is deprecated once new UI is released
     *
     * @param jsonString
     * @param key_ingored_mark: If the
     * @return cleaned json string
     */
    public static String cleanJsonConfigIgnored(String jsonString, String key_pattern, String key_ingored_mark) {
        JSONObject json = new JSONObject(jsonString.replaceAll("\\s+?/\\*.*?\\*/", ""));
        int index = 0, index_found = 0;
        for (String json_key_to_check;;) {
			json_key_to_check = index != 0 ? key_pattern + index : key_pattern.replace("_", "");
			if (!json.has(json_key_to_check))
				break;
			if (!json.getJSONObject(json_key_to_check).has(key_ingored_mark))
				index_found = index;
			else
				json.remove(json_key_to_check);
			++index;
		}
        if (index_found > 0)
            json.put(key_pattern.replace("_", ""), json.getJSONObject(key_pattern + index_found)).remove(key_pattern + index_found);
        return json.toString();
    }

    /**
     * This function will search topics in connectorConfig. If the topics are array, convert it to string.
     *
     * @param jsonString
     * @return cleaned json string
     */
    public static String convertTopicsFromArrayToString(String jsonString, String topicsKeyAliasString) {
        JSONObject json = new JSONObject(jsonString.replaceAll("\\s+?/\\*.*?\\*/", ""));
        if(json.has("connectorConfig"))
			for (String topicsKey : topicsKeyAliasString.split(","))
				if (json.getJSONObject("connectorConfig").has(topicsKey)) {
					Object topicsObj = json.getJSONObject("connectorConfig").get(topicsKey);
					if (topicsObj instanceof JSONArray)
						json.getJSONObject("connectorConfig").put(topicsKey,
								((JSONArray) topicsObj).join(",").replace("\"", ""));
				}
        return json.toString();
    }

    /**
     * A default short-cut call for clean raw json for connectConfig.
     * List of cleaning functions will be called through this
     * @param JSON_STRING
     * @return cleaned json string
     */
    public static String cleanJsonConfig(String JSON_STRING) {
        return convertTopicsFromArrayToString(JSON_STRING, ConstantApp.PK_DF_TOPICS_ALIAS);
    }

    /**
     * Generate a file name for the UDF Jar uploaded to avoid naming conflict
     * @param inputName
     * @return fileName
     */
    public static String generateUniqueFileName(String inputName) {
        Date curDate = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String DateToStr = format.format(curDate).replaceAll(space, underscore).replaceAll(colon, underscore);

        int randomNum = min + new Random().nextInt(max - min + 1);

        if (inputName == null || inputName.indexOf(period) <= 0)
			DateToStr = inputName + underscore + DateToStr + underscore + randomNum;
		else {
			int firstPart = inputName.indexOf(period);
			DateToStr = inputName.substring(0, firstPart) + underscore + DateToStr + underscore + randomNum + "."
					+ inputName.substring(firstPart + 1);
		}

        return DateToStr;
    }

    /**
     * Get the current folder of the running jar file
     * @return jarFilePath
     */
    public String getCurrentJarRunnigFolder() {
        String jarPath = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();  // "/home/vagrant/df-data-service-1.0-SNAPSHOT-fat.jar";
        int i = jarPath.lastIndexOf("/");

        if (i > 0)
			jarPath = jarPath.substring(0, i + 1);

        return jarPath;
    }

    /**
     * Build program parameters for Flink Jar in rest call
     * @param j
     * @param kafkaRestHostName
     * @param SchemaRegistryRestHostName
     * @return
     */
    public static JsonObject getFlinkJarPara(DFJobPOPJ j, String kafkaRestHostName, String SchemaRegistryRestHostName ) {

        String allowNonRestoredState = "false", savepointPath = "", parallelism = "1", entryClass = "",
				programArgs = "";
        if (j.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_SQLA2A.name()) {
            entryClass = ConstantApp.FLINK_SQL_CLIENT_CLASS_NAME;
            programArgs = String.join(" ", kafkaRestHostName, SchemaRegistryRestHostName,
                    j.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_INPUT),
                    j.getConnectorConfig().get(ConstantApp.PK_KAFKA_TOPIC_OUTPUT),
                    j.getConnectorConfig().get(ConstantApp.PK_FLINK_TABLE_SINK_KEYS),
                    HelpFunc.coalesce(j.getConnectorConfig().get(ConstantApp.PK_KAFKA_CONSUMER_GROURP),
                            ConstantApp.DF_TRANSFORMS_KAFKA_CONSUMER_GROUP_ID_FOR_FLINK),
                    // Use 0 as we only support one query in flink
                    "\"" + sqlCleaner(j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_SQL))[0] + "\"");
        } else if (j.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_UDF.name()) {
            entryClass = j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_JAR_CLASS_NAME);
            programArgs = j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_JAR_PARA);
        } else if(j.getConnectorType() == ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_FLINK_Script.name()) {
            entryClass = j.getConnectorConfig().get(ConstantApp.FLINK_TABLEAPI_CLIENT_CLASS_NAME);
            programArgs = j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_JAR_PARA);
        }

        return new JsonObject()
                .put("allowNonRestoredState", allowNonRestoredState)
                .put("savepointPath", savepointPath)
                .put("parallelism", parallelism)
                .put("entryClass", entryClass)
                .put("programArgs", programArgs);
    }

    /**
     * Find mongo sorting options
     * @param c
     * @param sortField
     * @param sortOrderField
     * @return
     */
    public static FindOptions getMongoSortFindOption(RoutingContext c, String sortField, String sortOrderField) {
        String sortName = HelpFunc.coalesce(c.request().getParam(sortField), "_id");
        if("id".equalsIgnoreCase(sortName)) sortName = "_" + sortName; //Mongo use _id
        return new FindOptions().setSort(new JsonObject().put(sortName,
				HelpFunc.strCompare(HelpFunc.coalesce(c.request().getParam(sortOrderField), "ASC"), "ASC", 1, -1)));
    }

    public static FindOptions getMongoSortFindOption(RoutingContext c) {
        return getMongoSortFindOption(c, "_sort", "_order");
    }

    /**
     * Get Connector or Transform sub-task status from task array on specific status keys
     * {
     *  "name": "59852ad67985372a792eafce",
     *  "connector": {
     *  "state": "RUNNING",
     *  "worker_id": "127.0.1.1:8083"
     *  },
     *  "tasks": [
     *  {
     *      "state": "RUNNING",
     *      "id": 0,
     *      "worker_id": "127.0.1.1:8083"
     *  }
     *  ]
     * }
     * @param taskStatus Responsed json object
     * @return
     */
    public static String getTaskStatusKafka(JSONObject taskStatus) {
		if (!taskStatus.has("connector"))
			return ConstantApp.DF_STATUS.NONE.name();
		if (!taskStatus.getJSONObject("connector").getString("state")
				.equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()) || !taskStatus.has("tasks"))
			return taskStatus.getJSONObject("connector").getString("state");
		JSONArray subTask = taskStatus.getJSONArray("tasks");
		String status = ConstantApp.DF_STATUS.RUNNING.name();
		for (int i = 0; i < subTask.length(); ++i)
			if (!subTask.getJSONObject(i).getString("state").equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
				status = ConstantApp.DF_STATUS.RWE.name();
				break;
			}
		return status;
	}

    public static String getTaskStatusKafka(JsonObject taskStatus) {
		if (!taskStatus.containsKey("connector"))
			return ConstantApp.DF_STATUS.NONE.name();
		if (!taskStatus.getJsonObject("connector").getString("state")
				.equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()) || !taskStatus.containsKey("tasks"))
			return taskStatus.getJsonObject("connector").getString("state");
		JsonArray subTask = taskStatus.getJsonArray("tasks");
		String status = ConstantApp.DF_STATUS.RUNNING.name();
		for (int i = 0; i < subTask.size(); ++i)
			if (!subTask.getJsonObject(i).getString("state").equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
				status = ConstantApp.DF_STATUS.RWE.name();
				break;
			}
		return status;
	}

    /**
     * Mapping rest state to df status category
     * @param taskStatus
     * @return
     */
    public static String getTaskStatusFlink(JsonObject taskStatus) {
		if (!taskStatus.containsKey("state"))
			return ConstantApp.DF_STATUS.NONE.name();
		if (!taskStatus.getString("state").equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())
				|| !taskStatus.containsKey("vertices"))
			return taskStatus.getString("state");
		JsonArray subTask = taskStatus.getJsonArray("vertices");
		String status = ConstantApp.DF_STATUS.RUNNING.name();
		for (int i = 0; i < subTask.size(); ++i)
			if (!subTask.getJsonObject(i).getString("status").equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
				status = ConstantApp.DF_STATUS.RWE.name();
				break;
			}
		return status;
	}
    /**
     * Mapping livy statement rest state to df status category
     * @param taskStatus
     * @return
     */
    public static String getTaskStatusSpark(JsonObject taskStatus) {
		if (!taskStatus.containsKey("state"))
			return ConstantApp.DF_STATUS.NONE.name();
		String restState = taskStatus.getString("state").toUpperCase(), returnState;
		switch (restState) {
		case "RUNNING":
		case "CANCELLING":
			returnState = ConstantApp.DF_STATUS.RUNNING.name();
			break;
		case "WAITING":
			returnState = ConstantApp.DF_STATUS.UNASSIGNED.name();
			break;
		case "AVAILABLE": {
			returnState = (!"ok".equalsIgnoreCase(taskStatus.getJsonObject("output").getString("status"))
					? ConstantApp.DF_STATUS.FAILED
					: ConstantApp.DF_STATUS.FINISHED).name();
			break;
		}
		case "ERROR":
			returnState = ConstantApp.DF_STATUS.FAILED.name();
			break;
		case "CANCELLED":
			returnState = ConstantApp.DF_STATUS.CANCELED.name();
			break;
		default:
			returnState = restState;
			break;
		}
		return returnState;
	}

    /**
     * Search the json object in the list of keys contains specified values
     * @param keyRoot
     * @param keyString
     * @param containsValue
     * @return searchCondition
     */
    public static JsonObject getContainsTopics(String keyRoot, String keyString, String containsValue) {
        JsonArray ja = new JsonArray();
        for(String key : keyString.split(","))
			ja.add(new JsonObject().put(keyRoot + "." + key,
					new JsonObject().put("$regex", ".*" + containsValue + ".*")));
        return new JsonObject().put("$or", ja);

    }

    /**
     * Used to format the livy result to better rich text so that it can show in the web ui better
     * @param livyStatementResult
     * @return
     */
    public static String livyTableResultToRichText(JsonObject livyStatementResult) {
		String tableHeader = "<style type=\"text/css\">.myOtherTable { background-color:#FFFFFF;border-collapse:collapse;color:#000}"
				+ ".myOtherTable th { background-color:#99ceff;color:black;width:50%; }"
				+ ".myOtherTable td, .myOtherTable th { padding:5px;border:0; }"
				+ ".myOtherTable td { border-bottom:1px dotted #BDB76B; }</style><table class=\"myOtherTable\">",
				tableTrailer = "</table>", dataRow = "";
		if (!livyStatementResult.getJsonObject("output").containsKey("data"))
			return "";
		JsonObject dataJason = livyStatementResult.getJsonObject("output").getJsonObject("data");
		if (livyStatementResult.getString("code").contains("%table")) {
			JsonObject output = dataJason.getJsonObject("application/vnd.livy.table.v1+json");
			JsonArray header = output.getJsonArray("headers"), data = output.getJsonArray("data");
			String headerRow = "<tr><th>";
			if (data.isEmpty())
				return "";
			String separator = "</th><th>";
			for (int i = 0; i < header.size(); ++i) {
				if (i == header.size() - 1)
					separator = "";
				String headerName = header.getJsonObject(i).getString("name");
				if ("0".equalsIgnoreCase(headerName))
					headerName = "result";
				headerRow += headerName + separator;
			}
			headerRow += "</th></tr>";
			for (int i = 0; i < data.size(); ++i)
				dataRow += jsonArrayToString(data.getJsonArray(i), "<tr><td>", "</td><td>", "</td></tr>");
			return tableHeader + headerRow + dataRow + tableTrailer;
		}
		if (!livyStatementResult.getString("code").contains("%json"))
			return "<pre>" + dataJason.getString("text/plain").trim() + "</pre>";
		JsonArray data = dataJason.getJsonArray("application/json");
		if (data.isEmpty())
			return "";
		for (int i = 0; i < data.size(); ++i)
			dataRow += jsonArrayToString(data.getJsonArray(i), "<tr><td>", "</td><td>", "</td></tr>");
		return tableHeader + dataRow + tableTrailer;
	}

    /**
     * Used to format Livy statement result to a list a fields and types for schema creation. Only support %table now
     * @param livyStatementResult
     * @return string of fields with type
     */
    public static String livyTableResultToAvroFields(JsonObject livyStatementResult, String subject) {
		if (!livyStatementResult.getJsonObject("output").containsKey("data"))
			return "";
		JsonObject dataJason = livyStatementResult.getJsonObject("output").getJsonObject("data");
		if (!livyStatementResult.getString("code").contains("%table"))
			return "";
		JsonArray header = dataJason.getJsonObject("application/vnd.livy.table.v1+json").getJsonArray("headers");
		for (int i = 0; i < header.size(); ++i)
			header.getJsonObject(i).put("type", typeHive2Avro(header.getJsonObject(i).getString("type")));
		return new JsonObject().put("type", "record").put("name", subject).put("fields", header).toString();
	}

    public static String mapToJsonStringFromHashMapD2U(HashMap<String, String> m) {
        return mapToJsonFromHashMapD2U(m).toString();
    }

    /**
     * Utility to remove dot from json attribute to underscore for web ui
     * @param m
     * @return
     */
    public static JsonObject mapToJsonFromHashMapD2U(HashMap<String, String> m) {
        JsonObject json = new JsonObject();
        for (String key : m.keySet())
			json.put(key.replace('.', '_'), m.get(key));
        return json;
    }

    /**
     * Utility to replace underscore from json attribute to dot for kafka connect
     * @param m
     * @return
     */
    public static String mapToJsonStringFromHashMapU2D(HashMap<String, String> m) {
        return mapToJsonFromHashMapU2D(m).toString();
    }

    /**
     * Utility to replace underscore from json attribute to dot for kafka connect
     * @param m
     * @return
     */
    public static JsonObject mapToJsonFromHashMapU2D(HashMap<String, String> m) {
        JsonObject json = new JsonObject();
        for (String key : m.keySet())
			json.put(key.replace('_', '.'), m.get(key));
        return json;
    }

    /**
     * Utility to extract HashMap from json for DFPOPJ
     * @param o
     * @return
     */
    public static HashMap<String, String> mapToHashMapFromJson( JsonObject o) {
        HashMap<String, String> hm = new HashMap<>();
        for (String key : o.fieldNames())
			hm.put(key, o.getValue(key).toString());
        return hm;
    }

    /**
     * This is mainly to bypass security control for response.
     * @param r
     */
    public static HttpServerResponse responseCorsHandleAddOn(HttpServerResponse r) {
        return r
                .putHeader("Access-Control-Allow-Origin", "*")
                .putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
                .putHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, X-Total-Count")
                .putHeader("Access-Control-Expose-Headers", "X-Total-Count")
                .putHeader("Access-Control-Max-Age", "60")
                .putHeader("X-Total-Count", "1" ) // Overwrite this in the sub call
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET);
    }

    /**
     * Convert string to Json format by remove first " and end " and replace \" to "
     * @param srcStr String to format
     * @return String formatted
     */
    public static String stringToJsonFormat(String srcStr) {
		return srcStr.isEmpty() ? "[]"
				: srcStr.replace("\"{", "{").replace("}\"", "}").replace("\\\"", "\"").replace("\"\"", "\"");
	}

    /**
     * Comparing string ease of lambda expression
     * @param a
     * @param b
     * @param <T>
     * @return object
     */
    public static <T> T strCompare(String a, String b, T c, T d) {
        return a.equalsIgnoreCase(b) ? c : d;
    }

    /**
     * Sort JsonArray when we do not use mongodb
     * @param c
     * @param jsonArray
     * @return
     */
    public static JSONArray sortJsonArray(RoutingContext c, JSONArray jsonArray) {

        String sortKey = HelpFunc.coalesce(c.request().getParam("_sort"), "id"),
				sortOrder = HelpFunc.coalesce(c.request().getParam("_order"), "ASC");
        JSONArray sortedJsonArray = new JSONArray();

        List<JSONObject> jsonValues = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); ++i)
			jsonValues.add(jsonArray.getJSONObject(i));
        Collections.sort( jsonValues, new Comparator<JSONObject>() {

            @Override
            public int compare(JSONObject a, JSONObject o) {
                String valA = new String(), valB = new String();
                try {
                    valA = a.get(sortKey).toString();
                    valB = o.get(sortKey).toString();
                }
                catch (JSONException e) {
                    e.printStackTrace();
                }

                return "ASC".equalsIgnoreCase(sortOrder) ? valA.compareTo(valB) : -valA.compareTo(valB);
            }
        });

        for (int i = 0; i < jsonArray.length(); ++i)
			sortedJsonArray.put(jsonValues.get(i));
        return sortedJsonArray;
    }

    /**
     * Get input of sql statements and remove comments line, extract \n and extra ;
     * @param sqlInput
     * @return array of cleaned sql statement without ;
     */
    public static String[] sqlCleaner(String sqlInput) {
        //Use @ to create extra space and \n for removing comments
        sqlInput = sqlInput.replaceAll("\n", "@\n");
        String cleanedSQL = "";
        for(String line : sqlInput.split("\n"))
			cleanedSQL += StringUtils.substringBefore(line, "--");
        return cleanedSQL.replace("@", " ").split(";");
    }

    /**
     * Convert spark SQL to pyspark code. If stream the result back is needed, add additional code.
     * @return
     */
    public static String sqlToPySpark(String[] sqlList, boolean streamBackFlag, String streamPath) {

        String pySparkCode = "";
        if(!streamPath.startsWith("file://")) streamPath = "file://" + streamPath;

        for(int i = 0; i < sqlList.length; ++i)
			if (i != sqlList.length - 1)
				pySparkCode += "sqlContext.sql(\"" + sqlList[i] + "\")\n";
			else {
				if (streamBackFlag)
					pySparkCode += "sqlContext.sql(\"" + sqlList[i]
							+ "\").coalesce(1).write.format(\"json\").mode(\"overwrite\").save(\"" + streamPath
							+ "\")\n";
				pySparkCode += "a = sqlContext.sql(\"" + sqlList[i] + "\").show(10)\n%table a";
			}

        return pySparkCode;
    }

    /**
     * Convert standard SQL to spark scala api code. If stream the result back is needed, add additional code.
     * @return
     */
    public static String sqlToSparkScala(String[] sqlList, boolean streamBackFlag, String streamPath) {

        String pySparkCode = "";
        if(!streamPath.startsWith("file://")) streamPath = "file://" + streamPath;

        for(int i = 0; i < sqlList.length; ++i)
			if (i != sqlList.length - 1)
				pySparkCode += "spark.sql(\"" + sqlList[i] + "\")\n";
			else {
				if (streamBackFlag)
					pySparkCode += "spark.sql(\"" + sqlList[i]
							+ "\").coalesce(1).write.format(\"json\").mode(\"overwrite\").save(\"" + streamPath
							+ "\")\n";
				pySparkCode += "val a = spark.sql(\"" + sqlList[i] + "\").show()\n";
			}

        return pySparkCode;
    }

    /**
     * Convert json from web ui ml training guideline (include guide, mlsql, pyspark) to pyspark code.
     * @return scala code
     */
    public static String mlGuideToScalaSpark(JsonObject config){
        String code = "";
        if (!"FEATURE_SRC_FILE".equalsIgnoreCase(config.getString("feature_source")))
			return code;
		code = "val data = spark.read.format(\"libsvm\").load(\"" + config.getString("feature_source_value") + "\")\n"
				+ "val Array(trainingData, testData) = data.randomSplit(Array("
				+ Integer.parseInt(config.getString("feature_source_sample")) / 100 + ","
				+ (1 - Integer.parseInt(config.getString("feature_source_sample")) / 100) + "), seed = 1234L)";
		if ("ML_CLASS_CLF_NB".equalsIgnoreCase(config.getString("model_class_method")))
			code += "\nimport org.apache.spark.ml.classification.NaiveBayes\n"
					+ "val model = new NaiveBayes().fit(trainingData)";
		return code += "\nval predictions = model.transform(data)\npredictions.show()";
    }

    /**
     * Convert Json Array to String with proper begin, separator, and end string.
     * @param a
     * @param begin
     * @param separator
     * @param end
     * @return
     */
    public static String jsonArrayToString(JsonArray a, String begin, String separator, String end) {
        for (int i = 0; i < a.size(); ++i) {
            if(i == a.size() - 1) separator = "";
            begin += a.getValue(i).toString() + separator;
        }
        return begin + end;
    }

    /**
     * Map hive type (from Livy statement result) to avro schema type
     * @param hiveType
     * @return type in Avro
     */
    private static String typeHive2Avro(String hiveType) {

        String avroType;
        switch (hiveType) {
		case "NULL_TYPE":
			avroType = "null";
			break;
		case "BYTE_TYPE":
			avroType = "bytes";
			break;
		default:
			avroType = "string";
			break;
		case "BOOLEAN_TYPE":
			avroType = "boolean";
			break;
		case "INT_TYPE":
			avroType = "int";
			break;
		case "DOUBLE_TYPE":
			avroType = "double";
			break;
		case "FLOAT_TYPE":
		case "DECIMAL_TYPE":
			avroType = "float";
			break;
		}
        return avroType;
    }

    /**
     * Upload Flink client to flink rest server
     * @param postURL
     * @param jarFilePath
     * @return
     */
    public static String uploadJar(String postURL, String jarFilePath) {
        HttpResponse<String> jsonResponse = null;
        try {
            jsonResponse = Unirest.post(postURL)
                    .field("file", new File(jarFilePath))
                    .asString();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        JsonObject response = new JsonObject(jsonResponse.getBody());
        return !response.containsKey("filename") ? "" : response.getString("filename");
    }
    /**
     * Helper class to update mongodb status
     * @param c
     * @param COLLECTION
     * @param updateJob
     * @param l
     */
    public static void updateRepoWithLogging(MongoClient c, String COLLECTION, DFJobPOPJ updateJob, Logger l) {
        c.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", updateJob.toJson()), v -> {
                    if (!v.failed())
						l.info(DFAPIMessage.logResponseMessage(1021, updateJob.getId()));
					else
						l.error(DFAPIMessage.logResponseMessage(9003, updateJob.getId() + "cause:" + v.cause()));
                }
        );
    }
}
