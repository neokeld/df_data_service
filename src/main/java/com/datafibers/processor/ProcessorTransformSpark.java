package com.datafibers.processor;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.DFAPIMessage;
import com.datafibers.util.HelpFunc;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * This is the utility class to communicate with Spark through Apache Livy Rest Service.
 */

public class ProcessorTransformSpark {
    private static final Logger LOG = Logger.getLogger(ProcessorTransformSpark.class);

    /**
     * ForwardPostAsAddJar is a generic function to submit any spark jar to the livy.
     * This function is equal to the spark-submit. Submit status will refreshed in status thread separately.
     */
    public static void forwardPostAsAddJar(Vertx v, WebClient c, DFJobPOPJ j, MongoClient mongo,
                                           String taskCollection, String sparkRestHost, int sparkRestPort) {
        // TODO to be implemented by livy batch api set
    }

    /**
     * ForwardPostAsAddOne is a generic function to submit pyspark code taking sql statement to the livy.
     * This function will not response df ui. Since the UI is refreshed right way. Submit status will refreshed in status
     * thread separately.
     *
     * @param c vertx web client for rest
     * @param j jd job object
     * @param mongo mongodb client
     * @param taskCollection mongo collection name to keep df tasks
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     * @param v used to initial blocking rest call for session status check
     */
    public static void forwardPostAsAddOne(Vertx v, WebClient c, DFJobPOPJ j, MongoClient mongo,
                                           String taskCollection, String sparkRestHost, int sparkRestPort,
                                           String rawResBody) {
        String taskId = j.getId();

        // Check all sessions submit a idle session. If all sessions are busy, create a new session
        c.get(sparkRestPort, sparkRestHost,
                ConstantApp.LIVY_REST_URL_SESSIONS)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .send(sar -> {
                            if (sar.succeeded()) {
                                String idleSessionId = "";
                                JsonArray sessionArray = sar.result().bodyAsJsonObject().getJsonArray("sessions");

                                for (int i = 0; i < sessionArray.size(); ++i) {
									if ("idle".equalsIgnoreCase(sessionArray.getJsonObject(i).getString("state"))) {
										idleSessionId = sessionArray.getJsonObject(i).getInteger("id").toString();
										break;
									}
								}

                                if (!"".equalsIgnoreCase(idleSessionId)) {
									addStatementToSession(c, j, sparkRestHost, sparkRestPort, mongo, taskCollection,
											idleSessionId, rawResBody);
								} else {
									c.post(sparkRestPort, sparkRestHost, ConstantApp.LIVY_REST_URL_SESSIONS)
											.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
													ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
											.sendJsonObject(new JsonObject().put("name", "df"), ar -> {
												if (!ar.succeeded()) {
													LOG.error(DFAPIMessage.logResponseMessage(9010,
															taskId + " Start new session failed with details - "
																	+ ar.cause()));
												} else {
													String newSessionId = ar.result().bodyAsJsonObject()
															.getInteger("id").toString();
													j.setJobConfig(ConstantApp.PK_LIVY_SESSION_ID, newSessionId);
													String restURL = "http://" + sparkRestHost + ":" + sparkRestPort
															+ ConstantApp.LIVY_REST_URL_SESSIONS + "/" + newSessionId
															+ "/state";
													WorkerExecutor executor = v.createSharedWorkerExecutor(taskId,
															ConstantApp.WORKER_POOL_SIZE, ConstantApp.MAX_RUNTIME);
													executor.executeBlocking(future -> {
														for (HttpResponse<JsonNode> res;;) {
															try {
																res = Unirest.get(restURL).header(
																		ConstantApp.HTTP_HEADER_CONTENT_TYPE,
																		ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
																		.asJson();
																if ("idle".equalsIgnoreCase(
																		res.getBody().getObject().getString("state"))) {
																	break;
																}
																Thread.sleep(2000);
															} catch (UnirestException | InterruptedException e) {
																LOG.error(DFAPIMessage.logResponseMessage(9006,
																		"exception - " + e.getCause()));
															}
														}
														addStatementToSession(c, j, sparkRestHost, sparkRestPort, mongo,
																taskCollection, newSessionId, rawResBody);
													}, res -> {
													});
												}
											});
								}
                            }
                });
    }

    /**
     * This method cancel a session by sessionId through livy rest API.
     * Job may not exist or got exception or timeout. In this case, just delete it for now.
     * If session Id is in idle, delete the session too.
     *
     * @param c  response for rest client
     * @param webClient web client for rest
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     * @param mongoClient repo handler
     * @param mongoCOLLECTION collection to keep data
     * @param sessionId The livy session ID to cancel the job
     */
    public static void forwardDeleteAsCancelOne(RoutingContext c, WebClient webClient,
                                                MongoClient mongoClient, String mongoCOLLECTION,
                                                String sparkRestHost, int sparkRestPort, String sessionId) {
        String id = c.request().getParam("id");
        if (sessionId == null || sessionId.trim().isEmpty()) {
			LOG.error(DFAPIMessage.logResponseMessage(9000, "sessionId is null in task " + id));
		} else {
			webClient.get(sparkRestPort, sparkRestHost, ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/state")
					.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
					.send(sar -> {
						if (!sar.succeeded() || sar.result().statusCode() != ConstantApp.STATUS_CODE_OK
								|| !"idle".equalsIgnoreCase(sar.result().bodyAsJsonObject().getString("state"))) {
							mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
									mar -> HelpFunc.responseCorsHandleAddOn(c.response())
											.setStatusCode(ConstantApp.STATUS_CODE_OK)
											.end(DFAPIMessage.getResponseMessage(1002, id)));
							LOG.info(DFAPIMessage.logResponseMessage(1002, id));
						} else {
							webClient
									.delete(sparkRestPort, sparkRestHost,
											ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId)
									.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
											ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
									.send(ar -> {
										if (!ar.succeeded()) {
											HelpFunc.responseCorsHandleAddOn(c.response())
													.setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
													.end(DFAPIMessage.getResponseMessage(9029));
											LOG.info(DFAPIMessage.logResponseMessage(9029, id));
										} else {
											int response = (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK)
													? 1002
													: 9012;
											mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
													mar -> HelpFunc.responseCorsHandleAddOn(c.response())
															.setStatusCode(ConstantApp.STATUS_CODE_OK)
															.end(DFAPIMessage.getResponseMessage(response, id)));
											LOG.info(DFAPIMessage.logResponseMessage(response, id));
										}
									});
						}
					});
		}
    }

    /**
     * This method is to update a task by creating livy session and resubmit the statement to livy.
     * Update should always use current session. If it is busy, the statement is in waiting state.
     * If the sesseion is closed or session id is not available, use new/idle session.
     * @param v  response for rest client
     * @param c vertx web client for rest
     * @param sparkRestHost flinbk rest hostname
     * @param sparkRestPort flink rest port number
     * @param mongoClient repo handler
     * @param taskCollection collection to keep data
     */
    public static void forwardPutAsUpdateOne(Vertx v, WebClient c,
                                             DFJobPOPJ j, MongoClient mongoClient,
                                             String taskCollection, String sparkRestHost, int sparkRestPort) {
        // When session id is not available, use new/idle session to add new statement
        if(j.getJobConfig() == null ||
                (j.getJobConfig() != null && !j.getJobConfig().containsKey(ConstantApp.PK_LIVY_STATEMENT_ID))) {
			forwardPostAsAddOne(v, c, j, mongoClient, taskCollection, sparkRestHost, sparkRestPort, "");
		} else {
            String sessionId = j.getJobConfig().get(ConstantApp.PK_LIVY_STATEMENT_ID);
            c.get(sparkRestPort, sparkRestHost,
                    ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/state")
                    .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                            ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .send(sar -> { // If session is active, we always wait
                                if (!sar.succeeded() || sar.result().statusCode() != ConstantApp.STATUS_CODE_OK) {
									forwardPostAsAddOne(v, c, j, mongoClient, taskCollection, sparkRestHost,
											sparkRestPort, "");
								} else {
									addStatementToSession(c, j, sparkRestHost, sparkRestPort, mongoClient,
											taskCollection, sessionId, "");
								}
                    });
        }
    }

    /**
     * This method first decode the REST GET request to DFJobPOPJ object. Then, it updates its job status and repack
     * for REST GET. After that, it forward the GET to Livy API to get session and statement status including logging.
     * Once REST API forward is successful, response. Right now, this is not being used by web ui.
     *
     * @param c response for rest client
     * @param webClient This is vertx non-blocking web client used for forwarding
     * @param sparkRestHost flink rest hostname
     * @param sparkRestPort flink rest port number
     */
    public static void forwardGetAsJobStatus(RoutingContext c, WebClient webClient, DFJobPOPJ j,
                                             String sparkRestHost, int sparkRestPort) {
        String sessionId = j.getJobConfig().get(ConstantApp.PK_LIVY_SESSION_ID),
				statementId = j.getJobConfig().get(ConstantApp.PK_LIVY_STATEMENT_ID), taskId = j.getId();
        if (sessionId == null || sessionId.trim().isEmpty() || statementId == null || statementId.trim().isEmpty()) {
            LOG.warn(DFAPIMessage.logResponseMessage(9000, taskId));
            HelpFunc.responseCorsHandleAddOn(c.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000, taskId,
                            "Cannot Get State Without Session Id/Statement Id."));
        } else {
			webClient
					.get(sparkRestPort, sparkRestHost,
							ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + ConstantApp.LIVY_REST_URL_STATEMENTS)
					.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
					.send(ar -> {
						if (!ar.succeeded() || ar.result().statusCode() != ConstantApp.STATUS_CODE_OK) {
							HelpFunc.responseCorsHandleAddOn(c.response())
									.setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
									.end(DFAPIMessage.getResponseMessage(9029, taskId,
											"Cannot Found State for job " + statementId));
							LOG.info(DFAPIMessage.logResponseMessage(9029, taskId));
						} else {
							JsonObject jo = ar.result().bodyAsJsonObject();
							System.out.println("get status = " + jo);
							JsonArray subTaskArray = jo.getJsonArray("statements"), statusArray = new JsonArray();
							for (int i = 0; i < subTaskArray.size(); ++i) {
								statusArray
										.add(new JsonObject()
												.put("subTaskId",
														sessionId + "_"
																+ subTaskArray.getJsonObject(i).getInteger("id"))
												.put("id",
														taskId + "_" + subTaskArray.getJsonObject(i).getInteger("id"))
												.put("jobId", sessionId)
												.put("dfTaskState",
														HelpFunc.getTaskStatusSpark(subTaskArray.getJsonObject(i)))
												.put("taskState",
														subTaskArray.getJsonObject(i).getString("state").toUpperCase())
												.put("statement", subTaskArray.getJsonObject(i).getString("code"))
												.put("output", HelpFunc
														.livyTableResultToRichText(subTaskArray.getJsonObject(i))));
							}
							System.out.println("get status of array = " + statusArray);
							HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
									.putHeader("X-Total-Count", String.valueOf(statusArray.size()))
									.end(Json.encodePrettily(statusArray.getList()));
							LOG.info(DFAPIMessage.logResponseMessage(1024, taskId));
						}
					});
		}
    }

    /**
     * Utilities to submit statement when session is in idle. It will submit sql, pyspark or scala to Livy server.
     *
     * @param c vertx web client for rest
     * @param j jd job object
     * @param mongo mongodb client
     * @param taskCollection mongo collection name to keep df tasks
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     * @param sessionId livy session id
     */
    private static void addStatementToSession(WebClient c, DFJobPOPJ j,
                                              String sparkRestHost, int sparkRestPort,
                                              MongoClient mongo, String taskCollection,
                                              String sessionId, String rawResBody) {
        String connectType = j.getConnectorType(), codeKind = "spark", code = "";
        // Here set stream back information. Later, the spark job status checker will upload the file to kafka
        Boolean streamBackFlag = false;
        String streamBackBasePath = "";
        if(j.getConnectorConfig().containsKey(ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG) &&
                        j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG)
                        .contentEquals("true")) {
            streamBackFlag = true;
            streamBackBasePath = ConstantApp.TRANSFORM_STREAM_BACK_PATH + "/" + j.getId() + "/";
            j.getConnectorConfig().put(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH, streamBackBasePath); //set full path
        }

        if(connectType.equalsIgnoreCase(ConstantApp.DF_CONNECT_TYPE.TRANSFORM_EXCHANGE_SPARK_SQL.name())) {
			code = HelpFunc.sqlToSparkScala(
					HelpFunc.sqlCleaner(j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_SQL)), streamBackFlag,
					streamBackBasePath);
		} else if (connectType.equalsIgnoreCase(ConstantApp.DF_CONNECT_TYPE.TRANSFORM_MODEL_SPARK_TRAIN.name())) {
			if ("FALSE".equalsIgnoreCase(j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_MT_GUIDE_ENABLE))) {
				code = StringUtils.substringBefore(j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_MT_CODE), "//");
				codeKind = j.getConnectorConfig().get(ConstantApp.PK_TRANSFORM_MT_CODE_KIND);
			} else {
				System.out.println("ML using guideline");
				System.out.println(
						"ML code generator source = " + j.toJson().getJsonObject("connectorConfig"));
				code = HelpFunc.mlGuideToScalaSpark(j.toJson().getJsonObject("connectorConfig"));
				j.setConnectorConfig(ConstantApp.PK_TRANSFORM_MT_CODE, code);
				System.out.println("ML code generated = " + code);
			}
		}

        c.post(sparkRestPort, sparkRestHost,
                ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId +
                        ConstantApp.LIVY_REST_URL_STATEMENTS)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
                        ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .sendJsonObject(new JsonObject().put("code", code).put("kind", codeKind),
                        sar -> {
                            if (sar.succeeded()) {
                                JsonObject response = sar.result().bodyAsJsonObject();
                                //System.out.println("Post returned = " + response);

                                // Get job submission status/result to keep in repo.
                                // Further status update comes from refresh status module in fibers
                                j.setJobConfig(ConstantApp.PK_LIVY_SESSION_ID, sessionId)
                                        .setJobConfig(ConstantApp.PK_LIVY_STATEMENT_ID,
                                                response.getInteger("id").toString()
                                        )
                                        .setJobConfig(
                                                ConstantApp.PK_LIVY_STATEMENT_CODE,
                                                response.getString("code"));

                                mongo.updateCollection(taskCollection, new JsonObject().put("_id", j.getId()),
                                        new JsonObject().put("$set", j.toJson()), v -> {
                                            if (!v.failed()) {
												LOG.info(DFAPIMessage.logResponseMessage(1005, j.getId()));
											} else {
												LOG.error(DFAPIMessage.logResponseMessage(1001,
														j.getId() + "error = " + v.cause()));
											}
                                        }
                                );
                            }
                        }
                );
    }

    /**
     * This is to get live job status for spark. Since spark now only has batch, we do not use it in web UI.
     * @param c route context
     * @param webClient vertx web client for rest
     * @param j jd job object
     * @param sparkRestHost spark/livy rest hostname
     * @param sparkRestPort spark/livy rest port number
     */
    @Deprecated
    public static void forwardGetAsJobStatusFromRepo(RoutingContext c, WebClient webClient, DFJobPOPJ j,
                                             String sparkRestHost, int sparkRestPort) {
        String sessionId = j.getJobConfig().get(ConstantApp.PK_LIVY_SESSION_ID),
				statementId = j.getJobConfig().get(ConstantApp.PK_LIVY_STATEMENT_ID), taskId = j.getId();
        if (sessionId == null || sessionId.trim().isEmpty() || statementId == null || statementId.trim().isEmpty()) {
            LOG.warn(DFAPIMessage.logResponseMessage(9000, taskId));
            HelpFunc.responseCorsHandleAddOn(c.response())
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000, taskId,
                            "Cannot Get State Without Session Id/Statement Id."));
        } else {
			webClient
					.get(sparkRestPort, sparkRestHost,
							ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + ConstantApp.LIVY_REST_URL_STATEMENTS)
					.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
					.send(ar -> {
						if (!ar.succeeded() || ar.result().statusCode() != ConstantApp.STATUS_CODE_OK) {
							HelpFunc.responseCorsHandleAddOn(c.response())
									.setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
									.end(DFAPIMessage.getResponseMessage(9029, taskId,
											"Cannot Found State for job " + statementId));
							LOG.info(DFAPIMessage.logResponseMessage(9029, taskId));
						} else {
							JsonArray subTaskArray = ar.result().bodyAsJsonObject().getJsonArray("statements"),
									statusArray = new JsonArray();
							for (int i = 0; i < subTaskArray.size(); ++i) {
								statusArray
										.add(new JsonObject()
												.put("subTaskId",
														sessionId + "_"
																+ subTaskArray.getJsonObject(i).getInteger("id"))
												.put("id",
														taskId + "_" + subTaskArray.getJsonObject(i).getInteger("id"))
												.put("jobId", sessionId)
												.put("dfTaskState",
														HelpFunc.getTaskStatusSpark(subTaskArray.getJsonObject(i)))
												.put("taskState",
														subTaskArray.getJsonObject(i).getString("state").toUpperCase())
												.put("statement", subTaskArray.getJsonObject(i).getString("code"))
												.put("output", "<table>\n"
														+ "  <tr><th>Firstname</th><th>Lastname</th><th>Age</th></tr>\n"
														+ "  <tr><td>Jill</td><td>Smith</td><td>50</td></tr>\n"
														+ "  <tr><td>Eve</td><td>Jackson</td><td>94</td></tr>\n"
														+ "  <tr><td>John</td><td>Doe</td><td>80</td></tr>\n"
														+ "</table>"));
							}
							System.out.println("get status of array = " + statusArray);
							HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
									.putHeader("X-Total-Count", String.valueOf(statusArray.size()))
									.end(Json.encodePrettily(statusArray.getList()));
							LOG.info(DFAPIMessage.logResponseMessage(1024, taskId));
						}
					});
		}
    }
}
