package com.datafibers.processor;

import com.datafibers.util.DFAPIMessage;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.log4j.Logger;
import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.HelpFunc;

public class ProcessorConnectKafka {
    private static final Logger LOG = Logger.getLogger(ProcessorConnectKafka.class);
        /**
     * This method is used to get the kafka job stauts. It first decodes the REST GET request to DFJobPOPJ object.
     * Then, it updates its job status and repack for Kafka REST GET.
     * After that, it forward the new GET to Kafka Connect. Once REST API forward is successful, response.
     * Since we regular refresh status from kafka connect, so repo always has latest, the only reason to out it here is
     * to get live status when opening the task.
     *
     * @param c This is the contect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param kafkaConnectRestHost rest server host name
     * @param kafkaConnectRestPort rest server port number
     * @param taskId This is the id used to look up status
     */
    public static void forwardGetAsGetOne(RoutingContext c, WebClient webClient,
                                          String kafkaConnectRestHost, int kafkaConnectRestPort, String taskId) {
        // Create REST Client for Kafka Connect REST Forward
        webClient.get(kafkaConnectRestPort, kafkaConnectRestHost,
                ConstantApp.KAFKA_CONNECT_REST_URL + "/" + taskId + "/status")
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .send(ar -> {
                    if (!ar.succeeded() || ar.result().statusCode() != ConstantApp.STATUS_CODE_OK) {
						HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
								.end(DFAPIMessage.getResponseMessage(9036, taskId, "No Status Found for " + taskId));
						LOG.info(DFAPIMessage.logResponseMessage(9036, taskId));
					} else {
						JsonObject jo = ar.result().bodyAsJsonObject();
						JsonArray subTaskArray = jo.getJsonArray("tasks");
						if (subTaskArray.isEmpty()) {
							subTaskArray.add(new JsonObject().put("subTaskId", "e").put("id", taskId + "_e")
									.put("jobId", taskId).put("dfTaskState", HelpFunc.getTaskStatusKafka(jo))
									.put("taskState", jo.getJsonObject("connector").getString("state"))
									.put("taskTrace", jo.getJsonObject("connector").getString("trace")));
						} else {
							for (int i = 0; i < subTaskArray.size(); ++i) {
								subTaskArray.getJsonObject(i)
										.put("subTaskId", subTaskArray.getJsonObject(i).getInteger("id"))
										.put("id", taskId + "_" + subTaskArray.getJsonObject(i).getInteger("id"))
										.put("jobId", taskId).put("dfTaskState", HelpFunc.getTaskStatusKafka(jo))
										.put("taskState", jo.getJsonObject("connector").getString("state"));
							}
						}
						HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
								.putHeader("X-Total-Count", String.valueOf(subTaskArray.size()))
								.end(Json.encodePrettily(subTaskArray.getList()));
						LOG.info(DFAPIMessage.logResponseMessage(1023, taskId));
					}
                });
    }

    /**
     * This method is used to get the kafka job stauts. It first decodes the REST GET request to DFJobPOPJ object.
     * Then, it updates its job status and repack for Kafka REST GET.
     * After that, it forward the new GET to Kafka Connect. Once REST API forward is successful, response.
     *
     * @param c This is the contect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param kafkaConnectRestHost rest server host name
     * @param kafkaConnectRestPort rest server port number
     */
    public static void forwardGetAsGetConfig(RoutingContext c, WebClient webClient,
                                             MongoClient mongoClient, String mongoCOLLECTION,
                                             String kafkaConnectRestHost, int kafkaConnectRestPort) {
        LOG.debug("Called forwardGetAsGetConfig");
        // Create REST Client for Kafka Connect REST Forward
        webClient.get(kafkaConnectRestPort, kafkaConnectRestHost, ConstantApp.KAFKA_CONNECT_PLUGIN_REST_URL)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .send(ar -> {
                    if (!ar.succeeded()) {
						LOG.error(DFAPIMessage.logResponseMessage(9036, ""));
					} else {
						JsonArray configArray = ar.result().bodyAsJsonArray(), configClassArray = new JsonArray();
						for (int i = 0; i < configArray.size(); ++i) {
							configClassArray.add(configArray.getJsonObject(i).getString("class"));
						}
						JsonObject query = new JsonObject().put("$and", new JsonArray()
								.add(new JsonObject().put("class", new JsonObject().put("$in", configClassArray)))
								.add(new JsonObject().put("meta_type", "installed_connect")));
						mongoClient.findWithOptions(mongoCOLLECTION, query, HelpFunc.getMongoSortFindOption(c), res -> {
							if ("[]".equalsIgnoreCase(res.result().toString())) {
								LOG.warn("RUN_BELOW_CMD_TO_SEE_FULL_METADATA");
								LOG.warn("mongoimport -c df_installed -d DEFAULT_DB --file df_installed.json");
							}
							if (res.succeeded()) {
								HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
										.end(Json.encodePrettily(
												(res.result().isEmpty() ? ar : res).result()));
							}
						});
					}
                });
    }

    /**
     * This method first decode the REST POST request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST POST. After that, it forward the new POST to Kafka Connect.
     * Once REST API forward is successful, update data to the local repository.
     *
     * @param c This is the contect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param kafkaConnectRestHost rest server host name
     * @param kafkaConnectRestPort rest server port number
     * @param dfJobResponsed This is the response object return to rest client or ui or mongo insert
     */
    public static void forwardPOSTAsAddOne(RoutingContext c, WebClient webClient,
                                           MongoClient mongoClient, String mongoCOLLECTION,
                                           String kafkaConnectRestHost, int kafkaConnectRestPort,
                                           DFJobPOPJ dfJobResponsed) {
        // Create REST Client for Kafka Connect REST Forward
        webClient.post(kafkaConnectRestPort, kafkaConnectRestHost, ConstantApp.KAFKA_CONNECT_REST_URL)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .sendJsonObject(dfJobResponsed.toKafkaConnectJson(),
                        ar -> {
                            if (!ar.succeeded()) {
								HelpFunc.responseCorsHandleAddOn(c.response())
										.setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
										.end(DFAPIMessage.getResponseMessage(9037));
								LOG.info(DFAPIMessage.logResponseMessage(9037, dfJobResponsed.getId()));
							} else {
								mongoClient.insert(mongoCOLLECTION, dfJobResponsed.toJson(),
										r -> HelpFunc.responseCorsHandleAddOn(c.response())
												.setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
												.end(Json.encodePrettily(dfJobResponsed)));
								LOG.info(DFAPIMessage.logResponseMessage(1000, dfJobResponsed.getId()));
							}
                        }
                );
    }

    /**
     * This method first decode the REST PUT request to DFJobPOPJ object.
     * Then, it updates its job status immediately in the repository and response to ui
     * After that, it repacks the request for Kafka REST PUT and forward the new POST to Kafka Connect.
     *
     * @param c This is the connect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param kafkaConnectRestHost rest server host name
     * @param kafkaConnectRestPort rest server port number
     * @param dfJobResponsed This is the response object return to rest client or ui or mongo insert
     */
    public static void forwardPUTAsUpdateOne (RoutingContext c, WebClient webClient,
                                              MongoClient mongoClient, String mongoCOLLECTION,
                                              String kafkaConnectRestHost, int kafkaConnectRestPort,
                                              DFJobPOPJ dfJobResponsed) {
        final String id = c.request().getParam("id"), restURL = ConstantApp.KAFKA_CONNECT_PLUGIN_CONFIG
				.replace("CONNECTOR_NAME_PLACEHOLDER", dfJobResponsed.getConnectUid());
        webClient.put(kafkaConnectRestPort, kafkaConnectRestHost, restURL)
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .sendJsonObject(dfJobResponsed.toKafkaConnectJsonConfig(),
                        ar -> {
                            if (ar.succeeded()) {
								LOG.info(DFAPIMessage.logResponseMessage(1000, dfJobResponsed.getId()));
							} else {
                                // If response is failed, repose df ui and still keep the task
                                HelpFunc.responseCorsHandleAddOn(c.response())
                                        .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                                        .end(DFAPIMessage.getResponseMessage(9038));
                                LOG.info(DFAPIMessage.logResponseMessage(9038, dfJobResponsed.getId()));
                            }
                        }
                );
        // Here update the repo right way to ack ui. Even something is wrong, status sync. can still catch the update
        mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id),
                // The update syntax: {$set, the json object containing the fields to update}
                new JsonObject().put("$set", dfJobResponsed.toJson()), v -> {
                    if (!v.failed()) {
						HelpFunc.responseCorsHandleAddOn(c.response()).end(DFAPIMessage.getResponseMessage(1000));
						LOG.info(DFAPIMessage.logResponseMessage(1000, id));
					} else {
						c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
								.end(DFAPIMessage.getResponseMessage(9003));
						LOG.error(DFAPIMessage.logResponseMessage(9003, id));
					}
                });
    }

    /**
     * This method first decode the REST PUT request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST PUT. After that, it forward the new PUT to Kafka Connect to pause or resume the job.
     * Once REST API forward is successful, update data to the local repository.
     *
     * @param c This is the contect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param kafkaConnectRestHost rest server host name
     * @param kafkaConnectRestPort rest server port number
     * @param dfJobResponsed This is the response object return to rest client or ui or mongo insert
     * @param action pause or resume connect
     */
    public static void forwardPUTAsPauseOrResumeOne (RoutingContext c, WebClient webClient,
                                                     MongoClient mongoClient, String mongoCOLLECTION,
                                                     String kafkaConnectRestHost, int kafkaConnectRestPort,
                                                     DFJobPOPJ dfJobResponsed, String action) {
        final String id = c.request().getParam("id"), connectURL = ConstantApp.KAFKA_CONNECT_REST_URL + "/"
				+ dfJobResponsed.getConnectUid() + "/" + action.toLowerCase();
        String status = dfJobResponsed.getStatus(), preStatus = status;
        if (!ConstantApp.KAFKA_CONNECT_ACTION_PAUSE.equalsIgnoreCase(status)
				&& !ConstantApp.KAFKA_CONNECT_ACTION_RESUME.equalsIgnoreCase(status)) {
			HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
					.end(DFAPIMessage.getResponseMessage(9032, "", "Invalid Status to Pause or Resume"));
		} else {
			status = (ConstantApp.KAFKA_CONNECT_ACTION_PAUSE.equalsIgnoreCase(action) ? ConstantApp.DF_STATUS.PAUSED
					: ConstantApp.DF_STATUS.RUNNING).name();
			dfJobResponsed.setStatus(status);
			LOG.debug("WILL_PUT_TO_KAFKA_CONNECT - " + dfJobResponsed.toKafkaConnectJsonConfig());
			webClient.put(kafkaConnectRestPort, kafkaConnectRestHost, connectURL)
					.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
					.sendJsonObject(dfJobResponsed.toKafkaConnectJsonConfig(), ar -> {
						if (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK_ACCEPTED) {
							LOG.info(DFAPIMessage.logResponseMessage(1000, dfJobResponsed.getId()));
						} else {
							mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id),
									new JsonObject().put("$set", dfJobResponsed.setStatus(preStatus).toJson()), v -> {
										if (!v.failed()) {
											HelpFunc.responseCorsHandleAddOn(c.response())
													.end(DFAPIMessage.getResponseMessage(9033));
											LOG.info(DFAPIMessage.logResponseMessage(9033, id));
										} else {
											c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
													.end(DFAPIMessage.getResponseMessage(9034));
											LOG.error(DFAPIMessage.logResponseMessage(9034, id));
										}
									});
						}
					});
			mongoClient.updateCollection(mongoCOLLECTION, new JsonObject().put("_id", id),
					new JsonObject().put("$set", dfJobResponsed.toJson()), v -> {
						if (!v.failed()) {
							HelpFunc.responseCorsHandleAddOn(c.response()).end(DFAPIMessage.getResponseMessage(1001));
							LOG.info(DFAPIMessage.logResponseMessage(1001, id));
						} else {
							c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
									.end(DFAPIMessage.getResponseMessage(9003));
							LOG.error(DFAPIMessage.logResponseMessage(9003, id));
						}
					});
		}
    }

    /**
     * This method first decode the REST DELETE request to DFJobPOPJ object. Then, it updates its job status and repack
     * for Kafka REST DELETE. After that, it forward the new DELETE to Kafka Connect.
     * Once REST API forward is successful, update data to the local repository.
     *
     * @param c This is the contect from REST API
     * @param webClient This is vertx non-blocking rest client used for forwarding
     * @param mongoClient This is the client used to insert final data to repository - mongodb
     * @param mongoCOLLECTION This is mongodb collection name
     * @param kafkaConnectRestHost rest server host name
     * @param kafkaConnectRestPort rest server port number
     * @param dfJobResponsed This is the response object return to rest client or ui or mongo insert
     */
    public static void forwardDELETEAsDeleteOne (RoutingContext c, WebClient webClient,
                                                 MongoClient mongoClient, String mongoCOLLECTION,
                                                 String kafkaConnectRestHost, int kafkaConnectRestPort,
                                                 DFJobPOPJ dfJobResponsed) {
        String id = c.request().getParam("id");
        // Create REST Client for Kafka Connect REST Forward
        webClient.delete(kafkaConnectRestPort, kafkaConnectRestHost,
                ConstantApp.KAFKA_CONNECT_REST_URL + "/" + dfJobResponsed.getConnectUid())
                .putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                .sendJsonObject(DFAPIMessage.getResponseJsonObj(1002),
                        ar -> {
                            if (!ar.succeeded()) {
								HelpFunc.responseCorsHandleAddOn(c.response())
										.setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
										.end(DFAPIMessage.getResponseMessage(9029));
								LOG.info(DFAPIMessage.logResponseMessage(9029, id));
							} else {
								int response = (ar.result().statusCode() == ConstantApp.STATUS_CODE_OK_NO_CONTENT)
										? 1002
										: 9012;
								mongoClient.removeDocument(mongoCOLLECTION, new JsonObject().put("_id", id),
										mar -> HelpFunc.responseCorsHandleAddOn(c.response())
												.setStatusCode(ConstantApp.STATUS_CODE_OK)
												.end(DFAPIMessage.getResponseMessage(response, id)));
								LOG.info(DFAPIMessage.logResponseMessage(response, id));
							}
                        }
                );
    }
}
