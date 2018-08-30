package com.datafibers.processor;

import com.datafibers.model.DFJobPOPJ;
import com.datafibers.util.ConstantApp;
import com.datafibers.util.DFAPIMessage;
import com.datafibers.util.HelpFunc;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import java.io.File;
import java.util.List;

/**
 * This is the utility class to communicate with Spark through Apache Livy Rest Service.
 */

public class ProcessorStreamBack {
    private static final Logger LOG = Logger.getLogger(ProcessorStreamBack.class);

    /**
     * Add a stream back task (AVRO file source connect) directly to df rest service.
     * @param wc_streamback
     * @param c
     * @param COLLECTION
     * @param schema_registry_rest_port
     * @param schema_registry_rest_host
     * @param df_rest_port
     * @param df_rest_host
     * @param createNewSchema
     * @param subject
     * @param schemaFields
     * @param streamBackMaster
     * @param streamBackWorker
     * @param l
     */
    public static void addStreamBackTask(WebClient wc_streamback, MongoClient c, String COLLECTION,
                                         int schema_registry_rest_port, String schema_registry_rest_host,
                                         int df_rest_port, String df_rest_host,
                                         Boolean createNewSchema,
                                         String subject, String schemaFields,
                                         DFJobPOPJ streamBackMaster, DFJobPOPJ streamBackWorker, Logger l) {
        if (!createNewSchema) {
			l.debug("use old schema ...");
			c.findOne(COLLECTION, new JsonObject().put("_id", streamBackWorker.getId()),
					new JsonObject().put("connectorConfig", 1), res -> {
						l.debug("res.succeeded() = " + res.succeeded());
						l.debug("res.result() = " + res.result());
						if (res.succeeded() && res.result() != null) {
							l.debug("Use old schema and update the stream back task");
							wc_streamback
									.put(df_rest_port, df_rest_host,
											ConstantApp.DF_CONNECTS_REST_URL + "/" + streamBackWorker.getId())
									.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
											ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
									.sendJsonObject(streamBackWorker.toPostJson(), war -> {
										l.debug("rest put result = " + war.result().bodyAsString());
										streamBackMaster.getConnectorConfig().put(
												ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
												(war.succeeded() ? ConstantApp.DF_STATUS.RUNNING
														: ConstantApp.DF_STATUS.FAILED).name());
										HelpFunc.updateRepoWithLogging(c, COLLECTION, streamBackMaster, l);
									});
						} else {
							l.debug("use old schema to create a new stream back task streamBackWorker.toJson() = "
									+ streamBackWorker.toJson());
							wc_streamback.post(df_rest_port, df_rest_host, ConstantApp.DF_CONNECTS_REST_URL)
									.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
											ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
									.sendJsonObject(streamBackWorker.toPostJson(), war -> {
										l.debug("rest post result = " + war.result().bodyAsString());
										streamBackMaster.getConnectorConfig().put(
												ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
												(war.succeeded() ? ConstantApp.DF_STATUS.RUNNING
														: ConstantApp.DF_STATUS.FAILED).name());
										HelpFunc.updateRepoWithLogging(c, COLLECTION, streamBackMaster, l);
									});
						}
					});
		} else {
			l.debug("create schema ...");
			wc_streamback
					.post(schema_registry_rest_port, schema_registry_rest_host,
							ConstantApp.SR_REST_URL_SUBJECTS + "/" + subject + ConstantApp.SR_REST_URL_VERSIONS)
					.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE, ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
					.sendJsonObject(new JsonObject().put(ConstantApp.SCHEMA_REGISTRY_KEY_SCHEMA, schemaFields),
							schemar -> {
								if (!schemar.succeeded()) {
									l.error("Schema creation failed for streaming back worker");
								} else {
									l.debug("Stream Back Schema is created with version "
											+ schemar.result().bodyAsString());
									l.debug("The schema fields are " + schemaFields);
									c.findOne(COLLECTION, new JsonObject().put("_id", streamBackWorker.getId()),
											new JsonObject().put("connectorConfig", 1), res -> {
												if (res.succeeded() && res.result() != null) {
													l.debug("found stream back task, update it");
													wc_streamback
															.put(df_rest_port, df_rest_host,
																	ConstantApp.DF_CONNECTS_REST_URL + "/"
																			+ streamBackWorker.getId())
															.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
																	ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
															.sendJsonObject(streamBackWorker.toPostJson(), war -> {
																l.debug("rest put result = "
																		+ war.result().bodyAsString());
																streamBackMaster.getConnectorConfig().put(
																		ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
																		(war.succeeded() ? ConstantApp.DF_STATUS.RUNNING
																				: ConstantApp.DF_STATUS.FAILED).name());
																HelpFunc.updateRepoWithLogging(c, COLLECTION,
																		streamBackMaster, l);
															});
												} else {
													l.debug("not found stram back task, create it");
													wc_streamback
															.post(df_rest_port, df_rest_host,
																	ConstantApp.DF_CONNECTS_REST_URL)
															.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
																	ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
															.sendJsonObject(streamBackWorker.toPostJson(), war -> {
																l.debug("rest post result = "
																		+ war.result().bodyAsString());
																streamBackMaster.getConnectorConfig().put(
																		ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
																		(war.succeeded() ? ConstantApp.DF_STATUS.RUNNING
																				: ConstantApp.DF_STATUS.FAILED).name());
																l.debug("response from create stream back tsdk = "
																		+ war.result().bodyAsString());
																HelpFunc.updateRepoWithLogging(c, COLLECTION,
																		streamBackMaster, l);
															});
												}
											});
								}
							});
		}
    }

    /**
     * EnableStreamBack is a generic function to submit stream back task if requested. It also check the status of the
     * stream back worker and update proper status in the stream back master (the transform which triggered the stream
     * back task. Another important function is whether to overwrite the status in updateJob. The stream master's status
     * has to be overwritten when the stream back working is in progressing in order to keep master running.
     *
     * @param wc_streamback vertx web client for rest
     * @param updateJob dfpopj job about to update to repo
     * @param c mongodb client
     * @param COLLECTION mongo collection to keep the task
     * @param schema_registry_rest_host Schema registry rest hostname
     * @param schema_registry_rest_port Schema registry rest port number
     * @param df_rest_port datafibers rest hostname
     * @param df_rest_host datafibers rest port number
     * @param schemaFields if we create a new topic, this is list of fields and types
     */
    public static void enableStreamBack(WebClient wc_streamback, DFJobPOPJ updateJob,
                                        MongoClient c, String COLLECTION,
                                        int schema_registry_rest_port, String schema_registry_rest_host,
                                        int df_rest_port, String df_rest_host, String schemaFields) {
        LOG.debug("Enter stream back call");
        String streamBackTaskId = updateJob.getId() + "sw",
				streamBackFilePath = updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH),
				streamBackTopic = updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TOPIC);
        if ("".equalsIgnoreCase(updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE))
				|| updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE)
						.equalsIgnoreCase(ConstantApp.DF_STATUS.UNASSIGNED.name())) {
			LOG.debug("Will create a new stream back job");
			updateJob.setStatus(ConstantApp.DF_STATUS.STREAMING.name())
					.setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
							ConstantApp.DF_STATUS.UNASSIGNED.name())
					.setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_ID, streamBackTaskId);
			DFJobPOPJ streamBackTask = new DFJobPOPJ().setId(streamBackTaskId).setTaskSeq("1").setName("stream_worker")
					.setDescription("stream_worker")
					.setConnectorType(ConstantApp.DF_CONNECT_TYPE.CONNECT_SOURCE_KAFKA_AvroFile.name())
					.setConnectorCategory("source").setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_TASK, "1")
					.setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_SRURI,
							"http://" + schema_registry_rest_host + ":" + schema_registry_rest_port)
					.setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_OW, "true")
					.setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_LOC, streamBackFilePath)
					.setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_GLOB, "*.json")
					.setConnectorConfig(ConstantApp.PK_STREAM_BACK_CONNECT_TOPIC, streamBackTopic);
			LOG.debug("Stream Back Connect POPJ = " + streamBackTask.toJson());
			addStreamBackTask(wc_streamback, c, COLLECTION, schema_registry_rest_port, schema_registry_rest_host,
					df_rest_port, df_rest_host,
					Boolean.parseBoolean(
							updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TOPIC_CREATION)),
					streamBackTopic, schemaFields, updateJob, streamBackTask, LOG);
		} else {
			String streamBackTaskState = updateJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE);
			LOG.debug("Found stream back state = " + streamBackTaskState);
			if (!streamBackTaskState.equalsIgnoreCase(ConstantApp.DF_STATUS.FINISHED.name())) {
				if (streamBackTaskState.equalsIgnoreCase(ConstantApp.DF_STATUS.FAILED.name())) {
					HelpFunc.updateRepoWithLogging(c, COLLECTION,
							updateJob.setStatus(ConstantApp.DF_STATUS.FAILED.name()), LOG);
				} else {
					c.findOne(COLLECTION, new JsonObject().put("_id", streamBackTaskId),
							new JsonObject().put("status", 1), res -> {
								if (!res.succeeded()) {
									LOG.error("Stream Back Task with Id = " + streamBackTaskId + " Not Found.");
								} else {
									String workerStatus = res.result().getString("status");
									LOG.debug("Stream back worker status = " + workerStatus);
									if (workerStatus.equalsIgnoreCase(ConstantApp.DF_STATUS.FAILED.name())
											|| workerStatus.equalsIgnoreCase(ConstantApp.DF_STATUS.LOST.name())) {
										updateJob.setStatus(ConstantApp.DF_STATUS.FAILED.name()).setConnectorConfig(
												ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
												ConstantApp.DF_STATUS.FAILED.name());
									} else {
										LOG.debug("Checking streaming process result ...");
										File dir = new File(streamBackFilePath);
										int jsonFileNumber = ((List<File>) FileUtils.listFiles(dir,
												new String[] { "json" }, false)).size(),
												processedFileNumber = ((List<File>) FileUtils.listFiles(dir,
														new String[] { "processed" }, false)).size(),
												processingFileNumber = ((List<File>) FileUtils.listFiles(dir,
														new String[] { "processing" }, false)).size();
										LOG.debug("jsonFileNumber = " + jsonFileNumber + " processedFileNumber = "
												+ processedFileNumber);
										if (jsonFileNumber != 0 || processedFileNumber < 0 || processingFileNumber != 0) {
											updateJob.setStatus(ConstantApp.DF_STATUS.STREAMING.name())
													.setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
															ConstantApp.DF_STATUS.RUNNING.name());
										} else {
											updateJob.setStatus(ConstantApp.DF_STATUS.FINISHED.name())
													.setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
															ConstantApp.DF_STATUS.FINISHED.name());
											wc_streamback
													.delete(df_rest_port, df_rest_host,
															ConstantApp.DF_CONNECTS_REST_URL + "/" + streamBackTaskId)
													.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
															ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
													.send(ar -> {
														if (ar.succeeded()) {
															LOG.info(DFAPIMessage.logResponseMessage(1031,
																	updateJob.getId()));
														} else {
															LOG.error(DFAPIMessage.logResponseMessage(9043,
																	updateJob.getId() + " has error "
																			+ ar.result().bodyAsString()));
														}
													});
										}
									}
									HelpFunc.updateRepoWithLogging(c, COLLECTION, updateJob, LOG);
								}
							});
				}
			}
		}
    }
}
