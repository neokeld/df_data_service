package com.datafibers.service;

import com.datafibers.model.DFLogPOPJ;
import com.datafibers.model.DFModelPOPJ;
import com.datafibers.processor.*;
import com.datafibers.util.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;
import io.vertx.ext.web.handler.TimeoutHandler;
import io.vertx.kafka.client.common.PartitionInfo;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.apache.log4j.Logger;
import com.datafibers.model.DFJobPOPJ;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.log4mongo.MongoDbAppender;

/**
 * DF Producer is used to route producer service to kafka connect rest or lunch locally.
 * The overall status is maintained in the its local database - mongodb
 */

public class DFDataProcessor extends AbstractVerticle {
    /** Generic attributes. */
    public String COLLECTION;
    public String COLLECTION_MODEL;
    public String COLLECTION_INSTALLED;
    public String COLLECTION_META;
    public String COLLECTION_LOG;
    private MongoClient mongo;
    private MongoAdminClient mongoDFInstalled;
    private WebClient wc_schema;
    private WebClient wc_connect;
    private WebClient wc_flink;
    private WebClient wc_spark;
    private WebClient wc_refresh;
    private WebClient wc_streamback;
    private String df_jar_path;
    private String df_jar_name;
    private String flink_jar_id;

    private Integer df_rest_port;

    /** Connects attributes. */
    private Boolean kafkaConnectEnabled;
    private String kafka_connect_rest_host;
    private Integer kafka_connect_rest_port;
    private Boolean kafka_connect_import_start;

    /** Transforms attributes flink. */
    public Boolean transform_engine_flink_enabled;
    private String flink_server_host;
    private Integer flink_rest_server_port;
    private String flink_rest_server_host_port;

    /** Transforms attributes spark. */
    public Boolean transform_engine_spark_enabled;
    private String spark_livy_server_host;
    private Integer spark_livy_server_port;

    /** Kafka attributes. */
    private String kafka_server_host;
    private Integer kafka_server_port;
    public String kafka_server_host_and_port;

    /** Schema Registry attributes. */
    private String schema_registry_host_and_port;
    private Integer schema_registry_rest_port;
    private String schema_registry_rest_hostname;

    /** Web HDFS attributes. */
    private Integer webhdfs_rest_port;
    private String webhdfs_rest_hostname;

    private static final Logger LOG = Logger.getLogger(DFDataProcessor.class);

    @Override
    public void start(Future<Void> v) {
        this.df_jar_path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        this.df_jar_name = new File(df_jar_path).getName();

        /* Get all application configurations */
        // Get generic variables
        this.COLLECTION = config().getString("db.collection.name", "df_processor");
        this.COLLECTION_MODEL = config().getString("db.collection_model.name", "df_model");
        this.COLLECTION_INSTALLED = config().getString("db.collection_installed.name", "df_installed");
        this.COLLECTION_META = config().getString("db.metadata.collection.name", "df_meta");
        this.COLLECTION_LOG = config().getString("db.log.collection.name", "df_log");
        String repoDb = config().getString("db.name", "DEFAULT_DB");
        String repoConnStr = config().getString("repo.connection.string", "mongodb://localhost:27017");
        String repoHostname = repoConnStr.replace("//", "").split(":")[1];
        String repoPort = repoConnStr.replace("//", "").split(":")[2];

        this.df_rest_port = config().getInteger("rest.port.df.processor", 8080);

        // Get Connects config
        this.kafkaConnectEnabled = config().getBoolean("kafka.connect.enable", Boolean.TRUE);
        this.kafka_connect_rest_host = config().getString("kafka.connect.rest.host", "localhost");
        this.kafka_connect_rest_port = config().getInteger("kafka.connect.rest.port", 8083);
        this.kafka_connect_import_start = config().getBoolean("kafka.connect.import.start", Boolean.TRUE);

        // Check Flink Transforms config
        this.transform_engine_flink_enabled = config().getBoolean("transform.engine.flink.enable", Boolean.TRUE);
        this.flink_server_host = config().getString("flink.server.host", "localhost");
        this.flink_rest_server_port = config().getInteger("flink.rest.server.port", 8001); // Same to Flink Web Dashboard
        this.flink_rest_server_host_port = (this.flink_server_host.contains("http")?
                this.flink_server_host : "http://" + this.flink_server_host) + ":" + this.flink_rest_server_port;

        // Check Spark Transforms config
        this.transform_engine_spark_enabled = config().getBoolean("transform.engine.spark.enable", Boolean.TRUE);
        this.spark_livy_server_host = config().getString("spark.livy.server.host", "localhost");
        this.spark_livy_server_port = config().getInteger("spark.livy.server.port", 8998);

        // Kafka config
        this.kafka_server_host = this.kafka_connect_rest_host;
        this.kafka_server_port = config().getInteger("kafka.server.port", 9092);
        this.kafka_server_host_and_port = this.kafka_server_host + ":" + this.kafka_server_port;

        // Schema Registry
        this.schema_registry_rest_hostname = kafka_connect_rest_host;
        this.schema_registry_rest_port = config().getInteger("kafka.schema.registry.rest.port", 8081);
        this.schema_registry_host_and_port = this.schema_registry_rest_hostname + ":" + this.schema_registry_rest_port;

        // WebHDFS
        this.webhdfs_rest_port = config().getInteger("webhdfs.server.port", 50070);
        this.webhdfs_rest_hostname = config().getString("webhdfs.server.host", "localhost");

        // Application init in separate thread and report complete once done
        vertx.executeBlocking(future -> {
            mongo = MongoClient.createShared(vertx, new JsonObject().put("connection_string", repoConnStr).put("db_name", repoDb));

            // df_meta mongo client to find default connector.class
            mongoDFInstalled = new MongoAdminClient(repoHostname, repoPort, repoDb, COLLECTION_INSTALLED);

            // Cleanup Log in mongodb
            if (config().getBoolean("db.log.cleanup.on.start", Boolean.TRUE)) {
				new MongoAdminClient(repoHostname, repoPort, repoDb).truncateCollection(COLLECTION_LOG).close();
			}

            // Set dynamic logging to MongoDB
            MongoDbAppender mongoAppender = new MongoDbAppender();
            mongoAppender.setDatabaseName(repoDb);
            mongoAppender.setCollectionName(COLLECTION_LOG);
            mongoAppender.setHostname(repoHostname);
            mongoAppender.setPort(repoPort);
            mongoAppender.activateOptions();
            Logger.getRootLogger().addAppender(mongoAppender);

            LOG.info(DFAPIMessage.logResponseMessage(1029, "Mongo Client & Log Shipping Setup Complete"));

            // Non-blocking Rest API Client to talk to Kafka Connect when needed
            if (this.kafkaConnectEnabled) {
                this.wc_connect = WebClient.create(vertx);
                this.wc_schema = WebClient.create(vertx);
            }

            // Import from remote server. It is blocking at this point.
            if (this.kafkaConnectEnabled && this.kafka_connect_import_start) {
                importAllFromKafkaConnect();
                // importAllFromFlinkTransform();
                startMetadataSink();
            }

            // Non-blocking Rest API Client to talk to Flink Rest when needed
            if (this.transform_engine_spark_enabled) {
				this.wc_spark = WebClient.create(vertx);
			}

            // Non-blocking Rest API Client to talk to Flink Rest when needed
            if (this.transform_engine_flink_enabled) {
                this.wc_flink = WebClient.create(vertx);
                // Delete all df jars already uploaded
                wc_flink.get(flink_rest_server_port, flink_server_host, ConstantApp.FLINK_REST_URL_JARS)
                        .send(ar -> {
                            if (!ar.succeeded()) {
								LOG.error(DFAPIMessage.logResponseMessage(9035, flink_jar_id));
							} else {
								JsonArray jarArray = ar.result().bodyAsJsonObject().getJsonArray("files");
								for (int i = 0; i < jarArray.size(); ++i) {
									if (jarArray.getJsonObject(i).getString("name").equalsIgnoreCase(df_jar_name)) {
										wc_flink.delete(flink_rest_server_port, flink_server_host,
												ConstantApp.FLINK_REST_URL_JARS + "/"
														+ jarArray.getJsonObject(i).getString("id"))
												.send(dar -> {
												});
									}
								}
								vertx.executeBlocking(jarfuture -> {
									this.flink_jar_id = HelpFunc.uploadJar(
											flink_rest_server_host_port + ConstantApp.FLINK_REST_URL_JARS_UPLOAD,
											this.df_jar_path);
									if (flink_jar_id.isEmpty()) {
										LOG.error(DFAPIMessage.logResponseMessage(9035, flink_jar_id));
									} else {
										LOG.info(DFAPIMessage.logResponseMessage(1028, flink_jar_id));
										LOG.info("********* DataFibers Services is started :) *********");
									}
								}, res -> {
								});
							}
                        });
            }

            if (!this.transform_engine_flink_enabled) {
				LOG.info("********* DataFibers Services is started :) *********");
			}
        }, res -> {});

        // Regular update Kafka connects/Flink transform status through unblocking api
        this.wc_refresh = WebClient.create(vertx);
        this.wc_streamback = WebClient.create(vertx);

        vertx.setPeriodic(ConstantApp.REGULAR_REFRESH_STATUS_TO_REPO, id -> {
            if(this.kafkaConnectEnabled) {
				updateKafkaConnectorStatus();
			}
            if(this.transform_engine_flink_enabled) {
				updateFlinkJobStatus();
			}
            if(this.transform_engine_spark_enabled) {
				updateSparkJobStatus();
			}
        });

        // Start Core application
        startWebApp(http -> completeStartup(http, v));
    }

    private void startWebApp(Handler<AsyncResult<HttpServer>> next) {
        // Create a router object for rest.
        Router router = Router.router(vertx);

        // Job including both Connects and Transforms Rest API definition
        router.options(ConstantApp.DF_PROCESSOR_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_PROCESSOR_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_PROCESSOR_REST_URL).handler(this::getAllProcessor);
        router.get(ConstantApp.DF_PROCESSOR_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_PROCESSOR_REST_URL_WILD).handler(BodyHandler.create());

        // Connects Rest API definition
        router.options(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_CONNECTS_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_CONNECTS_REST_URL).handler(this::getAllConnects);
        router.get(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_CONNECTS_REST_URL_WILD).handler(BodyHandler.create());

        router.post(ConstantApp.DF_CONNECTS_REST_URL).handler(this::addOneConnects); // Kafka Connect Forward
        router.put(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::putOneConnect); // Kafka Connect Forward
        router.delete(ConstantApp.DF_CONNECTS_REST_URL_WITH_ID).handler(this::deleteOneConnects); // Kafka Connect Forward

        // Transforms Rest API definition
        router.options(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::corsHandle);
        router.options(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::getAllTransforms);
        router.get(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::getOne);
        router.route(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL_WILD).handler(BodyHandler.create());
        router.post(ConstantApp.DF_TRANSFORMS_UPLOAD_FILE_REST_URL).handler(this::uploadFiles);
        router.route(ConstantApp.DF_TRANSFORMS_REST_URL_WILD).handler(BodyHandler.create());

        router.post(ConstantApp.DF_TRANSFORMS_REST_URL).handler(this::addOneTransforms); // Flink Forward
        router.put(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::updateOneTransforms); // Flink Forward
        router.delete(ConstantApp.DF_TRANSFORMS_REST_URL_WITH_ID).handler(this::deleteOneTransforms); // Flink Forward

        // Model Rest API definition
        router.options(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_MODEL_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_MODEL_REST_URL).handler(this::getAllModels);
        router.get(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::getOneModel);
        router.route(ConstantApp.DF_MODEL_REST_URL_WILD).handler(BodyHandler.create());

        router.post(ConstantApp.DF_MODEL_REST_URL).handler(this::addOneModel);
        router.put(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::updateOneModel);
        router.delete(ConstantApp.DF_MODEL_REST_URL_WITH_ID).handler(this::deleteOneModel);

        // Schema Registry
        router.options(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_SCHEMA_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_SCHEMA_REST_URL).handler(this::getAllSchemas); // Schema Registry Forward
        router.get(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::getOneSchema); // Schema Registry Forward
        router.route(ConstantApp.DF_SCHEMA_REST_URL_WILD).handler(BodyHandler.create()); // Schema Registry Forward
                router.post(ConstantApp.DF_SCHEMA_REST_URL).handler(this::addOneSchema); // Schema Registry Forward
        router.put(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::updateOneSchema); // Schema Registry Forward
        router.delete(ConstantApp.DF_SCHEMA_REST_URL_WITH_ID).handler(this::deleteOneSchema); // Schema Registry Forward

        // Logging Rest API definition
        router.options(ConstantApp.DF_LOGGING_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_LOGGING_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_LOGGING_REST_URL).handler(this::getAllLogs);
        router.get(ConstantApp.DF_LOGGING_REST_URL_WITH_ID).handler(this::getOneLogs);
        router.route(ConstantApp.DF_LOGGING_REST_URL_WILD).handler(BodyHandler.create());

        // Status Rest API definition
        router.options(ConstantApp.DF_TASK_STATUS_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_TASK_STATUS_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_TASK_STATUS_REST_URL_WITH_ID).handler(this::getOneStatus);
        router.get(ConstantApp.DF_TASK_STATUS_REST_URL_WILD).handler(this::getOneStatus);

        // Subject to Task Rest API definition
        router.options(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL_WITH_ID).handler(this::getAllTasksOneTopic);
        router.get(ConstantApp.DF_SUBJECT_TO_TASK_REST_URL_WILD).handler(this::getAllTasksOneTopic);

        // Avro Consumer Rest API definition and set timeout to 10s
        router.route(ConstantApp.DF_AVRO_CONSUMER_REST_URL).handler(TimeoutHandler.create(10000));
        router.options(ConstantApp.DF_AVRO_CONSUMER_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_AVRO_CONSUMER_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_AVRO_CONSUMER_REST_URL_WITH_ID).handler(this::pollAllFromTopic);
        router.get(ConstantApp.DF_AVRO_CONSUMER_REST_URL_WILD).handler(this::pollAllFromTopic);

        // Subject to partition info. Rest API definition
        router.options(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL).handler(this::corsHandle);
        router.options(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL_WITH_ID).handler(this::getAllTopicPartitions);
        router.get(ConstantApp.DF_SUBJECT_TO_PAR_REST_URL_WILD).handler(this::getAllTopicPartitions);

        // Get all installed connect or transform
        router.options(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL_WITH_ID).handler(this::corsHandle);
        router.options(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL).handler(this::getAllProcessorConfigs);
        router.get(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL_WITH_ID).handler(this::getOneProcessorConfig);
        router.route(ConstantApp.DF_PROCESSOR_CONFIG_REST_URL_WILD).handler(BodyHandler.create());

        // Process History
        router.options(ConstantApp.DF_PROCESS_HIST_REST_URL).handler(this::corsHandle);
        router.get(ConstantApp.DF_PROCESS_HIST_REST_URL).handler(this::getAllProcessHistory);
                // Create the HTTP server and pass the "accept" method to the request handler.
        vertx.createHttpServer().requestHandler(router::accept)
                                .listen(config().getInteger("rest.port.df.processor", 8080), next::handle);
    }

    private void completeStartup(AsyncResult<HttpServer> s, Future<Void> v) {
        if (s.succeeded()) {
			v.complete();
		} else {
			v.fail(s.cause());
		}
    }

    @Override
    public void stop() throws Exception {
        this.mongo.close();
        this.mongoDFInstalled.close();
        this.wc_connect.close();
        this.wc_schema.close();
        this.wc_flink.close();
    }

    /**
     * This is mainly to bypass security control for local API testing.
     * @param c
     */
    public void corsHandle(RoutingContext c) {
        c.response()
                .putHeader("Access-Control-Allow-Origin", "*")
                .putHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
                .putHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, X-Total-Count")
                .putHeader("Access-Control-Expose-Headers", "X-Total-Count")
                .putHeader("Access-Control-Max-Age", "60").end();
    }

    /**
     * Handle upload UDF Jar form for Flink UDF Transformation
     * @param c
     *
     * @api {post} /uploaded_files 7.Upload file
     * @apiVersion 0.1.1
     * @apiName uploadFiles
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is triggered through "ADD File" in the create transform view of Web Admin Console.
     * @apiParam	{binary}	None        Binary String of file content.
     * @apiSuccess {JsonObject[]} uploaded_file_name     The name of the file uploaded.
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     {
     *       "code" : "200",
     *       "uploaded_file_name": "/home/vagrant/flink_word_count.jar",
     *     }
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "uploaded_file_name" : "failed"
     *     }
     */
    private void uploadFiles (RoutingContext c) {
        Iterator<FileUpload> fileUploadIterator = c.fileUploads().iterator();
        for (String fileName; fileUploadIterator.hasNext();) {
			FileUpload fileUpload = fileUploadIterator.next();
			try {
				fileName = URLDecoder.decode(fileUpload.fileName(), "UTF-8");
				String jarPath = new HelpFunc().getCurrentJarRunnigFolder(),
						currentDir = config().getString("upload.dest", jarPath),
						fileToBeCopied = currentDir + HelpFunc.generateUniqueFileName(fileName);
				LOG.debug("===== fileToBeCopied: " + fileToBeCopied);
				vertx.fileSystem().copy(fileUpload.uploadedFileName(), fileToBeCopied, res -> {
					if (!res.succeeded()) {
						HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
								.end(DFAPIMessage.getCustomizedResponseMessage("uploaded_file_name", "Failed"));
					} else {
						LOG.info("FILE COPIED GOOD ==> " + fileToBeCopied);
						HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
								.end(DFAPIMessage.getCustomizedResponseMessage("uploaded_file_name", fileToBeCopied));
					}
				});
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
    }

    /**
     * Generic getAll method for REST API End Point.
     * @param c
     */
    private void getAll(RoutingContext c, String connectorCategoryFilter) {
        mongo.findWithOptions(COLLECTION,
				"all".equalsIgnoreCase(connectorCategoryFilter) ? new JsonObject()
						: new JsonObject().put("$and",
								new JsonArray()
										.add(new JsonObject().put("$where",
												"JSON.stringify(this).indexOf('" + c.request().getParam("q")
														+ "') != -1"))
										.add(new JsonObject().put("connectorCategory", connectorCategoryFilter))),
				HelpFunc.getMongoSortFindOption(c), results -> {
					List<DFJobPOPJ> jobs = results.result().stream().map(DFJobPOPJ::new).collect(Collectors.toList());
					HelpFunc.responseCorsHandleAddOn(c.response()).putHeader("X-Total-Count", String.valueOf(jobs.size()))
							.end(Json.encodePrettily(jobs));
				});
    }

    /**
     * This is for fetch both connects and transforms
     * @param c
     *
     * @api {get} /processor 1.List all tasks
     * @apiVersion 0.1.1
     * @apiName getAll
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get list of all connects and transforms.
     * @apiSuccess	{JsonObject[]}	connects    List of connect and transform task profiles.
     * @apiSampleRequest http://localhost:8080/api/df/processor
     */
    private void getAllProcessor(RoutingContext c) {
        getAll(c, "ALL");
    }

    /**
     * Get all DF connects
     *
     * @param c
     *
     * @api {get} /ps 1.List all connects task
     * @apiVersion 0.1.1
     * @apiName getAllConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is where we get data for all active connects.
     * @apiSuccess	{JsonObject[]}	connects    List of connect task profiles.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     [ {
     *          "id" : "58471d13bba4a429f8a272b6",
     *          "taskSeq" : "1",
     *          "name" : "tesavro",
     *          "connectUid" : "58471d13bba4a429f8a272b6",
     *          "jobUid" : "reserved for job level tracking",
     *          "connectorType" : "CONNECT_KAFKA_SOURCE_AVRO",
     *          "connectorCategory" : "CONNECT",
     *          "description" : "task description",
     *          "status" : "LOST",
     *          "udfUpload" : null,
     *          "jobConfig" : null,
     *          "connectorConfig" : {
     *              "connector.class" : "com.datafibers.kafka.connect.FileGenericSourceConnector",
     *              "schema.registry.uri" : "http://localhost:8081",
     *              "cuid" : "58471d13bba4a429f8a272b6",
     *              "file.location" : "/home/vagrant/df_data/",
     *              "tasks.max" : "1",
     *              "file.glob" : "*.{json,csv}",
     *              "file.overwrite" : "true",
     *              "schema.subject" : "test-value",
     *              "topic" : "testavro"
     *          }
     *       }
     *     ]
     * @apiSampleRequest http://localhost:8080/api/df/ps
     */
    private void getAllConnects(RoutingContext c) {
        getAll(c,"CONNECT");
    }

    /**
     * Get all DF transforms
     *
     * @param c
     *
     * @api {get} /tr 1.List all transforms task
     * @apiVersion 0.1.1
     * @apiName getAllConnects
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is where get data for all active transforms.
     * @apiSuccess	{JsonObject[]}	connects    List of transform task profiles.
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     [ {
     *          "id" : "58471d13bba4a429f8a272b6",
     *          "taskSeq" : "1",
     *          "name" : "tesavro",
     *          "connectUid" : "58471d13bba4a429f8a272b6",
     *          "jobUid" : "reserved for job level tracking",
     *          "connectorType" : "TRANSFORM_FLINK_SQL_A2J",
     *          "connectorCategory" : "TRANSFORM",
     *          "description" : "task description",
     *          "status" : "LOST",
     *          "udfUpload" : null,
     *          "jobConfig" : null,
     *          "connectorConfig" : {
     *              "cuid" : "58471d13bba4a429f8a272b0",
     *              "trans.sql":"SELECT STREAM symbol, name FROM finance"
     *              "group.id":"consumer3",
     *              "topic.for.query":"finance",
     *              "topic.for.result":"stock",
     *              "schema.subject" : "test-value"
     *          }
     *       }
     *     ]
     * @apiSampleRequest http://localhost:8080/api/df/tr
     */
    private void getAllTransforms(RoutingContext c) {
        getAll(c,"TRANSFORM");
    }

    /**
     * Get all ML models for REST API End Point.
     * @param c
     */
    private void getAllModels(RoutingContext c) {
        mongo.findWithOptions(COLLECTION_MODEL, new JsonObject(), HelpFunc.getMongoSortFindOption(c),
                results -> {
                    List<DFModelPOPJ> jobs = results.result().stream().map(DFModelPOPJ::new).collect(Collectors.toList());
                    HelpFunc.responseCorsHandleAddOn(c.response())
                            .putHeader("X-Total-Count", String.valueOf(jobs.size()) )
                            .end(Json.encodePrettily(jobs));
                });
    }

    /**
     * Get all schema from schema registry
     * @param c
     *
     * @api {get} /schema 1.List all schema
     * @apiVersion 0.1.1
     * @apiName getAllSchemas
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is where we get list of available schema data from schema registry.
     * @apiSuccess	{JsonObject[]}	connects    List of schemas added in schema registry.
     * @apiSampleRequest http://localhost:8080/api/df/schema
     */
    public void getAllSchemas(RoutingContext c) {
        ProcessorTopicSchemaRegistry.forwardGetAllSchemas(vertx, c, schema_registry_host_and_port);
    }

    /**
     * List all configurations for Connect or Transforms
     * @param c
     *
     * @api {get} /config 4.List processor lib
     * @apiVersion 0.1.1
     * @apiName getAllProcessorConfigs
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get list of configured or installed connect/transform jar or libraries.
     * @apiSuccess	{JsonObject[]}	config    List of processors' configuration.
     * @apiSampleRequest http://localhost:8080/api/df/config
     */
    private void getAllProcessorConfigs(RoutingContext c) {
        ProcessorConnectKafka.forwardGetAsGetConfig(c, wc_connect, mongo, COLLECTION_INSTALLED,
                kafka_connect_rest_host, kafka_connect_rest_port);
    }

    /**
     * Get all connector process history from df_meta topic sinked into Mongo
     * @param c
     *
     * @api {get} /hist 2.List all processed history
     * @apiVersion 0.1.1
     * @apiName getAllProcessHistory
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get history of processed files in tasks or jobs.
     * @apiSuccess	{JsonObject[]}	history    List of processed history.
     * @apiSampleRequest http://localhost:8080/api/df/hist
     */
    private void getAllProcessHistory(RoutingContext c) {
        mongo.runCommand("aggregate",
				new JsonObject()
						.put("aggregate", config().getString("db.metadata.collection.name", this.COLLECTION_META))
						.put("pipeline", new JsonArray()
								.add(new JsonObject().put("$group",
										new JsonObject()
												.put("_id",
														new JsonObject().put("cuid", "$cuid").put("file_name",
																"$file_name"))
												.put("cuid", new JsonObject().put("$first", "$cuid"))
												.put("file_name", new JsonObject().put("$first", "$file_name"))
												.put("schema_version",
														new JsonObject().put("$first", "$schema_version"))
												.put("last_modified_timestamp",
														new JsonObject().put("$first", "$last_modified_timestamp"))
												.put("file_size", new JsonObject().put("$first", "$file_size"))
												.put("topic_sent", new JsonObject().put("$first", "$topic_sent"))
												.put("schema_subject",
														new JsonObject().put("$first", "$schema_subject"))
												.put("start_time", new JsonObject().put("$min", "$current_timemillis"))
												.put("end_time", new JsonObject().put("$max", "$current_timemillis"))
												.put("status", new JsonObject().put("$min", "$status"))))
								.add(new JsonObject().put("$project", new JsonObject().put("_id", 1)
										.put("id",
												new JsonObject().put("$concat",
														new JsonArray().add("$cuid").add("$file_name")))
										.put("cuid", 1).put("file_name", 1).put("schema_version", 1)
										.put("schema_subject", 1).put("last_modified_timestamp", 1).put("file_owner", 1)
										.put("file_size", 1).put("topic_sent", 1).put("status", 1)
										.put("process_milliseconds",
												new JsonObject().put("$subtract",
														new JsonArray().add("$end_time").add("$start_time")))))
								.add(new JsonObject().put("$sort",
										new JsonObject().put(
												HelpFunc.coalesce(c.request().getParam("_sortField"), "name"),
												HelpFunc.strCompare(
														HelpFunc.coalesce(c.request().getParam("_sortDir"), "ASC"),
														"ASC", 1, -1))))),
				res -> {
					if (!res.succeeded()) {
						res.cause().printStackTrace();
					} else {
						JsonArray resArr = res.result().getJsonArray("result");
						HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
								.putHeader(ConstantApp.HTTP_HEADER_TOTAL_COUNT, String.valueOf(resArr.size()))
								.end(Json.encodePrettily(resArr));
					}
				});
    }

    /**
     * Get all logging information from df repo
     * @param c
     *
     * @api {get} /logs 3.List all df logs in the current run
     * @apiVersion 0.1.1
     * @apiName getAllLogs
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get df logs.
     * @apiSuccess	{JsonObject[]}	logs    List of processed logs.
     * @apiSampleRequest http://localhost:8080/api/df/logs
     */
    private void getAllLogs(RoutingContext c) {
        JsonObject searchCondition;

        String searchKeywords = c.request().getParam("q");
        searchCondition = searchKeywords == null || searchKeywords.isEmpty()
				? new JsonObject().put("level", new JsonObject().put("$ne", "DEBUG"))
				: new JsonObject().put("$and",
						new JsonArray()
								.add(new JsonObject().put("$where",
										"JSON.stringify(this).indexOf('" + searchKeywords + "') != -1"))
								.add(new JsonObject().put("level", new JsonObject().put("$ne", "DEBUG"))));

        mongo.findWithOptions(COLLECTION_LOG, searchCondition, HelpFunc.getMongoSortFindOption(c),
                results -> {
                    List<DFLogPOPJ> jobs = results.result().stream().map(DFLogPOPJ::new).collect(Collectors.toList());
                    HelpFunc.responseCorsHandleAddOn(c.response())
                            .putHeader("X-Total-Count", String.valueOf(jobs.size()) )
                            .end(Json.encodePrettily(jobs));
                });
    }
    /**
     * Get all tasks information using specific topic
     * @param c
     *
     * @api {get} /s2t 5.List all df tasks using specific topic
     * @apiVersion 0.1.1
     * @apiName getAllTasksOneTopic
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where get list of tasks using specific topic.
     * @apiSuccess	{JsonObject[]}	topic    List of tasks related to the topic.
     * @apiSampleRequest http://localhost:8080/api/df/s2t
     */
    private void getAllTasksOneTopic(RoutingContext c) {
        final String topic = c.request().getParam("id");
        if (topic == null) {
			c.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST).end(DFAPIMessage.getResponseMessage(9000));
			LOG.error(DFAPIMessage.getResponseMessage(9000, "TOPIC_IS_NULL"));
		} else {
			mongo.find(COLLECTION,
					HelpFunc.getContainsTopics("connectorConfig", ConstantApp.PK_DF_ALL_TOPIC_ALIAS, topic),
					results -> {
						List<DFJobPOPJ> jobs = results.result().stream().map(DFJobPOPJ::new).collect(Collectors.toList());
						HelpFunc.responseCorsHandleAddOn(c.response()).putHeader("X-Total-Count", String.valueOf(jobs.size()))
								.end(Json.encodePrettily(jobs));
					});
		}
    }

    /**
     * Describe topic with topic specified
     *
     * @api {get} /s2p/:taskId   6. Get partition information for the specific subject/topic
     * @apiVersion 0.1.1
     * @apiName getAllTopicPartitions
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get partition information for the subject/topic.
     * @apiParam {String}   topic      topic name.
     * @apiSuccess	{JsonObject[]}	info.    partition info.
     * @apiSampleRequest http://localhost:8080/api/df/s2p/:taskId
     */
    private void getAllTopicPartitions(RoutingContext c) {
        final String topic = c.request().getParam("id");
        if (topic == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, topic));
        } else {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_server_host_and_port);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, ConstantApp.DF_CONNECT_KAFKA_CONSUMER_GROUP_ID);
            props.put(ConstantApp.SCHEMA_URI_KEY, "http://" + schema_registry_host_and_port);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

            KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
            ArrayList<JsonObject> responseList = new ArrayList<>();

            // Subscribe to a single topic
            consumer.partitionsFor(topic, ar -> {
                if (!ar.succeeded()) {
					LOG.error(DFAPIMessage.logResponseMessage(9030, topic + "-" + ar.cause().getMessage()));
				} else {
					for (PartitionInfo partitionInfo : ar.result()) {
						responseList.add(new JsonObject().put("id", partitionInfo.getTopic())
								.put("partitionNumber", partitionInfo.getPartition())
								.put("leader", partitionInfo.getLeader().getIdString())
								.put("replicas", StringUtils.join(partitionInfo.getReplicas(), ','))
								.put("insyncReplicas", StringUtils.join(partitionInfo.getInSyncReplicas(), ',')));
						HelpFunc.responseCorsHandleAddOn(c.response())
								.putHeader("X-Total-Count", String.valueOf(responseList.size()))
								.end(Json.encodePrettily(responseList));
						consumer.close();
					}
				}
            });

            consumer.exceptionHandler(e -> LOG.error(DFAPIMessage.logResponseMessage(9031, topic + "-" + e.getMessage())));
        }
    }

    /**
     * Poll all available information from specific topic
     * @param c
     *
     * @api {get} /avroconsumer 7.List all df tasks using specific topic
     * @apiVersion 0.1.1
     * @apiName poolAllFromTopic
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where consume data from specific topic in one pool.
     * @apiSuccess	{JsonObject[]}	topic    Consumer from the topic.
     * @apiSampleRequest http://localhost:8080/api/df/avroconsumer
     */
    private void pollAllFromTopic(RoutingContext c) {
        final String topic = c.request().getParam("id");
        if (topic == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "TOPIC_IS_NULL"));
        } else {
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_server_host_and_port);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, ConstantApp.DF_CONNECT_KAFKA_CONSUMER_GROUP_ID);
                props.put(ConstantApp.SCHEMA_URI_KEY, "http://" + schema_registry_host_and_port);
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
                props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

                KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, props);
                ArrayList<JsonObject> responseList = new ArrayList<>();

                consumer.handler(record -> {
                    //LOG.debug("Processing value=" + record.record().value() + ",offset=" + record.record().offset());
                    responseList.add(new JsonObject()
                            .put("id", record.record().offset())
                            .put("value", new JsonObject(record.record().value().toString()))
                            .put("valueString", Json.encodePrettily(new JsonObject(record.record().value().toString())))
                    );
                    if(responseList.size() >= ConstantApp.AVRO_CONSUMER_BATCH_SIE ) {
                        HelpFunc.responseCorsHandleAddOn(c.response())
                                .putHeader("X-Total-Count", String.valueOf(responseList.size()))
                                .end(Json.encodePrettily(responseList));
                        consumer.pause();
                        consumer.commit();
                        consumer.close();
                    }
                });
                consumer.exceptionHandler(e -> LOG.error(DFAPIMessage.logResponseMessage(9031, topic + "-" + e.getMessage())));

                // Subscribe to a single topic
                consumer.subscribe(topic, ar -> {
                    if (ar.succeeded()) {
						LOG.info(DFAPIMessage.logResponseMessage(1027, "topic = " + topic));
					} else {
						LOG.error(DFAPIMessage.logResponseMessage(9030, topic + "-" + ar.cause().getMessage()));
					}
                });
        }
    }

    /**
     * Generic getOne method for REST API End Point.
     * @param routingContext
     *
     * @api {get} /ps/:id    3. Get a connect task
     * @apiVersion 0.1.1
     * @apiName getOne
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is where we get data for one task with specified id.
     * @apiParam {String}   id      task Id (_id in mongodb).
     * @apiSuccess	{JsonObject[]}	connects    One connect task profiles.
     * @apiSampleRequest http://localhost:8080/api/df/ps/:id
     */
    /**
     * @api {get} /tr/:id    3. Get a transform task
     * @apiVersion 0.1.1
     * @apiName getOne
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is where we get data for one task with specified id.
     * @apiParam {String}   id      task Id (_id in mongodb).
     * @apiSuccess	{JsonObject[]}	transforms    One transform task profiles.
     * @apiSampleRequest http://localhost:8080/api/df/tr/:id
     */
    private void getOne(RoutingContext c) {
        final String id = c.request().getParam("id");
        if (id == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "id=null"));
        } else {
			mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
				if (!ar.succeeded()) {
					c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
							.end(DFAPIMessage.getResponseMessage(9002));
					LOG.error(DFAPIMessage.logResponseMessage(9002, id));
				} else {
					if (ar.result() == null) {
						c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
								.end(DFAPIMessage.getResponseMessage(9001));
						LOG.error(DFAPIMessage.logResponseMessage(9001, id));
						return;
					}
					HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
							.end(Json.encodePrettily(new DFJobPOPJ(ar.result())));
					LOG.info(DFAPIMessage.logResponseMessage(1003, id));
				}
			});
		}
    }

    /** Get one model from repository. */
    private void getOneModel(RoutingContext c) {
        final String id = c.request().getParam("id");
        if (id == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "id=null"));
        } else {
			mongo.findOne(COLLECTION_MODEL, new JsonObject().put("_id", id), null, ar -> {
				if (!ar.succeeded()) {
					c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
							.end(DFAPIMessage.getResponseMessage(9002));
					LOG.error(DFAPIMessage.logResponseMessage(9002, id));
				} else {
					if (ar.result() == null) {
						c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
								.end(DFAPIMessage.getResponseMessage(9001));
						LOG.error(DFAPIMessage.logResponseMessage(9001, id));
						return;
					}
					HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
							.end(Json.encodePrettily(ar.result()));
					LOG.info(DFAPIMessage.logResponseMessage(1003, id));
				}
			});
		}
    }

    /**
     * @api {get} /config/:id    5. Get a config info.
     * @apiVersion 0.1.1
     * @apiName getOneProcessorConfig
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get config for one specific processor type.
     * @apiParam {String}   id      config Id (aka. connectorType).
     * @apiSuccess	{JsonObject[]}	all    One processor config.
     * @apiSampleRequest http://localhost:8080/api/df/config/:id
     */
    private void getOneProcessorConfig(RoutingContext c) {
        final String id = c.request().getParam("id");
        if (id == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, "id=null"));
        } else {
			mongo.findOne(COLLECTION_INSTALLED, new JsonObject().put("connectorType", id), null, ar -> {
				if (!ar.succeeded()) {
					c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
							.end(DFAPIMessage.getResponseMessage(9002));
					LOG.error(DFAPIMessage.logResponseMessage(9002, id));
				} else {
					if (ar.result() == null) {
						c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
								.end(DFAPIMessage.getResponseMessage(9001));
						LOG.error(DFAPIMessage.logResponseMessage(9001, id));
						return;
					}
					HelpFunc.responseCorsHandleAddOn(c.response()).setStatusCode(ConstantApp.STATUS_CODE_OK)
							.end(Json.encodePrettily(ar.result()));
					LOG.info(DFAPIMessage.logResponseMessage(1003, id));
				}
			});
		}
    }

    /**
     * Get one schema with schema subject specified
     * 1) Retrieve a specific subject latest information:
     * curl -X GET -i http://localhost:8081/subjects/Kafka-value/versions/latest
     *
     * 2) Retrieve a specific subject compatibility:
     * curl -X GET -i http://localhost:8081/config/finance-value
     *
     * @api {get} /schema/:subject   2. Get a schema
     * @apiVersion 0.1.1
     * @apiName getOneSchema
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is where we get schema with specified schema subject.
     * @apiParam {String}   subject      schema subject name in schema registry.
     * @apiSuccess	{JsonObject[]}	schema    One schema object.
     * @apiSampleRequest http://localhost:8080/api/df/schema/:subject
     */
    private void getOneSchema(RoutingContext c) {
        ProcessorTopicSchemaRegistry.forwardGetOneSchema(c, wc_schema,
                kafka_server_host, schema_registry_rest_port);
    }

    /**
     * Get one task logs with task id specified from repo
     *
     * @api {get} /logs/:taskId   2. Get a task status
     * @apiVersion 0.1.1
     * @apiName getOneLogs
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get task logs with specified task id.
     * @apiParam {String}   taskId      taskId for connect or transform.
     * @apiSuccess	{JsonObject[]}	logs    logs of the task.
     * @apiSampleRequest http://localhost:8080/api/df/logs/:taskId
     *
     *
     * @apiSuccessExample {json} Success-Response:
     *     HTTP/1.1 200 OK
     *     [ {
     *          "_id" : ObjectId("59827ca37985374c290c0bfd"),
     *          "timestamp" : ISODate("2017-08-03T01:30:11.720Z"),
     *          "level" : "INFO",
     *          "thread" : "vert.x-eventloop-thread-0",
     *          "message" : "{\"code\":\"1015\",\"message\":\"INFO - IMPORT_ACTIVE_CONNECTS_STARTED_AT_STARTUP\",\"comments\":\"CONNECT_IMPORT\"}",
     *          "loggerName" : {
     *          "fullyQualifiedClassName" : "com.datafibers.service.DFDataProcessor",
     *          "package" : [
     *          "com",
     *          "datafibers",
     *          "service",
     *          "DFDataProcessor"
     *          ],
     *          "className" : "DFDataProcessor"
     *          },
     *          "fileName" : "DFDataProcessor.java",
     *          "method" : "importAllFromKafkaConnect",
     *          "lineNumber" : "1279",
     *          "class" : {
     *          "fullyQualifiedClassName" : "com.datafibers.service.DFDataProcessor",
     *          "package" : [
     *          "com",
     *          "datafibers",
     *          "service",
     *          "DFDataProcessor"
     *          ],
     *          "className" : "DFDataProcessor"
     *          },
     *          "host" : {
     *          "process" : "19497@vagrant",
     *          "name" : "vagrant",
     *          "ip" : "127.0.1.1"
     *          }
     *          }
     *     ]
     * @apiSampleRequest http://localhost:8080/api/logs/:id
     */
    private void getOneLogs(RoutingContext c) {
        final String id = c.request().getParam("id");
        if (id == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, id));
        } else {
			mongo.findWithOptions(COLLECTION_LOG, new JsonObject().put("message", new JsonObject().put("$regex", id)),
					HelpFunc.getMongoSortFindOption(c), results -> {
						List<JsonObject> jobs = results.result();
						HelpFunc.responseCorsHandleAddOn(c.response()).putHeader("X-Total-Count", String.valueOf(jobs.size()))
								.end(Json.encodePrettily(jobs));
					});
		}
    }

    /**
     * Get one task LIVE status with task id specified
     *
     * @api {get} /status/:taskId   2. Get a task status
     * @apiVersion 0.1.1
     * @apiName getOneStatus
     * @apiGroup All
     * @apiPermission none
     * @apiDescription This is where we get task status with specified task id.
     * @apiParam {String}   taskId      taskId for connect or transform.
     * @apiSuccess	{JsonObject[]}	status    status of the task.
     * @apiSampleRequest http://localhost:8080/api/df/status/:taskId
     */
    private void getOneStatus(RoutingContext c) {
        final String id = c.request().getParam("id");
        if (id == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.getResponseMessage(9000, id));
        } else {
			mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
				if (!ar.succeeded()) {
					c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
							.end(DFAPIMessage.getResponseMessage(9002));
					LOG.error(DFAPIMessage.logResponseMessage(9002, id));
				} else {
					if (ar.result() == null) {
						c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
								.end(DFAPIMessage.getResponseMessage(9001));
						LOG.error(DFAPIMessage.logResponseMessage(9001, id));
						return;
					}
					DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
					if ("CONNECT".equalsIgnoreCase(dfJob.getConnectorCategory())) {
						ProcessorConnectKafka.forwardGetAsGetOne(c, wc_connect, kafka_connect_rest_host,
								kafka_connect_rest_port, id);
					}
					if ("TRANSFORM".equalsIgnoreCase(dfJob.getConnectorCategory())) {
						if (dfJob.getConnectorType().contains("FLINK")) {
							ProcessorTransformFlink.forwardGetAsJobStatus(c, wc_flink, flink_server_host,
									flink_rest_server_port, id, dfJob.getFlinkIDFromJobConfig());
						}
						if (dfJob.getConnectorType().contains("SPARK")) {
							ProcessorTransformSpark.forwardGetAsJobStatus(c, wc_spark, dfJob, spark_livy_server_host,
									spark_livy_server_port);
						}
					}
				}
			});
		}
    }

    /**
     * Connects specific addOne End Point for Rest API
     * @param c
     *
     * @api {post} /ps 4.Add a connect task
     * @apiVersion 0.1.1
     * @apiName addOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how we add or submit a connect to DataFibers.
     * @apiParam    {String}  None        Json String of task as message body.
     * @apiSuccess (201) {JsonObject[]} connect     The newly added connect task.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 409 Conflict
     *     {
     *       "code" : "409",
     *       "message" : "POST Request exception - Conflict"
     *     }
     */
    private void addOneConnects(RoutingContext c) {
        final DFJobPOPJ dfJob = Json.decodeValue(
                HelpFunc.cleanJsonConfig(c.getBodyAsString()), DFJobPOPJ.class);
        // Set initial status for the job
        dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
        String mongoId = (dfJob.getId() != null && !dfJob.getId().isEmpty())? dfJob.getId() : new ObjectId().toString();
        dfJob.setConnectUid(mongoId).setId(mongoId).getConnectorConfig().put("cuid", mongoId);
        LOG.debug("newly added connect is " + dfJob.toJson());
        if (dfJob.getConnectorConfig().containsKey(ConstantApp.PK_KAFKA_CONNECTOR_CLASS)
				&& dfJob.getConnectorConfig().get(ConstantApp.PK_KAFKA_CONNECTOR_CLASS) != null) {
			LOG.debug("Use connector.class in the config received");
		} else {
			String connectorClass = mongoDFInstalled.lkpCollection("connectorType", dfJob.getConnectorType(), "class");
			if (connectorClass == null || connectorClass.isEmpty()) {
				LOG.info(DFAPIMessage.logResponseMessage(9024, mongoId + " - " + connectorClass));
			} else {
				dfJob.getConnectorConfig().put(ConstantApp.PK_KAFKA_CONNECTOR_CLASS, connectorClass);
				LOG.info(DFAPIMessage.logResponseMessage(1018, mongoId + " - " + connectorClass));
			}
		}
        if (!this.kafkaConnectEnabled || !dfJob.getConnectorType().contains("CONNECT")) {
			mongo.insert(COLLECTION, dfJob.toJson(), r -> HelpFunc.responseCorsHandleAddOn(c.response())
					.setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED).end(Json.encodePrettily(dfJob)));
			LOG.warn(DFAPIMessage.logResponseMessage(9008, mongoId));
		} else {
			if (!dfJob.getConnectUid().equalsIgnoreCase(dfJob.getConnectorConfig().get("name"))
					&& dfJob.getConnectorConfig().get("name") != null) {
				dfJob.getConnectorConfig().put("name", dfJob.getConnectUid());
				LOG.info(DFAPIMessage.logResponseMessage(1004, dfJob.getId()));
			}
			ProcessorConnectKafka.forwardPOSTAsAddOne(c, wc_connect, mongo, COLLECTION, kafka_connect_rest_host,
					kafka_connect_rest_port, dfJob);
		}
    }

    /**
     * Transforms specific addOne End Point for Rest API
     * @param c
     *
     * @api {post} /tr 4.Add a transform task
     * @apiVersion 0.1.1
     * @apiName addOneTransforms
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is how we submit or add a transform task to DataFibers.
     * @apiParam   {String}	 None        Json String of task as message body.
     * @apiSuccess (201) {JsonObject[]} connect     The newly added connect task.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 409 Conflict
     *     {
     *       "code" : "409",
     *       "message" : "POST Request exception - Conflict."
     *     }
     */
    private void addOneTransforms(RoutingContext c) {
        String rawResBody = HelpFunc.cleanJsonConfig(c.getBodyAsString());
        final DFJobPOPJ dfJob = Json.decodeValue(rawResBody, DFJobPOPJ.class);

        dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
        String mongoId = (dfJob.getId() != null && !dfJob.getId().isEmpty())? dfJob.getId() : new ObjectId().toString();
        dfJob.setConnectUid(mongoId).setId(mongoId).getConnectorConfig().put(ConstantApp.PK_TRANSFORM_CUID, mongoId);

        if(dfJob.getConnectorType().contains("SPARK") && dfJob.getConnectorType().contains("TRANSFORM")) {
            LOG.info("calling spark engine with body = " + dfJob.toJson());
            ProcessorTransformSpark.forwardPostAsAddOne(vertx, wc_spark, dfJob, mongo, COLLECTION,
                    spark_livy_server_host, spark_livy_server_port, rawResBody
            );
        } else {
            // Flink refers to KafkaServerHostPort.java
            JsonObject para = HelpFunc.getFlinkJarPara(dfJob,
                    this.kafka_server_host_and_port,
                    this.schema_registry_host_and_port);

            ProcessorTransformFlink.forwardPostAsSubmitJar(wc_flink, dfJob, mongo, COLLECTION,
                    flink_server_host, flink_rest_server_port, flink_jar_id,
                    para.getString("allowNonRestoredState"),
                    para.getString("savepointPath"),
                    para.getString("entryClass"),
                    para.getString("parallelism"),
                    para.getString("programArgs"));
        }

        mongo.insert(COLLECTION, dfJob.toJson(), r ->
                HelpFunc.responseCorsHandleAddOn(c.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                        .end(Json.encodePrettily(dfJob)));
        LOG.info(DFAPIMessage.logResponseMessage(1000, dfJob.getId()));
    }

    /** Add one model's meta information to repository. */
    private void addOneModel(RoutingContext c) {
        String rawResBody = HelpFunc.cleanJsonConfig(c.getBodyAsString());
        LOG.debug("TESTING addOneModel - resRaw = " + rawResBody);
        final DFModelPOPJ dfModel = Json.decodeValue(rawResBody, DFModelPOPJ.class);
        dfModel.setId((dfModel.getId() != null && !dfModel.getId().isEmpty()) ? dfModel.getId() : new ObjectId().toString());

        mongo.insert(COLLECTION_MODEL, dfModel.toJson(), r ->
                HelpFunc.responseCorsHandleAddOn(c.response())
                        .setStatusCode(ConstantApp.STATUS_CODE_OK_CREATED)
                        .end(Json.encodePrettily(dfModel)));
        LOG.info(DFAPIMessage.logResponseMessage(1000, dfModel.getId()));
    }

    /** Add one schema to schema registry
     *
     * @api {post} /schema 3.Add a Schema
     * @apiVersion 0.1.1
     * @apiName addOneSchema
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is how we add a new schema to schema registry service
     * @apiParam   {String}  None        Json String of Schema as message body.
     * @apiSuccess (201) {JsonObject[]} connect     The newly added connect task.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 409 Conflict
     *     {
     *       "code" : "409",
     *       "message" : "POST Request exception - Conflict"
     *     }
     */
    private void addOneSchema(RoutingContext c) {
        ProcessorTopicSchemaRegistry.forwardAddOneSchema(c, wc_schema,
                kafka_server_host, schema_registry_rest_port);

        JsonObject jsonObj = c.getBodyAsJson();
        // Since Vertx kafka admin still need zookeeper, we now use kafka native admin api until vertx version get updated
        KafkaAdminClient.createTopic(kafka_server_host_and_port,
                jsonObj.getString("id"),
                !jsonObj.containsKey(ConstantApp.TOPIC_KEY_PARTITIONS) ? 1
						: jsonObj.getInteger(ConstantApp.TOPIC_KEY_PARTITIONS),
                !jsonObj.containsKey(ConstantApp.TOPIC_KEY_REPLICATION_FACTOR) ? 1
						: jsonObj.getInteger(ConstantApp.TOPIC_KEY_REPLICATION_FACTOR)
        );
    }

    /**
     * Connects specific pause or resume End Point for Rest API
     * @param c
     *
     * @api {put} /ps/:id   5.Pause or resume a connect task
     * @apiVersion 0.1.1
     * @apiName updateOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how we pause or resume a connect task.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void putOneConnect(RoutingContext c) {
        final DFJobPOPJ dfJob = Json.decodeValue(c.getBodyAsString(),DFJobPOPJ.class);
        String status = dfJob.getStatus();
        if (!ConstantApp.KAFKA_CONNECT_ACTION_PAUSE.equalsIgnoreCase(status)
				&& !ConstantApp.KAFKA_CONNECT_ACTION_RESUME.equalsIgnoreCase(status)) {
			updateOneConnects(c, "restart".equalsIgnoreCase(status));
		} else {
			ProcessorConnectKafka.forwardPUTAsPauseOrResumeOne(c, wc_connect, mongo, COLLECTION,
					kafka_connect_rest_host, kafka_connect_rest_port, dfJob, status);
		}
    }

    /**
     * Connects specific updateOne End Point for Rest API
     * @param c
     *
     * @api {put} /ps/:id   5.Update a connect task
     * @apiVersion 0.1.1
     * @apiName updateOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how we update the connect configuration to DataFibers.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void updateOneConnects(RoutingContext c, Boolean enforceSubmit) {
        final String id = c.request().getParam("id");
        final DFJobPOPJ dfJob = Json.decodeValue(c.getBodyAsString(),DFJobPOPJ.class);

        String connectorConfigString = HelpFunc.mapToJsonStringFromHashMapD2U(dfJob.getConnectorConfig());
        if (id == null || dfJob.toJson() == null) {
			c.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST).end(DFAPIMessage.getResponseMessage(9000));
			LOG.error(DFAPIMessage.logResponseMessage(9000, id));
		} else {
			mongo.findOne(COLLECTION, new JsonObject().put("_id", id), new JsonObject().put("connectorConfig", 1),
					res -> {
						if (!res.succeeded()) {
							LOG.error(DFAPIMessage.logResponseMessage(9014, id + " details - " + res.cause()));
						} else {
							String before_update_connectorConfigString = res.result().getJsonObject("connectorConfig")
									.toString();
							if (enforceSubmit || (this.kafkaConnectEnabled
									&& dfJob.getConnectorType().contains("CONNECT")
									&& connectorConfigString.compareTo(before_update_connectorConfigString) != 0)) {
								ProcessorConnectKafka.forwardPUTAsUpdateOne(c, wc_connect, mongo, COLLECTION,
										kafka_connect_rest_host, kafka_connect_rest_port, dfJob);
							} else {
								LOG.info(DFAPIMessage.logResponseMessage(1007, id));
								mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id),
										new JsonObject().put("$set", dfJob.toJson()), v -> {
											if (!v.failed()) {
												HelpFunc.responseCorsHandleAddOn(c.response())
														.end(DFAPIMessage.getResponseMessage(1001));
												LOG.info(DFAPIMessage.logResponseMessage(1001, id));
											} else {
												c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
														.end(DFAPIMessage.getResponseMessage(9003));
												LOG.error(DFAPIMessage.logResponseMessage(9003, id));
											}
										});
							}
						}
					});
		}
    }

    /**
     * Transforms specific updateOne End Point for Rest API
     * @param c
     *
     * @api {put} /tr/:id   5.Update a transform task
     * @apiVersion 0.1.1
     * @apiName updateOneConnects
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is how we update the transform configuration to DataFibers.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void updateOneTransforms(RoutingContext c) {
        final String id = c.request().getParam("id");
        final DFJobPOPJ dfJob = Json.decodeValue(c.getBodyAsString(), DFJobPOPJ.class);
        String connectorConfigString = HelpFunc.mapToJsonStringFromHashMapD2U(dfJob.getConnectorConfig());
        if (id == null || dfJob.toJson() == null) {
			c.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST).end(DFAPIMessage.getResponseMessage(9000));
			LOG.error(DFAPIMessage.logResponseMessage(9000, id));
		} else {
			mongo.findOne(COLLECTION, new JsonObject().put("_id", id), new JsonObject().put("connectorConfig", 1),
					res -> {
						if (!res.succeeded()) {
							LOG.error(DFAPIMessage.logResponseMessage(9014, id + " details - " + res.cause()));
						} else {
							String before_update_connectorConfigString = res.result().getJsonObject("connectorConfig")
									.toString();
							if (this.transform_engine_flink_enabled && dfJob.getConnectorType().contains("FLINK")
									&& connectorConfigString.compareTo(before_update_connectorConfigString) != 0) {
								JsonObject para = HelpFunc.getFlinkJarPara(dfJob, this.kafka_server_host_and_port,
										this.schema_registry_host_and_port);
								if (!dfJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())) {
									ProcessorTransformFlink.forwardPostAsSubmitJar(wc_flink, dfJob, mongo, COLLECTION,
											flink_server_host, flink_rest_server_port, flink_jar_id,
											para.getString("allowNonRestoredState"), para.getString("savepointPath"),
											para.getString("entryClass"), para.getString("parallelism"),
											para.getString("programArgs"));
								} else {
									ProcessorTransformFlink.forwardPutAsRestartJob(c, wc_flink, mongo,
											COLLECTION_INSTALLED, COLLECTION, flink_server_host, flink_rest_server_port,
											flink_jar_id, dfJob.getJobConfig().get(ConstantApp.PK_FLINK_SUBMIT_JOB_ID),
											dfJob, para.getString("allowNonRestoredState"),
											para.getString("savepointPath"), para.getString("entryClass"),
											para.getString("parallelism"), para.getString("programArgs"));
								}
								dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
							} else if (!this.transform_engine_spark_enabled || !dfJob.getConnectorType().contains("SPARK")
									|| connectorConfigString.compareTo(before_update_connectorConfigString) == 0) {
								LOG.info(DFAPIMessage.logResponseMessage(1007, id));
							} else {
								dfJob.setStatus(ConstantApp.DF_STATUS.UNASSIGNED.name());
								if ("true".equalsIgnoreCase(
										dfJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG))) {
									dfJob.setConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_TASK_STATE,
											ConstantApp.DF_STATUS.UNASSIGNED.name());
								}
								ProcessorTransformSpark.forwardPutAsUpdateOne(vertx, wc_spark, dfJob, mongo, COLLECTION,
										spark_livy_server_host, spark_livy_server_port);
							}
							mongo.updateCollection(COLLECTION, new JsonObject().put("_id", id),
									new JsonObject().put("$set", dfJob.toJson()), v -> {
										if (!v.failed()) {
											HelpFunc.responseCorsHandleAddOn(c.response())
													.end(DFAPIMessage.getResponseMessage(1001));
											LOG.info(DFAPIMessage.logResponseMessage(1001, id));
										} else {
											c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
													.end(DFAPIMessage.getResponseMessage(9003));
											LOG.error(DFAPIMessage.logResponseMessage(9003, id));
										}
									});
						}
					});
		}
    }

    /** Update one model information. */
    private void updateOneModel(RoutingContext c) {
        final String id = c.request().getParam("id");
        if (id == null) {
			c.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST).end(DFAPIMessage.getResponseMessage(9000));
			LOG.error(DFAPIMessage.logResponseMessage(9000, id));
		} else {
			mongo.updateCollection(COLLECTION_MODEL, new JsonObject().put("_id", id),
					new JsonObject().put("$set", Json.decodeValue(c.getBodyAsString(), DFModelPOPJ.class).toJson()),
					v -> {
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
     * Update specified schema in schema registry
     * @api {put} /schema/:id   4.Update a schema
     * @apiVersion 0.1.1
     * @apiName updateOneSchema
     * @apiGroup Schema
     * @apiPermission none
     * @apiDescription This is how we update specified schema information in schema registry.
     * @apiParam    {String}    subject  schema subject in schema registry.
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 404 Not Found
     *     {
     *       "code" : "409",
     *       "message" : "PUT Request exception - Not Found."
     *     }
     */
    private void updateOneSchema(RoutingContext c) {
        ProcessorTopicSchemaRegistry.forwardUpdateOneSchema(c, wc_schema,
                kafka_server_host, schema_registry_rest_port);
    }

    /**
     * Connects specific deleteOne End Point for Rest API
     * @param c
     *
     * @api {delete} /ps/:id   6.Delete a connect task
     * @apiVersion 0.1.1
     * @apiName deleteOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how to delete a specific connect.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "message" : "Delete Request exception - Bad Request."
     *     }
     */
    private void deleteOneConnects(RoutingContext c) {
        String id = c.request().getParam("id");
                if (id == null) {
            c.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
					mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
						if (ar.succeeded()) {
							if (ar.result() == null) {
								c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
										.end(DFAPIMessage.getResponseMessage(9001));
								LOG.error(DFAPIMessage.logResponseMessage(9001, id));
								return;
							}
							DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
							if (this.kafkaConnectEnabled && dfJob.getConnectorType().contains("CONNECT")) {
								ProcessorConnectKafka.forwardDELETEAsDeleteOne(c, wc_connect, mongo, COLLECTION,
										kafka_connect_rest_host, kafka_connect_rest_port, dfJob);
							} else {
								mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
										remove -> c.response().end(DFAPIMessage.getResponseMessage(1002, id)));
								LOG.info(DFAPIMessage.logResponseMessage(1002, id));
							}
						}
					});
				}
    }
	    /**
     * Transforms specific deleteOne End Point for Rest API
     * @param c
     *
     * @api {delete} /tr/:id   6.Delete a transform task
     * @apiVersion 0.1.1
     * @apiName deleteOneTransforms
     * @apiGroup Transform
     * @apiPermission none
     * @apiDescription This is how to delete a specific transform.
     * @apiParam    {String}    id  task Id (_id in mongodb).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "message" : "Delete Request exception - Bad Request."
     *     }
     */
    private void deleteOneTransforms(RoutingContext c) {
        String id = c.request().getParam("id");

        if (id == null) {
            c.response()
                    .setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
			mongo.findOne(COLLECTION, new JsonObject().put("_id", id), null, ar -> {
				if (!ar.succeeded()) {
					c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
							.end(DFAPIMessage.getResponseMessage(9001));
					LOG.error(DFAPIMessage.logResponseMessage(9001, id));
				} else {
					DFJobPOPJ dfJob = new DFJobPOPJ(ar.result());
					String jobId;
					if (dfJob.getConnectorConfig().containsKey(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH)) {
						File streamBackFolder = new File(
								dfJob.getConnectorConfig(ConstantApp.PK_TRANSFORM_STREAM_BACK_PATH));
						try {
							FileUtils.deleteDirectory(streamBackFolder);
							LOG.info("STREAM_BACK_FOLDER_DELETED " + streamBackFolder);
						} catch (IOException ioe) {
							LOG.error("DELETE_STREAM_BACK_FOLDER_FAILED " + ioe.getCause());
						}
					}
					if (dfJob.getJobConfig() == null) {
						mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
								remove -> HelpFunc.responseCorsHandleAddOn(c.response())
										.setStatusCode(ConstantApp.STATUS_CODE_OK)
										.end(DFAPIMessage.getResponseMessage(1002)));
						LOG.info(DFAPIMessage.logResponseMessage(1002, id + "- service job info not found"));
					} else if (dfJob.getJobConfig() != null && this.transform_engine_flink_enabled
							&& dfJob.getConnectorType().contains("FLINK")
							&& dfJob.getJobConfig().containsKey(ConstantApp.PK_FLINK_SUBMIT_JOB_ID)) {
						jobId = dfJob.getJobConfig().get(ConstantApp.PK_FLINK_SUBMIT_JOB_ID);
						if ("RUNNING".equalsIgnoreCase(dfJob.getStatus())) {
							ProcessorTransformFlink.forwardDeleteAsCancelJob(c, wc_flink, mongo, COLLECTION,
									this.flink_server_host, this.flink_rest_server_port, jobId);
							LOG.info(DFAPIMessage.logResponseMessage(1006, id));
						} else {
							mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
									remove -> HelpFunc.responseCorsHandleAddOn(c.response())
											.setStatusCode(ConstantApp.STATUS_CODE_OK)
											.end(DFAPIMessage.getResponseMessage(1002)));
							LOG.info(DFAPIMessage.logResponseMessage(1002, id + "- FLINK_JOB_NOT_RUNNING"));
						}
					} else if (dfJob.getJobConfig() == null || !this.transform_engine_spark_enabled
							|| !dfJob.getConnectorType().contains("SPARK")
							|| !dfJob.getJobConfig().containsKey(ConstantApp.PK_LIVY_SESSION_ID)) {
						mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
								remove -> HelpFunc.responseCorsHandleAddOn(c.response())
										.setStatusCode(ConstantApp.STATUS_CODE_OK)
										.end(DFAPIMessage.getResponseMessage(1002)));
						LOG.info(DFAPIMessage.logResponseMessage(1002,
								id + "- Has jobConfig but missing service job id."));
					} else {
						jobId = dfJob.getJobConfig().get(ConstantApp.PK_LIVY_SESSION_ID);
						if ("RUNNING".equalsIgnoreCase(dfJob.getStatus())) {
							ProcessorTransformSpark.forwardDeleteAsCancelOne(c, wc_spark, mongo, COLLECTION,
									this.spark_livy_server_host, this.spark_livy_server_port, jobId);
							LOG.info(DFAPIMessage.logResponseMessage(1006, id));
						} else {
							mongo.removeDocument(COLLECTION, new JsonObject().put("_id", id),
									remove -> HelpFunc.responseCorsHandleAddOn(c.response())
											.setStatusCode(ConstantApp.STATUS_CODE_OK)
											.end(DFAPIMessage.getResponseMessage(1002)));
							LOG.info(DFAPIMessage.logResponseMessage(1002, id + "- SPARK_JOB_NOT_RUNNING"));
						}
					}
				}
			});
		}
    }

    /**
     * Delete model information as well as mode it's self. Now, we use hdfs cml for instead
     */
    private void deleteOneModel(RoutingContext c) {
        String id = c.request().getParam("id");

        if (id == null) {
            c.response().setStatusCode(ConstantApp.STATUS_CODE_BAD_REQUEST)
                    .end(DFAPIMessage.getResponseMessage(9000));
            LOG.error(DFAPIMessage.logResponseMessage(9000, id));
        } else {
			mongo.findOne(COLLECTION_MODEL, new JsonObject().put("_id", id), null, ar -> {
				if (ar.succeeded()) {
					if (ar.result() == null) {
						c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
								.end(DFAPIMessage.getResponseMessage(9001));
						LOG.error(DFAPIMessage.logResponseMessage(9001, id));
						return;
					}
					DFModelPOPJ dfModelPOPJ = new DFModelPOPJ(ar.result());
					wc_connect
							.delete(webhdfs_rest_port, webhdfs_rest_hostname,
									ConstantApp.WEBHDFS_REST_URL + "/" + dfModelPOPJ.getPath()
											+ ConstantApp.WEBHDFS_REST_DELETE_PARA)
							.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
									ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
							.sendJsonObject(DFAPIMessage.getResponseJsonObj(1002), ard -> {
								if (!ard.succeeded()) {
									c.response().setStatusCode(ConstantApp.STATUS_CODE_NOT_FOUND)
											.end(DFAPIMessage.getResponseMessage(9001));
									LOG.error(DFAPIMessage.logResponseMessage(9001, id));
								} else {
									mongo.removeDocument(COLLECTION_MODEL, new JsonObject().put("_id", id),
											remove -> c.response().end(DFAPIMessage.getResponseMessage(1002, id)));
									LOG.info(DFAPIMessage.logResponseMessage(1002, id));
								}
							});
				}
			});
		}
    }

    /**
     * Schema specific deleteOne End Point for Rest API
     * @param c
     *
     * @api {delete} /schema/:id   6.Delete a schema/topic
     * @apiVersion 0.1.1
     * @apiName deleteOneConnects
     * @apiGroup Connect
     * @apiPermission none
     * @apiDescription This is how to delete a specific schema/topic.
     * @apiParam    {String}    id  schema subject(or topic).
     * @apiSuccess  {String}    message     OK.
     * @apiError    code        The error code.
     * @apiError    message     The error message.
     * @apiErrorExample {json} Error-Response:
     *     HTTP/1.1 400 Bad Request
     *     {
     *       "code" : "400",
     *       "message" : "Delete Request exception - Bad Request."
     *     }
     */
    private void deleteOneSchema(RoutingContext c) {
        ProcessorTopicSchemaRegistry.forwardDELETEAsDeleteOne(c, wc_schema,
                kafka_server_host, schema_registry_rest_port);
        KafkaAdminClient.deleteTopics(kafka_server_host_and_port, c.request().getParam("id"));
    }

    /**
     * Start a Kafka connect in background to keep sinking meta data to mongodb
     * This is blocking process since it is a part of application initialization.
     */
    private void startMetadataSink() {
        String restURI = "http://" + this.kafka_connect_rest_host + ":" + this.kafka_connect_rest_port
				+ ConstantApp.KAFKA_CONNECT_REST_URL,
				metaDBHost = config().getString("repo.connection.string", "mongodb://localhost:27017").replace("//", "")
						.split(":")[1],
				metaDBPort = config().getString("repo.connection.string", "mongodb://localhost:27017").replace("//", "")
						.split(":")[2],
				metaDBName = config().getString("db.name", "DEFAULT_DB");
        // Create meta-database if it is not exist
        new MongoAdminClient(metaDBHost, Integer.parseInt(metaDBPort), metaDBName)
                .createCollection(this.COLLECTION_META)
                .close();

        String metaSinkConnect = new JSONObject().put("name", "metadata_sink_connect").put("config",
                new JSONObject().put("connector.class", "org.apache.kafka.connect.mongodb.MongodbSinkConnector")
                        .put("tasks.max", "2")
                        .put("host", metaDBHost)
                        .put("port", metaDBPort)
                        .put("bulk.size", "1")
                        .put("mongodb.database", metaDBName)
                        .put("mongodb.collections", config().getString("db.metadata.collection.name", this.COLLECTION_META))
                        .put("topics", config().getString("kafka.topic.df.metadata", "df_meta"))).toString();
        try {
            HttpResponse<String> res = Unirest.get(restURI + "/metadata_sink_connect/status")
                    .header("accept", ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if(res.getStatus() == ConstantApp.STATUS_CODE_NOT_FOUND) {
				Unirest.post(restURI).header("accept", "application/json").header("Content-Type", "application/json")
						.body(metaSinkConnect).asString();
			}

            String dfMetaSchemaSubject = config().getString("kafka.topic.df.metadata", "df_meta"),
					schemaRegistryRestURL = "http://" + this.schema_registry_host_and_port + "/subjects/"
							+ dfMetaSchemaSubject + "/versions";
            HttpResponse<String> schmeaRes = Unirest.get(schemaRegistryRestURL + "/latest")
                    .header("accept", ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
                    .asString();

            if (schmeaRes.getStatus() != ConstantApp.STATUS_CODE_NOT_FOUND) {
				LOG.info(DFAPIMessage.logResponseMessage(1009, "META_DATA_SCHEMA_REGISTRATION"));
			} else {
				Unirest.post(schemaRegistryRestURL).header("accept", ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
						.header("Content-Type", ConstantApp.AVRO_REGISTRY_CONTENT_TYPE)
						.body(new JSONObject().put("schema",
								config().getString("df.metadata.schema",
										"{\"type\":\"record\",\"name\": \"df_meta\",\"fields\":["
												+ "{\"name\": \"cuid\", \"type\": \"string\"},"
												+ "{\"name\": \"file_name\", \"type\": \"string\"},"
												+ "{\"name\": \"file_size\", \"type\": \"string\"}, "
												+ "{\"name\": \"file_owner\", \"type\": \"string\"},"
												+ "{\"name\": \"last_modified_timestamp\", \"type\": \"string\"},"
												+ "{\"name\": \"current_timestamp\", \"type\": \"string\"},"
												+ "{\"name\": \"current_timemillis\", \"type\": \"long\"},"
												+ "{\"name\": \"stream_offset\", \"type\": \"string\"},"
												+ "{\"name\": \"topic_sent\", \"type\": \"string\"},"
												+ "{\"name\": \"schema_subject\", \"type\": \"string\"},"
												+ "{\"name\": \"schema_version\", \"type\": \"string\"},"
												+ "{\"name\": \"status\", \"type\": \"string\"}]}"))
								.toString())
						.asString();
				LOG.info(DFAPIMessage.logResponseMessage(1008, "META_DATA_SCHEMA_REGISTRATION"));
			}

            JSONObject resObj = new JSONObject(res.getBody());
            LOG.info(
					DFAPIMessage.logResponseMessage(1010,
							"topic:df_meta, status:" + (!resObj.has("connector") ? "Unknown"
									: resObj.getJSONObject("connector").get("state").toString())));
        } catch (UnirestException ue) {
            LOG.error(DFAPIMessage
                    .logResponseMessage(9015, "exception details - " + ue.getCause()));
        }
    }

    /**
     * Get initial method to import all active connectors from Kafka connect to mongodb repo
     * The same function does not apply to the transforms since we are not able to rebuild their configs.
     */
    private void importAllFromKafkaConnect() {
        LOG.info(DFAPIMessage.logResponseMessage(1015, "CONNECT_IMPORT"));
        String restURI = "http://" + this.kafka_connect_rest_host+ ":" + this.kafka_connect_rest_port +
                ConstantApp.KAFKA_CONNECT_REST_URL;
        try {
            String resStr = Unirest.get(restURI).header("accept", "application/json").asString().getBody();
            if (resStr.compareToIgnoreCase("[]") == 0 || "[null]".equalsIgnoreCase(resStr)) {
				LOG.info(DFAPIMessage.logResponseMessage(1013, "CONNECT_IMPORT"));
			} else {
				for (String connectName : resStr.substring(2, resStr.length() - 2).split("\",\"")) {
					if ("null".equalsIgnoreCase(connectName)) {
						continue;
					}
					JsonNode resConfig = Unirest.get(restURI + "/" + connectName + "/config").header("accept", "application/json").asJson().getBody();
					String resConnectType = "metadata_sink_connect".equalsIgnoreCase(resConfig.getObject().getString("name"))
							? ConstantApp.DF_CONNECT_TYPE.INTERNAL_METADATA_COLLECT.name()
							: mongoDFInstalled.lkpCollection("class",
									!resConfig.getObject().has("connector.class")
											? ConstantApp.DF_CONNECT_TYPE.NONE.name()
											: resConfig.getObject().getString("connector.class"),
									"connectorType");
					HttpResponse<JsonNode> resConnectorStatus = Unirest.get(restURI + "/" + connectName + "/status")
							.header("accept", "application/json").asJson();
					String resStatus = resConnectorStatus.getStatus() != 200 ? ConstantApp.DF_STATUS.LOST.name()
							: HelpFunc.getTaskStatusKafka(resConnectorStatus.getBody().getObject());
					mongo.count(COLLECTION, new JsonObject().put("connectUid", connectName), count -> {
						if (!count.succeeded()) {
							LOG.error(DFAPIMessage.logResponseMessage(9018,
									"exception_details - " + connectName + " - " + count.cause()));
						} else if (count.result() == 0) {
							DFJobPOPJ insertJob = new DFJobPOPJ(new JsonObject().put("_id", connectName)
									.put("name", "imported " + connectName).put("taskSeq", "0")
									.put("connectUid", connectName).put("connectorType", resConnectType)
									.put("connectorCategory", "CONNECT").put("status", resStatus)
									.put("jobConfig",
											new JsonObject().put("comments", "This is imported from Kafka Connect."))
									.put("connectorConfig", new JsonObject(resConfig.getObject().toString())));
							mongo.insert(COLLECTION, insertJob.toJson(), ar -> {
								if (!ar.failed()) {
									LOG.debug(DFAPIMessage.logResponseMessage(1012, "CONNECT_IMPORT"));
								} else {
									LOG.error(
											DFAPIMessage.logResponseMessage(9016, "exception_details - " + ar.cause()));
								}
							});
						} else {
							mongo.findOne(COLLECTION, new JsonObject().put("connectUid", connectName), null,
									findidRes -> {
										if (!findidRes.succeeded()) {
											LOG.error(DFAPIMessage.logResponseMessage(9002,
													"CONNECT_IMPORT - " + findidRes.cause()));
										} else {
											DFJobPOPJ updateJob = new DFJobPOPJ(findidRes.result());
											try {
												updateJob.setStatus(resStatus)
														.setConnectorConfig(new ObjectMapper().readValue(
																resConfig.getObject().toString(),
																new TypeReference<HashMap<String, String>>() {
																}));
											} catch (IOException ioe) {
												LOG.error(DFAPIMessage.logResponseMessage(9017,
														"CONNECT_IMPORT - " + ioe.getCause()));
											}
											mongo.updateCollection(COLLECTION,
													new JsonObject().put("_id", updateJob.getId()),
													new JsonObject().put("$set", updateJob.toJson()), v -> {
														if (!v.failed()) {
															LOG.debug(DFAPIMessage.logResponseMessage(1001,
																	"CONNECT_IMPORT - " + updateJob.getId()));
														} else {
															LOG.error(DFAPIMessage.logResponseMessage(9003,
																	"CONNECT_IMPORT - " + updateJob.getId() + "-"
																			+ v.cause()));
														}
													});
										}
									});
						}
					});
				}
			}
        } catch (UnirestException ue) {
            LOG.error(DFAPIMessage.logResponseMessage(9006, "CONNECT_IMPORT - " + ue.getCause()));
        }
        LOG.info(DFAPIMessage.logResponseMessage(1014, "CONNECT_IMPORT"));
    }

    /**
     * Keep refreshing the active Kafka connectors' status in repository against remote Kafka REST Server.
     */
    private void updateKafkaConnectorStatus() {
        List<String> list = new ArrayList<>();
        // Add all Kafka connect
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*connect.*)"); // case insensitive matching
        list.add(ConstantApp.DF_CONNECT_TYPE.INTERNAL_METADATA_COLLECT.name()); // update metadata sink as well

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (!result.succeeded()) {
				LOG.error(DFAPIMessage.logResponseMessage(9002, result.cause().getMessage()));
			} else {
				for (JsonObject json : result.result()) {
					String connectName = json.getString("connectUid"), statusRepo = json.getString("status"),
							taskId = json.getString("_id");
					wc_refresh
							.get(kafka_connect_rest_port, kafka_connect_rest_host,
									ConstantApp.KAFKA_CONNECT_REST_URL + "/" + connectName + "/status")
							.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
									ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
							.send(ar -> {
								if (!ar.succeeded()) {
									LOG.error(DFAPIMessage.logResponseMessage(9006, ar.cause().getMessage()));
								} else {
									String resStatus = (ar.result().statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND)
											? ConstantApp.DF_STATUS.LOST.name()
											: HelpFunc.getTaskStatusKafka(ar.result().bodyAsJsonObject());
									if (statusRepo.compareToIgnoreCase(resStatus) == 0 || (resStatus
											.equalsIgnoreCase(ConstantApp.DF_STATUS.UNASSIGNED.name())
											&& statusRepo.equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name()))) {
										LOG.debug(DFAPIMessage.logResponseMessage(1020, taskId));
									} else {
										DFJobPOPJ updateJob = new DFJobPOPJ(json);
										updateJob.setStatus(resStatus);
										mongo.updateCollection(COLLECTION, new JsonObject().put("_id", taskId),
												new JsonObject().put("$set", updateJob.toJson()), v -> {
													if (!v.failed()) {
														LOG.info(DFAPIMessage.logResponseMessage(1019, taskId));
													} else {
														LOG.error(DFAPIMessage.logResponseMessage(9003,
																taskId + "cause:" + v.cause()));
													}
												});
									}
								}
							});
				}
			}
        });
    }

    /**
     * Keep refreshing the active Flink transforms/jobs' status in repository against remote Flink REST Server.
     */
    private void updateFlinkJobStatus() {
        List<String> list = new ArrayList<>();
        // Add all transform
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*transform_exchange_flink.*)") ;

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (!result.succeeded()) {
				LOG.error(DFAPIMessage.logResponseMessage(9002, "TRANSFORM_STATUS_REFRESH:" + result.cause()));
			} else {
				for (JsonObject json : result.result()) {
					String statusRepo = json.getString("status"), taskId = json.getString("_id"),
							jobId = ConstantApp.FLINK_DUMMY_JOB_ID;
					DFJobPOPJ updateJob = new DFJobPOPJ(json);
					if (json.getValue("jobConfig") == null
							|| !json.getJsonObject("jobConfig").containsKey(ConstantApp.PK_FLINK_SUBMIT_JOB_ID)) {
						updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name());
						mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
								new JsonObject().put("$set", updateJob.toJson()), v -> {
									if (!v.failed()) {
										LOG.info(DFAPIMessage.logResponseMessage(1022, taskId));
									} else {
										LOG.error(DFAPIMessage.logResponseMessage(9003, taskId + "cause:" + v.cause()));
									}
								});
					} else {
						jobId = json.getJsonObject("jobConfig").getString(ConstantApp.PK_FLINK_SUBMIT_JOB_ID);
						wc_refresh
								.get(flink_rest_server_port, flink_server_host,
										ConstantApp.FLINK_REST_URL + "/" + jobId)
								.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
										ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
								.send(ar -> {
									if (!ar.succeeded()) {
										LOG.info(DFAPIMessage.logResponseMessage(9006,
												"TRANSFORM_STATUS_REFRESH_FOUND " + taskId + " LOST"));
										updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name());
									} else {
										String resStatus = ar.result().statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND
												? ConstantApp.DF_STATUS.LOST.name()
												: HelpFunc.getTaskStatusFlink(ar.result().bodyAsJsonObject());
										if (statusRepo.compareToIgnoreCase(resStatus) == 0) {
											LOG.debug(DFAPIMessage.logResponseMessage(1022, "Flink " + taskId));
										} else {
											updateJob.setStatus(resStatus);
											LOG.info(DFAPIMessage.logResponseMessage(1021, "Flink " + taskId));
										}
									}
									mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
											new JsonObject().put("$set", updateJob.toJson()), v -> {
												if (!v.failed()) {
													LOG.debug(DFAPIMessage.logResponseMessage(1001, taskId));
												} else {
													LOG.error(DFAPIMessage.logResponseMessage(9003,
															taskId + "cause:" + v.cause()));
												}
											});
								});
					}
				}
			}
        });
    }

    /**
     * Keep refreshing the active Spark transforms/jobs' status in repository against remote Livy REST Server
     * We need to find out three type of information
     * * session status
     * * statement status
     * * last query result in rich text format at "livy_statement_output"
     *
     * In addition, this function will trigger batch spark sql result set stream back when it is needed.
     */
    private void updateSparkJobStatus() {
        List<String> list = new ArrayList<>();
        // Add all transform
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*transform_exchange_spark.*)") ;
        HelpFunc.addSpecifiedConnectTypetoList(list, "(?i:.*transform_model_spark.*)") ;

        mongo.find(COLLECTION, new JsonObject().put("connectorType", new JsonObject().put("$in", list)), result -> {
            if (!result.succeeded()) {
				LOG.error(DFAPIMessage.logResponseMessage(9002, "TRANSFORM_STATUS_REFRESH:" + result.cause()));
			} else {
				for (JsonObject json : result.result()) {
					String repoStatus = json.getString("status"), taskId = json.getString("_id");
					DFJobPOPJ updateJob = new DFJobPOPJ(json);
					if (json.getValue("jobConfig") == null
							|| !json.getJsonObject("jobConfig").containsKey(ConstantApp.PK_LIVY_SESSION_ID)
							|| !json.getJsonObject("jobConfig").containsKey(ConstantApp.PK_LIVY_STATEMENT_ID)) {
						updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name());
						mongo.updateCollection(COLLECTION, new JsonObject().put("_id", updateJob.getId()),
								new JsonObject().put("$set", updateJob.toJson()), v -> {
									if (v.failed()) {
										LOG.error(DFAPIMessage.logResponseMessage(9003, taskId + "cause:" + v.cause()));
									} else {
										LOG.debug(DFAPIMessage.logResponseMessage(1001,
												"Missing Livy session/statement with task Id " + taskId));
									}
								});
					} else if (!updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.RUNNING.name())
							&& !updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.STREAMING.name())
							&& !updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.UNASSIGNED.name())
							&& !updateJob.getStatus().equalsIgnoreCase(ConstantApp.DF_STATUS.LOST.name())) {
						LOG.debug(DFAPIMessage.logResponseMessage(1022, taskId));
					} else {
						String sessionId = json.getJsonObject("jobConfig").getString(ConstantApp.PK_LIVY_SESSION_ID),
								statementId = json.getJsonObject("jobConfig")
										.getString(ConstantApp.PK_LIVY_STATEMENT_ID),
								repoFullCode = json.getJsonObject("jobConfig")
										.getString(ConstantApp.PK_LIVY_STATEMENT_CODE);
						wc_refresh
								.get(spark_livy_server_port, spark_livy_server_host,
										ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId + "/state")
								.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
										ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
								.send(sar -> {
									if (!sar.succeeded() || sar.result().statusCode() != ConstantApp.STATUS_CODE_OK) {
										HelpFunc.updateRepoWithLogging(mongo, COLLECTION,
												updateJob.setJobConfig(ConstantApp.PK_LIVY_SESSION_STATE,
														ConstantApp.DF_STATUS.LOST.name()),
												LOG);
										LOG.info(DFAPIMessage.logResponseMessage(9006,
												"TRANSFORM_STATUS_REFRESH_FOUND SPARK " + taskId + " LOST SESSION"));
									} else {
										updateJob.setJobConfig(ConstantApp.PK_LIVY_SESSION_STATE,
												sar.result().bodyAsJsonObject().getString("state"));
										LOG.debug("session info response = " + sar.result().bodyAsJsonObject());
										wc_refresh.get(spark_livy_server_port, spark_livy_server_host,
												ConstantApp.LIVY_REST_URL_SESSIONS + "/" + sessionId
														+ ConstantApp.LIVY_REST_URL_STATEMENTS + "/" + statementId)
												.putHeader(ConstantApp.HTTP_HEADER_CONTENT_TYPE,
														ConstantApp.HTTP_HEADER_APPLICATION_JSON_CHARSET)
												.send(ar -> {
													if (!ar.succeeded()
															|| ar.result().statusCode() != ConstantApp.STATUS_CODE_OK) {
														HelpFunc.updateRepoWithLogging(mongo, COLLECTION,
																updateJob.setStatus(ConstantApp.DF_STATUS.LOST.name()),
																LOG);
														LOG.info(DFAPIMessage.logResponseMessage(9006,
																"TRANSFORM_STATUS_REFRESH_FOUND SPARK " + taskId
																		+ " LOST STATEMENT"));
													} else {
														JsonObject resultJo = ar.result().bodyAsJsonObject();
														LOG.debug("statement info response = " + resultJo);
														String resStatus = ar.result()
																.statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND
																		? ConstantApp.DF_STATUS.LOST.name()
																		: HelpFunc.getTaskStatusSpark(resultJo),
																resFullCode = ar.result()
																		.statusCode() == ConstantApp.STATUS_CODE_NOT_FOUND
																				? ""
																				: resultJo.getString("code");
														LOG.debug("repoStatus = " + repoStatus + " resStatus = "
																+ resStatus);
														LOG.debug("repoFullCode = " + repoFullCode + " resFullCode = "
																+ resFullCode);
														if (repoStatus.compareToIgnoreCase(resStatus) == 0
																|| !repoFullCode.equalsIgnoreCase(resFullCode)) {
															LOG.debug(DFAPIMessage.logResponseMessage(1022, taskId));
														} else {
															LOG.debug("Change Detected");
															updateJob.setStatus(resStatus)
																	.setJobConfig(ConstantApp.PK_LIVY_STATEMENT_STATE,
																			resultJo.getString("state"))
																	.setJobConfig(
																			ConstantApp.PK_LIVY_STATEMENT_PROGRESS,
																			resultJo.getValue("progress").toString())
																	.setJobConfig(ConstantApp.PK_LIVY_STATEMENT_STATUS,
																			resultJo.getJsonObject("output")
																					.getString("status").toUpperCase())
																	.setJobConfig(
																			ConstantApp.PK_LIVY_STATEMENT_TRACEBACK,
																			!resultJo.getJsonObject("output")
																					.containsKey("traceback")
																							? ""
																							: resultJo
																									.getJsonObject(
																											"output")
																									.getJsonArray(
																											"traceback")
																									.toString())
																	.setJobConfig(
																			ConstantApp.PK_LIVY_STATEMENT_EXCEPTION,
																			!resultJo.getJsonObject("output")
																					.containsKey("evalue")
																							? ""
																							: resultJo
																									.getJsonObject(
																											"output")
																									.getString("evalue"))
																	.setJobConfig(ConstantApp.PK_LIVY_STATEMENT_OUTPUT,
																			HelpFunc.livyTableResultToRichText(
																					resultJo));
															if (!resStatus.equalsIgnoreCase(
																	ConstantApp.DF_STATUS.FINISHED.name())
																	|| !"true".equalsIgnoreCase(
																			updateJob.getConnectorConfig(
																					ConstantApp.PK_TRANSFORM_STREAM_BACK_FLAG))) {
																HelpFunc.updateRepoWithLogging(mongo, COLLECTION,
																		updateJob, LOG);
																LOG.debug(
																		"PK_TRANSFORM_STREAM_BACK_FLAG Not Found or equal to FALSE or Master Status Not FINISHED, so update as regular job.");
															} else {
																LOG.debug("Stream Back Enabled");
																ProcessorStreamBack.enableStreamBack(wc_streamback,
																		updateJob, mongo, COLLECTION,
																		schema_registry_rest_port,
																		schema_registry_rest_hostname, df_rest_port,
																		"localhost",
																		HelpFunc.livyTableResultToAvroFields(resultJo,
																				updateJob.getConnectorConfig(
																						ConstantApp.PK_TRANSFORM_STREAM_BACK_TOPIC)));
															}
														}
													}
												});
									}
								});
					}
				}
			}
        });
    }
}
