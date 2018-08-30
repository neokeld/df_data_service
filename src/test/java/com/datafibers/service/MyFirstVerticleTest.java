package com.datafibers.service;

import com.datafibers.model.DFJobPOPJ;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * This is our JUnit test for our verticle. The test uses vertx-unit, so we declare a custom runner.
 */
@RunWith(VertxUnitRunner.class)
public class MyFirstVerticleTest {

  private Vertx vertx;
  private Integer port;
  private static MongodProcess MONGO;
  private static int MONGO_PORT = 12345;

  @BeforeClass
  public static void initialize() throws IOException {
    MONGO = MongodStarter.getDefaultInstance().prepare(new MongodConfigBuilder().version(Version.Main.PRODUCTION)
			.net(new Net(MONGO_PORT, Network.localhostIsIPv6())).build()).start();
  }

  @AfterClass
  public static void shutdown() {
    MONGO.stop();
  }

  /**
   * Before executing our test, let's deploy our verticle.
   * <p/>
   * This method instantiates a new Vertx and deploy the verticle. Then, it waits in the verticle has successfully
   * completed its start sequence (thanks to `context.asyncAssertSuccess`).
   *
   * @param c the test context.
   */
  @Before
  public void setUp(TestContext c) throws IOException {
    vertx = Vertx.vertx();

    // Let's configure the verticle to listen on the 'test' port (randomly picked).
    // We create deployment options and set the _configuration_ json object:
    ServerSocket socket = new ServerSocket(0);
    port = socket.getLocalPort();
    socket.close();

    vertx.deployVerticle(
			DFDataProcessor.class.getName(), new DeploymentOptions().setConfig(new JsonObject().put("http.port", port)
					.put("db_name", "whiskies-test").put("connection_string", "mongodb://localhost:" + MONGO_PORT)),
			c.asyncAssertSuccess());
  }

  /**
   * This method, called after our test, just cleanup everything by closing the vert.x instance
   *
   * @param c the test context
   */
  @After
  public void tearDown(TestContext c) {
    vertx.close(c.asyncAssertSuccess());
  }

  /**
   * Let's ensure that our application behaves correctly.
   *
   * @param c the test context
   */
  @Test
  public void testMyApplication(TestContext c) {
    // This test is asynchronous, so get an async handler to inform the test when we are done.
    final Async async = c.async();

    // We create a HTTP client and query our application. When we get the response we check it contains the 'Hello'
    // message. Then, we call the `complete` method on the async handler to declare this async (and here the test) done.
    // Notice that the assertions are made on the 'context' object and are not Junit assert. This ways it manage the
    // async aspect of the test the right way.
    vertx.createHttpClient().getNow(port, "localhost", "/", response -> response.handler(body -> {
		assert body.toString().contains("Hello");
		async.complete();
	}));
  }

  @Test
  public void checkThatTheIndexPageIsServed(TestContext c) {
    Async async = c.async();
    vertx.createHttpClient().getNow(port, "localhost", "/assets/index.html", response -> {
      c.assertEquals(response.statusCode(), 200);
      c.assertEquals(response.headers().get("content-type"), "text/html");
      response.bodyHandler(body -> {
        assert body.toString().contains("<title>My DFJobPOPJ Collection</title>");
        async.complete();
      });
    });
  }

  @Test
  public void checkThatWeCanAdd(TestContext c) {
    Async async = c.async();
    final String json = Json.encodePrettily(new DFJobPOPJ("Jameson", "Ireland","Register"));
    vertx.createHttpClient().post(port, "localhost", "/api/df")
        .putHeader("content-type", "application/json")
        .putHeader("content-length", Integer.toString(json.length()))
        .handler(response -> {
          c.assertEquals(response.statusCode(), 201);
          assert response.headers().get("content-type").contains("application/json");
          response.bodyHandler(body -> {
            final DFJobPOPJ DFJob = Json.decodeValue(body.toString(), DFJobPOPJ.class);
            c.assertEquals(DFJob.getName(), "Jameson");
            c.assertEquals(DFJob.getConnectUid(), "Ireland");
            c.assertNotNull(DFJob.getId());
            async.complete();
          });
        })
        .write(json)
        .end();
  }
}
