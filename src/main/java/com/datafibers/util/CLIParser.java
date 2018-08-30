package com.datafibers.util;

import com.datafibers.processor.*;
import com.datafibers.service.DFDataProcessor;
import com.datafibers.service.DFInitService;
import com.datafibers.service.DFWebUI;
import org.apache.commons.cli.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CLIParser {
    private static final Logger LOG = Logger.getLogger(CLIParser.class);
    private String[] args;
    private Options options = new Options();
    public String run_mode = "";
    public String service_mode = "";
    public String test_mode = "";
    public String admin_tool = "";
    public String debug_mode = "";


    public CLIParser(String[] args) {
        this.args = args;
        options.addOption("h", "help", false, "show usage help");
        options.addOption("d", "debug", false, "run application in debug level");
        options.addOption("t", "test", true, "run configured test cases, <arg>=test_case_number");
        options.addOption("u", "webui", true, "enable web, <arg>=ui|noui");
        options.addOption("m", "mode", true, "running vertx mode, <arg>=cluster|standalone");
        options.addOption("a", "admin", true,
                "run admin tools, <arg>=Function, such as " +
                        "\n Function: remove_tasks - remove all tasks/processors from repo." +
                        "\n Usage: " +
                        "\n -a remove_tasks" +
                        "\n -a remove_tasks(localhost,27017,db_name,db_collection_name)" +
                        "\n Function: import_df_install (aka. idi) - rebuild df_install configs" +
                        "\n Usage: " +
                        "\n -a import_df_install" +
                        "\n -a import_df_install(localhost,27017,db_name,db_collection_name)"
        );
    }

    public CommandLine parse() {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        if (args == null || args.length == 0) return null;
        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("h"))
                help();

            if (cmd.hasOption("d")) {
                this.debug_mode = "DEBUG";
                LogManager.getLogger(DFInitService.class).setLevel(Level.DEBUG);
                LogManager.getLogger(DFWebUI.class).setLevel(Level.DEBUG);
                LogManager.getLogger(DFDataProcessor.class).setLevel(Level.DEBUG);
                LogManager.getLogger(ProcessorConnectKafka.class).setLevel(Level.DEBUG);
                LogManager.getLogger(ProcessorStreamBack.class).setLevel(Level.DEBUG);
                LogManager.getLogger(ProcessorTopicSchemaRegistry.class).setLevel(Level.DEBUG);
                LogManager.getLogger(ProcessorTransformFlink.class).setLevel(Level.DEBUG);
                LogManager.getLogger(ProcessorTransformSpark.class).setLevel(Level.DEBUG);
            }

            if (cmd.hasOption("m"))
				this.run_mode = "cluster".equalsIgnoreCase(cmd.getOptionValue("m")) ? "Cluster" : "Standalone";

            if (cmd.hasOption("u"))
				this.service_mode = "ui".equalsIgnoreCase(cmd.getOptionValue("u")) ? "WebUI" : "Processor";

            if (cmd.hasOption("t"))
				this.test_mode = "TEST_CASE_"
						+ (!cmd.getOptionValue("t").matches("[-+]?\\d*\\.?\\d+") ? "1" : cmd.getOptionValue("t") + "");

            if (cmd.hasOption("a") && cmd.getOptionValue("a") != null)
				this.admin_tool = "ADMIN_TOOL_" + cmd.getOptionValue("a");

        } catch (ParseException e) {
            LOG.warn(DFAPIMessage.logResponseMessage(9020, "exception - " + e.getCause()));
            help();
        }
        return null;
    }

    public String getRunMode () {
        if (args == null || args.length == 0) {
            LOG.info("Starting both DataFibers Service and Web UI ...");
            return null;
        }

        if(args.length > 0 && args[0].contains("-conf")) // ignore -conf option which is used by vertx config
            return null;

        LOG.info("Starting DataFibers in customized options.");
        LOG.info("run_mode = " + this.run_mode);
        LOG.info("service_mode = " + this.service_mode);
        LOG.info("test_mode = " + this.test_mode);
        LOG.info("admin_tool = " + this.admin_tool);
        return this.run_mode + this.service_mode + this.test_mode + this.admin_tool + this.debug_mode;
    }

    public void help() {
        new HelpFormatter().printHelp("Main", options);
        System.exit(0);
    }
}
