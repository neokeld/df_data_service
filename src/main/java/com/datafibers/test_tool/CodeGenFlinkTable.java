package com.datafibers.test_tool;
import net.openhft.compiler.CompilerUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;

import com.datafibers.util.DynamicRunner;

/**
 * The goal is to define a function with parameter TABLE, Transformation Script and return TABLE
 * This function will dynamically generate code and run.
 */
public class CodeGenFlinkTable {

	public static void main(String args[]) {

		String transform2 = "select(\"name\");\n",
				header = "package dynamic;\nimport org.apache.flink.api.table.Table;\nimport com.datafibers.util.*;\n",
				javaCode2 = header + "public class FlinkScript implements DynamicRunner {\n@Override \n"
						+ "    public Table transTableObj(Table tbl) {\ntry {return tbl." + transform2
						+ "} catch (Exception e) {};return null;}}";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		tableEnv.registerTableSource("mycsv", new CsvTableSource("/Users/will/Downloads/file.csv", new String[] { "name", "id", "score", "comments" },
				new TypeInformation<?>[] { Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING() }));
		TableSink sink = new CsvTableSink("/Users/will/Downloads/out.csv", "|");
		Table ingest = tableEnv.scan("mycsv");

		try {
			// write the result Table to the TableSink
			((DynamicRunner) CompilerUtils.CACHED_COMPILER.loadFromJava("dynamic.FlinkScript", javaCode2).newInstance())
					.transTableObj(ingest).writeToSink(sink);
			env.execute();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}