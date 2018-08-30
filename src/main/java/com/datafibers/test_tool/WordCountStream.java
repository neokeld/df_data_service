package com.datafibers.test_tool;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;

/**
 * The goal is to define a function with parameter TABLE, Transformation Script and return TABLE
 * This function will dynamically generate code and run.
 */
public class WordCountStream {


	public static void main(String args[]) {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

		// Create a DataStream from a list of elements
		//DataStream<Integer> ds = env.fromElements(1, 2, 3, 4, 5);

		tableEnv.registerTableSource("mycsv", new CsvTableSource("/Users/will/Downloads/file.csv", new String[] { "name", "id", "score", "comments" },
				new TypeInformation<?>[] { Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING() }));



		tableEnv.scan("mycsv").select("name").writeToSink(new CsvTableSink("/Users/will/Downloads/out.csv", "|"));
		try {
			env.execute();
		} catch (Exception e) {

		}

		System.out.print("DONE");
	}
}