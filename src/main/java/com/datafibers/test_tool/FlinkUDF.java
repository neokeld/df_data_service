package com.datafibers.test_tool;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkUDF {
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 0x716BD14718AD2E0L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			for (String token : value.toLowerCase().split("\\W+")) {
				if (!token.isEmpty()) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
}
