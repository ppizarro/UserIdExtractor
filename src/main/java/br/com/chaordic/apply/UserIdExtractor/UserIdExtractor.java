package br.com.chaordic.apply.UserIdExtractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * UserIdExtractor
 * 
 */
public class UserIdExtractor extends Configured implements Tool {

	/*
	static class KeyBasedMultipleTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {

		@Override
		protected String generateFileNameForKeyValue(Text key, Text value, String name) {
			return key.toString() + "/" + name;
		}
	}
    */

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Extecute: UserIdExtractor <input-path> <output-path>");
			return 1;
		}

		Job job = new Job(getConf());
		job.setJobName("UserID Extractor");
		job.setJarByClass(UserIdExtractor.class);

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(UserIdMapper.class);
		job.setMapOutputKeyClass(UserIdTimeKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setGroupingComparatorClass(UserIdGroupComprator.class);
		job.setPartitionerClass(UserIdPartitioner.class);
		job.setReducerClass(UserIdReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setNumReduceTasks(4);

		//job.setOutputFormatClass(TextOutputFormat.class);
		//job.setOutputFormatClass(KeyBasedMultipleTextOutputFormat);
		//MultipleOutputFormat.setOutputPath(job, new Path(args[1]));
		//MultipleOutputs.addMultiNamedOutput(job, "userId", KeyBasedMultipleTextOutputFormat.class, Text.class, Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(final String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new UserIdExtractor(),
				args);
		System.exit(res);
	}
}
