package br.com.chaordic.apply.UserIdExtractor;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class UserIdReducer extends Reducer<UserIdTimeKey, Text, Text, Text> {

	private Text outKey = new Text();
	private MultipleOutputs<Text, Text> multipleOutputs;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void reduce(UserIdTimeKey key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		outKey.set(key.getUserId());
		for (Text value : values) {
			//context.write(outKey, value);
			multipleOutputs.write(outKey, value, key.getUserId().toString());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}
}
