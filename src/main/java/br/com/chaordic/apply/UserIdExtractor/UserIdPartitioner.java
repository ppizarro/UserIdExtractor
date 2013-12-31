package br.com.chaordic.apply.UserIdExtractor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserIdPartitioner extends Partitioner<UserIdTimeKey, Text> {

	@Override
	public int getPartition(UserIdTimeKey key, Text value, int numPartitions) {
		// consider only userId part of key
		return key.userIdHashCode() % numPartitions;
	}
}
