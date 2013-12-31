package br.com.chaordic.apply.UserIdExtractor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserIdGroupComprator extends WritableComparator {

	protected UserIdGroupComprator() {
		super(UserIdTimeKey.class, true);
	}

	@Override
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2) {
		UserIdTimeKey key1 = (UserIdTimeKey)w1;
		UserIdTimeKey key2 = (UserIdTimeKey)w2;
		
		//consider only the userId part of the key
		return key1.userIdCompareTo(key2);
	}
}
