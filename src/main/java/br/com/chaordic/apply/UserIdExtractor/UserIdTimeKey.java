package br.com.chaordic.apply.UserIdExtractor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UserIdTimeKey implements WritableComparable<UserIdTimeKey> {
	private Text userId;
	private LongWritable seconds;

	public UserIdTimeKey() {
		userId = new Text();
		seconds = new LongWritable();
	}

	public void set(String userId, long seconds) {
		this.userId.set(userId);
		this.seconds.set(seconds);
	}

	public Text getUserId() {
		return userId;
	}

	public void setUserId(Text userId) {
		this.userId = userId;
	}

	public LongWritable getSeconds() {
		return seconds;
	}

	public void setSeconds(LongWritable seconds) {
		this.seconds = seconds;
	}

	public void readFields(DataInput in) throws IOException {
		userId.readFields(in);
		seconds.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		userId.write(out);
		seconds.write(out);
	}

	public int compareTo(UserIdTimeKey other) {
		int cmp = userId.compareTo(other.getUserId());
		if (0 == cmp) {
			cmp = seconds.compareTo(other.getSeconds());
		}
		return cmp;
	}

	public int userIdCompareTo(UserIdTimeKey other) {
		int cmp = userId.compareTo(other.getUserId());
		return cmp;
	}

	public int userIdHashCode() {
		return Math.abs(userId.hashCode());
	}

	public int hashCode() {
		return userId.hashCode() * 256 + seconds.hashCode();
	}

	public boolean equals(Object obj) {
		boolean isEqual = false;
		if (obj instanceof UserIdTimeKey) {
			UserIdTimeKey other = (UserIdTimeKey) obj;
			isEqual = userId.equals(other.userId)
					&& seconds.equals(other.seconds);
		}

		return isEqual;
	}

	public String toString() {
		return userId.toString() + ":" + seconds.get();
	}
}
