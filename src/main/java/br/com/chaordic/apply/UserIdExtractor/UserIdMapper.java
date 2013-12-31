package br.com.chaordic.apply.UserIdExtractor;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserIdMapper extends
		Mapper<LongWritable, Text, UserIdTimeKey, Text> {

	private Date date;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
	private Long timeStamp;
	private String userID;
	private UserIdTimeKey outKey = new UserIdTimeKey();

	/*
	 * Parsear o arquivo não é o ponto do exercício, assumido todas as simplificações
	 */
	private void parseLogLine(String line) throws ParseException {
    	int beginIndex = line.indexOf('[');
    	int endIndex = line.indexOf(']', beginIndex + 1);
    	if (beginIndex < 0 || endIndex < 0)
			throw new ParseException("Failed to parse timestamp", 0);
    	
		date = dateFormat.parse(line.substring(beginIndex + 1, endIndex));
		timeStamp = date.getTime();
        
        beginIndex = line.indexOf("\"userid=", endIndex);
    	endIndex = line.indexOf('"', beginIndex + 8);
    	if (beginIndex < 0  || endIndex < 0)
			throw new ParseException("Failed to parse userId", 0);
    	
    	userID = line.substring(beginIndex + 8, endIndex);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		try {
			parseLogLine(value.toString());
			outKey.set(userID, timeStamp);
			context.write(outKey, value);
		} catch (ParseException e) {
			throw new IOException("Failed to parse timestamp", e);
		}
	}

}
