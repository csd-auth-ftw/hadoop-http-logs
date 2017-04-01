package csd.auth.ftw;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HttpErrorMapper extends Mapper<Object, Text, IntWritable, Text> {
	private static final String REGEX_STR = ".* - - \\[(.*)\\] .*\"(?:GET|POST|PUT|PATCH|DELETE) (\\/.*) HTTP\\/\\d\\.\\d\" ([0-9]{3}) .*";
	
	private int statusCode;
	private String url;
	private String timestamp;
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		if (parseRequestLine(value.toString())) {
			// skip line if it's not an error
			if (statusCode < 400)
				return;
			
			// write error
			IntWritable keyStatusCode = new IntWritable(statusCode);
			if (context.getJobName().equals(HttpLogAnalyser.URL_JOB_NAME)) {
				Text valueUrl = new Text(url);
				context.write(keyStatusCode, valueUrl);
			} else if (context.getJobName().equals(HttpLogAnalyser.TIMESTAMP_JOB_NAME)) {
				Text valueTimestamp = new Text(timestamp);
				context.write(keyStatusCode, valueTimestamp);
			}
		}
	}

	private boolean parseRequestLine(String line) {
		Pattern pattern = Pattern.compile(REGEX_STR);
		Matcher matcher = pattern.matcher(line);
		
		if (!matcher.find())
			return false;
		
		statusCode = Integer.parseInt(matcher.group(3));
		url = matcher.group(2).trim();
		timestamp = matcher.group(1).trim();
		
		return true;
	}
}
