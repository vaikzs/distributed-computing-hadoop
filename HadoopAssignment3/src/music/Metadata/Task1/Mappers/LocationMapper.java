package music.Metadata.Task1.Mappers;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vaikunth Sridharan
 * 
 */

public class LocationMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String key_ = null,value_ = null;
		String regexForLocation = "[0-9]\\.[0-9]+<SEP>[a-zA-Z\\,\\s\\\\t]+";
		Matcher m = Pattern.compile("(?=(" + regexForLocation + "))").matcher(
				line);
		while (m.find()) {
			key_ = m.group(1).split("<SEP>")[1];
			value_ = line.split("<SEP>")[0];
			System.out.println(m.group(1));
			context.write(new Text(key_.trim()), new Text(value_));
		}
			
	}

}
