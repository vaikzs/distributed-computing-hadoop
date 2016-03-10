package music.Metadata.Task0.Mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vaikunth Sridharan
 *
 */
public class UniqueTrackMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("<SEP>");
		System.out.println(line[2]);
		Text text_key = new Text(line[2]);
		Text text_value;
		if (line.length == 4)
			text_value = new Text(line[1] + "<SEP>" + line[3] + "<uniqtrac>");
		else
			text_value = new Text("<uniqtrac>");

		context.write(text_key, text_value);
	}
}