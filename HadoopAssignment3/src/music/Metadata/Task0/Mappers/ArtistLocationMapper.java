package music.Metadata.Task0.Mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vaikunth Sridharan
 *
 */
public class ArtistLocationMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] line = value.toString().split("<SEP>");
		Text text_key = new Text(line[3]);
		Text text_value;
		if (line.length == 5)
			text_value = new Text(line[0] + "<SEP>" + line[1] + "<SEP>"
					+ line[2] + "<SEP>" + line[4] + "<artslocs>");
		else
			text_value = new Text("<artslocs>");
		context.write(text_key, text_value);

	}
}