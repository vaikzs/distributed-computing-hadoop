package music.Metadata.Task0.Mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Vaikunth Sridharan
 *
 */
public class UniqueArtistMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] line = value.toString().split("<SEP>");

		context.write(new Text(line[3]), new Text(line[0] + "<SEP>" + line[1]
				+ "<SEP>" + line[2] + "<uniqarts>"));

	}
}