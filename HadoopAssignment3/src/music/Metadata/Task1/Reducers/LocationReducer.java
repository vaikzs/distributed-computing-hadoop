package music.Metadata.Task1.Reducers;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Vaikunth Sridharan
 * 
 */
public class LocationReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder artists = new StringBuilder();
		for (Text t : values)
		{
			artists.append("<SEP>"+t);
		}
		context.write(key, new Text(artists.toString()));
	}

}