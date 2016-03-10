package music.Metadata.Task0.Reducers;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Vaikunth Sridharan
 * 
 */
public class MusicArtistReducer extends Reducer<Text, Text, Text, Text> {
	static StringBuilder line;
	static StringBuilder artist, artistloc, songt;

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		artist = new StringBuilder();
		songt = new StringBuilder();
		artistloc = new StringBuilder();
		int tempLoc = 0, tempArt = 0, tempTrac = 0;
		for (Text value : values) {
			line = new StringBuilder();
			line.append(value.toString());

			if (line.substring(line.length() - 10, line.length()).equals(
					"<uniqarts>")) {
				if (line.substring(0, line.length() - 10).isEmpty())
					artist.append("<SEP><SEP>");
				else {
					if (tempArt == 0)
						artist.append(line.substring(0, line.length() - 10)
								+ "<SEP>");
					else {
						if (artist.toString().endsWith("<SEP>"))
							{
							artist.replace(artist.length() - 5,
									artist.length(), "");
						artist.append("<I>"
								+ line.substring(0, line.length() - 10)+"<SEP>");
							}
						
					}
				}
				tempLoc = 0;
				tempTrac = 0;
				tempArt = 1;
			}
			if (line.substring(line.length() - 10, line.length()).equals(
					"<uniqtrac>")) {
				if (line.substring(0, line.length() - 10).isEmpty())
					songt.append("<SEP><SEP>");
				else {
					if(tempTrac == 0){
				
						songt.append(line.substring(0, line.length() - 10)
							+ "<SEP>");
											}
					else{
						if (songt.toString().endsWith("<SEP>"))
							{
							songt.replace(songt.length() - 5,
									songt.length(), "");
						songt.append("<I>"
								+ line.substring(0, line.length() - 10)+"<SEP>");
							}
						
					}
				}
				tempLoc = 0;
				tempArt = 0;
				tempTrac = 1;
			}
			if (line.substring(line.length() - 10, line.length()).equals(
					"<artslocs>")) {

				if (line.substring(0, line.length() - 10).isEmpty())
					artistloc.append("<SEP><SEP><SEP><SEP>");
				else {
					if (tempLoc == 0)
					{	
						artistloc.append(line.substring(0, line.length() - 10)
								+ "<SEP>");
					}
					else {
						if (artistloc.toString().endsWith("<SEP>"))
							{
							artistloc.replace(artistloc.length() - 5,
									artistloc.length(), "");
						artistloc.append("<I>"
								+ line.substring(0, line.length() - 10)+"<SEP>");
							}
						
					}
				}
				tempLoc = 1;
				tempArt = 0;
				tempTrac = 0;
			}
		}
		
		int songEmp = 0;
		if (artist.toString().isEmpty()) {
			// if artistID not found append <SEP>
			artist.append("<SEP><SEP><SEP>");
			
		}
		if (songt.toString().isEmpty()) {
			// if song title not found append <SEP>
			songt.append("<SEP><SEP>");
		}
			if (artistloc.toString().isEmpty()) {
		
			// if song title not found append <SEP>
			artistloc.append("<SEP><SEP><SEP><SEP>");
			
		}
		
			songt.replace(songt.length()-5, songt.length(), "");
		
		
		String output  = key.toString() + "<SEP>"+artist.toString() 
				+ artistloc.toString() + songt.toString();
		if(!artist.toString().endsWith("<SEP>")){
		output = key.toString() + "<SEP>"+artist.toString() + "<SEP>"
				+ artistloc.toString() + songt.toString();
		}else if(!artistloc.toString().endsWith("<SEP>")){
			output = key.toString() + "<SEP>"+artist.toString() 
					+ artistloc.toString()+"<SEP>" + songt.toString();
						
		}else if(!artistloc.toString().endsWith("<SEP>")&&!artist.toString().endsWith("<SEP>")){
			output = key.toString() + "<SEP>"+artist.toString() + "<SEP>"
					+ artistloc.toString()+"<SEP>" + songt.toString();
		}
		context.write(new Text(output), new Text(""));

	}

}