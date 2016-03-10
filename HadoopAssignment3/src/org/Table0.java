package org;

import music.Metadata.Task0.Reducers.MusicArtistReducer;
import music.Metadata.Task0.Mappers.ArtistLocationMapper;
import music.Metadata.Task0.Mappers.UniqueArtistMapper;
import music.Metadata.Task0.Mappers.UniqueTrackMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Vaikunth Sridharan
 * 
 */
@SuppressWarnings("deprecation")
public class Table0 extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "MergingTask");
		
		job.setJarByClass(Table0.class);
		if (args.length == 2) {
			MultipleInputs.addInputPath(job, new Path(args[0]
					+ "/unique_artists.txt"), TextInputFormat.class,
					UniqueArtistMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[0]
					+ "/unique_tracks.txt"), TextInputFormat.class,
					UniqueTrackMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[0]
					+ "/artist_location.txt"), TextInputFormat.class,
					ArtistLocationMapper.class);
			
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
		} else {
			MultipleInputs.addInputPath(job, new Path(
					"/input0/unique_artists.txt"), TextInputFormat.class,
					UniqueArtistMapper.class);
			MultipleInputs.addInputPath(job, new Path(
					"/input0/unique_tracks.txt"), TextInputFormat.class,
					UniqueTrackMapper.class);
			MultipleInputs.addInputPath(job, new Path(
					"/input0/artist_location.txt"), TextInputFormat.class,
					ArtistLocationMapper.class);
			FileOutputFormat.setOutputPath(job, new Path("/output0"));
		}
		job.setReducerClass(MusicArtistReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return (job.waitForCompletion(true) ? 0 : 1);

	}

	public static void main(String[] args) throws Exception {

		int ecode = ToolRunner.run(new Table0(), args);
		System.exit(ecode);

	}

}