package org;

import music.Metadata.Task1.Mappers.LocationMapper;
import music.Metadata.Task1.Reducers.LocationReducer;

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

@SuppressWarnings("deprecation")
public class Table1 extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		
		
		Job job = new Job(conf, "LocationInformation");
		job.setJarByClass(Table1.class);
		if(args.length!=2){
		MultipleInputs.addInputPath(job, new Path(
				"/input1"), TextInputFormat.class,
				LocationMapper.class);
		FileOutputFormat.setOutputPath(job, new Path("/output1"));
		}
		else{
			MultipleInputs.addInputPath(job, new Path(
					args[0]), TextInputFormat.class,
					LocationMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			
	}
		
		job.setReducerClass(LocationReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	
	
	public static void main(String[] args) throws Exception{
		
		int ecode = ToolRunner.run(new Table1(), args);
		System.exit(ecode);
		
		
		
	}
}
