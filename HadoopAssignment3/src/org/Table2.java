package org;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Vaikunth Sridharan
 * 
 */
public class Table2 {
	public static HashMap<String, HashMap<Integer, Text>> ls = new HashMap<String, HashMap<Integer, Text>>();
	public static TreeMap<Integer, Integer> noArtists = new TreeMap<Integer, Integer>();
	public static HashMap<Text, Text> hashMapOfTask_0_Output = new HashMap<Text, Text>();
	private static String command_line;

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static HashMap<Integer, Text> hashMapOfArtists = new HashMap<Integer, Text>();
		private final static HashMap<Integer, Text> hashMapOfArtist = new HashMap<Integer, Text>();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			InputSplit fileSplit = context.getInputSplit();
			String filename = fileSplit.toString();

			if (filename.contains(command_line)) {
				System.out.println("FileName------------------------------> "
						+ filename);

				String[] artistName = value.toString().split("<SEP>");

				// System.out.println(artistName[0]+artistName[1]);

				if (artistName.length > 2) {

					for (int i = 1; i < artistName.length; i++) {
						context.write(new Text(artistName[0]), one);
						hashMapOfArtists.put(i - 1, new Text(artistName[i]));
					}

					ls.put(artistName[0], hashMapOfArtists);
				} else {
					context.write(new Text(artistName[0]), one);
					hashMapOfArtist.put(0, new Text(artistName[1]));
					ls.put(artistName[0], hashMapOfArtist);
				}
			} else {
				String[] line = value.toString().split("<SEP>");
				System.out.println(value.toString());
				hashMapOfTask_0_Output.put(new Text(line[0]),
						new Text(value.toString()));

			}

		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, Text> {
		private IntWritable result = new IntWritable();

		public List<Integer> sortDescending(List<Integer> arr) {
			Comparator<Integer> c = Collections.reverseOrder();
			Collections.sort(arr, c);
			return arr;
		}

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			for (int i = 0; i <= noArtists.size(); i++)
				noArtists.put(i, Integer.parseInt(result.toString()));

			Boolean flag = false;
			if (ls.containsKey(key.toString())) {

				// for (int j = 0; j < 5; j++) {
				// System.out.println(noArtists);
				int l = 1;
				if (l <= 5) {
					for (int k = noArtists.size() - 1; k >= 0; k--) {

						if (noArtists.get(k) == ls.get(key.toString()).size()) {
							for (Text e : hashMapOfTask_0_Output.keySet()) {
								for (Object a : ls.get(key.toString()).values()
										.toArray()) {
									if (a.equals(e)) {
										context.write(new Text(
												hashMapOfTask_0_Output.get(e)),
												new Text(""));
									}

								}

							}

							// }
						}
						l++;
					}
				}
			}

			System.out.println(key.toString() + result);

		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length > 0)
			command_line = args[0];

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(Table2.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		if (args.length < 2) {
			MultipleInputs.addInputPath(job, new Path(
					"output1/part-r-00000"), TextInputFormat.class,
					TokenizerMapper.class);
			MultipleInputs.addInputPath(job, new Path(
					"output0/part-r-00000"), TextInputFormat.class,
					TokenizerMapper.class);
			FileOutputFormat.setOutputPath(job, new Path("output2"));
		} else {
			MultipleInputs.addInputPath(job, new Path(args[0]),
					TextInputFormat.class, TokenizerMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]),
					TextInputFormat.class, TokenizerMapper.class);
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}