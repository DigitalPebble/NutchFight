package com.digitalpebble.commoncrawl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLDeduplicator extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory
			.getLogger(URLDeduplicator.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new URLDeduplicator(),
				args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//
		Job job = new Job(conf);
		job.setJarByClass(URLDeduplicator.class);

		job.setJobName("URLDeduplicator");

		// inputPath =
		// "s3n://aws-publicdatasets/common-crawl/crawl-data//common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wat/CC-MAIN-20131204131715-00099-ip-10-33-133-15.ec2.internal.warc.wat.gz";

		if (args.length != 2) {
			System.err
					.println("com.digitalpebble.commoncrawl.URLDeduplicator inPath outPath");
			return -1;
		}

		String inputPath = args[0];

		LOG.info("Input path: " + inputPath);
		FileInputFormat.addInputPath(job, new Path(inputPath));

		String outputPath = args[1];

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}

		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setMapperClass(UrlDeduplicatorMapper.class);
		job.setReducerClass(UrlDeduplicatorReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// no reducers needed
		// job.setNumReduceTasks(0);

		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}

	static class UrlDeduplicatorReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		protected void reduce(Text key, Iterable<NullWritable> value,
				Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}

	}

	static class UrlDeduplicatorMapper extends
			Mapper<org.apache.hadoop.io.LongWritable, Text, Text, NullWritable> {

		public void map(org.apache.hadoop.io.LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}

}