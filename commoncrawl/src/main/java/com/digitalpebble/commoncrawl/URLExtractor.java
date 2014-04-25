package com.digitalpebble.commoncrawl;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simply reads the URLS from the common crawl dataset and stores them in a
 * simple file
 **/

public class URLExtractor extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory
			.getLogger(URLExtractor.class);

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new URLExtractor(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		//
		Job job = new Job(conf);
		job.setJarByClass(URLExtractor.class);

		job.setJobName("URLExtractor");

		// inputPath =
		// "s3n://aws-publicdatasets/common-crawl/crawl-data//common-crawl/crawl-data/CC-MAIN-2013-48/segments/1386163035819/wat/CC-MAIN-20131204131715-00099-ip-10-33-133-15.ec2.internal.warc.wat.gz";

		if (args.length != 2) {
			System.err
					.println("com.digitalpebble.commoncrawl.URLExtractor inPath outPath");
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

		job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setMapperClass(UrlExtractorMapper.class);

		// no reducers needed
		job.setNumReduceTasks(0);

		if (job.waitForCompletion(true)) {
			return 0;
		} else {
			return 1;
		}
	}

	static class UrlExtractorMapper extends
			Mapper<Text, ArchiveReader, Text, NullWritable> {
		private Text outKey = new Text();

		protected static enum MAPPERCOUNTER {
			RECORDS_IN, NO_SERVER, EXCEPTIONS
		}

		@Override
		public void map(Text key, ArchiveReader value, Context context)
				throws IOException {
			for (ArchiveRecord r : value) {
				String sourceURL = r.getHeader().getUrl();
				if (StringUtils.isBlank(sourceURL))
					continue;
				outKey.set(sourceURL);
				try {
					context.write(outKey, NullWritable.get());
				} catch (Exception ex) {
					LOG.error("Caught Exception", ex);
					context.getCounter(MAPPERCOUNTER.EXCEPTIONS).increment(1);
				} finally {
					IOUtils.closeQuietly(r);
				}
			}
		}
	}

}