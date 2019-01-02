package sampler;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import wekautil.WekaUtils;

import java.io.IOException;
import java.util.Random;

public class RandomSampler extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(RandomSampler.class);
	private static final String TRAIN = "TRAIN";
	private static final String TEST = "TEST";

	public static class RandomSamplerMapper extends Mapper<Object, Text, NullWritable, Text> {

		private Random random = new Random();
		private Double percentage;
		private final Text emitKey = new Text();
		private MultipleOutputs<Text, Text> multipleOutputs;

		@Override
		protected void setup(Context context){
			String filterPercentage = context.getConfiguration().get("filterPercentage");
			percentage = Double.parseDouble(filterPercentage) / 100.0;
			multipleOutputs = new MultipleOutputs(context);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			final String isbn = value.toString().split(",")[0];
			emitKey.set(isbn);
			if (random.nextDouble() < percentage) {
				multipleOutputs.write(TRAIN, NullWritable.get(), value);
			} else {
				multipleOutputs.write(TEST, NullWritable.get(), value);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
	    String output = args[1];
	    String samplerInput = output+"/preprocess";
	    String samplerOutput = output+"/sampler";
		Boolean localRun = Boolean.parseBoolean(args[2]);
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Random Sampler");
		job.setJarByClass(RandomSampler.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");

		// Delete output directory, only to ease local development; will not work on AWS. ===========
        if(localRun){
            final FileSystem fileSystem = FileSystem.get(conf);
            WekaUtils.deleteCacheFiles(fileSystem, new Path(samplerOutput));
        }
		// ================

        job.setNumReduceTasks(0);
		// Filter percentage is 90 percent
		jobConf.set("filterPercentage", "90");

		job.setMapperClass(RandomSamplerMapper.class);
		MultipleOutputs.addNamedOutput(job, TRAIN, TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, TEST, TextOutputFormat.class, NullWritable.class, Text.class);
		FileInputFormat.addInputPath(job, new Path(samplerInput));
		FileOutputFormat.setOutputPath(job, new Path(samplerOutput));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}