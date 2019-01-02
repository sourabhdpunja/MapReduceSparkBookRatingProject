package accuracycalc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import wekautil.WekaUtils;

public class AccuracyCalculator extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(AccuracyCalculator.class);

    public static class AccuracyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(final Object key, final Text value, final Context context){
            String[] values = value.toString().split(",");
            double actualScore = Double.parseDouble(values[1]);
            double forecastScore = Double.parseDouble(values[2]);
            double mape = calculateMeanAbsPerError(actualScore, forecastScore);
            mape = mape*1000;
            double smape = calculateSymMeanAbsPerError(actualScore, forecastScore);
            smape = smape*1000;
            double rmse = calculateRMSE(actualScore, forecastScore);
            rmse = rmse*1000;
            context.getCounter(ACCURACY_COUNTER.TOTAL_RECORD_COUNT).increment(1);
            context.getCounter(ACCURACY_COUNTER.MAPE).increment((long) mape);
            context.getCounter(ACCURACY_COUNTER.SMAPE).increment((long) smape);
            context.getCounter(ACCURACY_COUNTER.RMSE).increment((long) rmse);
        }

        private double calculateMeanAbsPerError(double actualValue, double forecastValue){
            return Math.abs((actualValue-forecastValue)/actualValue);
        }

        private double calculateSymMeanAbsPerError(double actualValue, double forecastValue){
            return (Math.abs(forecastValue-actualValue))/(Math.abs(actualValue) + Math.abs(forecastValue));
        }

        private double calculateRMSE(double actualValue, double forecastValue){
            return Math.pow((actualValue-forecastValue), 2);
        }
    }

    public enum ACCURACY_COUNTER {
        TOTAL_RECORD_COUNT,
        MAPE,
        SMAPE,
        RMSE
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[1]+"/predicted";
        String output = args[1]+"/accuracy";
        Boolean localRun = Boolean.parseBoolean(args[2]);
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Calculate accuracy");
        job.setJarByClass(AccuracyCalculator.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
        if(localRun){
            final FileSystem fileSystem = FileSystem.get(conf);
            WekaUtils.deleteCacheFiles(fileSystem, new Path(output));
        }
        // ================

        job.setNumReduceTasks(0);
        job.setMapperClass(AccuracyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        int jobStatus = job.waitForCompletion(true) ? 0 : 1;
        // get all the job related counters
        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(ACCURACY_COUNTER.TOTAL_RECORD_COUNT);
        double totalRecords = counter.getValue();
        logger.info("Total number of records predicted: "+totalRecords);

        counter = counters.findCounter(ACCURACY_COUNTER.MAPE);
        double mape = counter.getValue()/1000.0;
        mape = (100/totalRecords)*mape;
        logger.info("Mean absolute percentage error of ensemble is: "+mape);

        counter = counters.findCounter(ACCURACY_COUNTER.SMAPE);
        double smape = counter.getValue()/1000.0;
        smape = (100/totalRecords)*smape;
        logger.info("Symmetric mean absolute percentage error of ensemble is: "+smape);

        counter = counters.findCounter(ACCURACY_COUNTER.RMSE);
        double rmse = counter.getValue()/1000.0;
        rmse = Math.sqrt((rmse/totalRecords));
        logger.info("Root mean square error of ensemble is: "+rmse);
        return jobStatus;
    }
}
