// Ref: https://www.youtube.com/watch?v=fh4ouoKs8H0
package prediction;

import customwritable.ClassifierWritable;
import exceptions.BookRatingWekaException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import weka.core.Instance;
import weka.core.Instances;
import wekautil.WekaUtils;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class EnsemblePredict extends Configured implements  Tool{
    private static final Logger logger = LogManager.getLogger(EnsemblePredict.class);

    public static class PredictorMapper extends Mapper<Object, ClassifierWritable, Text, DoubleWritable> {

        Instances testDataSet;

        @Override
        public void setup(Context context) throws IOException {
            List<String> fileNames = WekaUtils.getCacheFileNames(context.getCacheFiles());
            WekaUtils.readFromDistributedCache(fileNames).ifPresent(instances -> testDataSet =instances);
        }

        @Override
        public void map(Object key, ClassifierWritable value, Context context){
            for(Instance instance : testDataSet){
                double actualClass = instance.classValue();
                String actual = testDataSet.classAttribute().value((int) actualClass);
                String isbnString = instance.stringValue(0) + "," + actual;
                Text isbn = new Text(isbnString);

                try {
                    double predictedClass = value.getClassifier().classifyInstance(instance);
                    String predicted = testDataSet.classAttribute().value((int) predictedClass);
                    DoubleWritable predictedVal = new DoubleWritable(Double.parseDouble(predicted));
                    context.write(isbn, predictedVal);
                } catch (Exception e) {
                    logger.error(e);
                    throw new BookRatingWekaException("Exception occurred while predicting value.", e);
                }
            }
        }
    }

    public static class PredictorReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

        @Override
        public void reduce(final Text key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;

            for(DoubleWritable value : values){
                sum = sum + value.get();
                count++;
            }
            Double predicted = 11.0;
            if(count!=0){
                predicted = sum/count;
            }
            DoubleWritable finalPredictedScore = new DoubleWritable(predicted);
            context.write(key, finalPredictedScore);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String output = args[1];
        Boolean localRun = Boolean.parseBoolean(args[2]);
        String trainedModels = output+"/ensemblemodel";
        String testData = output+"/sampler";
        String predicted = output+"/predicted";
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Prediction");
        job.setJarByClass(EnsemblePredict.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
        if(localRun){
            final FileSystem fileSystem = FileSystem.get(conf);
            WekaUtils.deleteCacheFiles(fileSystem, new Path(predicted));
        }
        // ================

        job.setMapperClass(PredictorMapper.class);
        job.setReducerClass(PredictorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(trainedModels));
        FileOutputFormat.setOutputPath(job, new Path(predicted));

        Path testDataPath = new Path(testData);
        FileSystem fs = FileSystem.get(testDataPath.toUri(), conf);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(testDataPath, false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            URI uri = locatedFileStatusRemoteIterator.next().getPath().toUri();
            String path = uri.getPath();
            if(path.contains("TEST")){
                logger.info("Adding file " + path + " to distributed cache");
                job.addCacheFile(uri);
            }
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
