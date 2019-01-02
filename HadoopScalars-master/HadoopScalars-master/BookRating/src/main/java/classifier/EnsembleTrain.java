package classifier;

import customwritable.ClassifierWritable;
import exceptions.BookRatingWekaException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.lazy.IBk;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.filters.Filter;
import weka.filters.supervised.instance.Resample;
import wekautil.WekaUtils;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class EnsembleTrain extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(EnsembleTrain.class);

    public static class ClassifierMapper extends Mapper<Object, Text, NullWritable, ClassifierWritable> {

        private Instances arffDataSet;

        @Override
        public void setup(Context context) throws IOException {
            List<String> fileNames = WekaUtils.getCacheFileNames(context.getCacheFiles());
            WekaUtils.readFromDistributedCache(fileNames).ifPresent(instances -> arffDataSet=instances);
        }

        @Override
        public void map(final Object key, Text value, final Context context) throws IOException, InterruptedException {
            ClassifierWritable classifierWritable = trainModel(value.toString());
            context.write(NullWritable.get(), classifierWritable);
        }

        /**
         * Train the classifier on the data set.
         * @param value have all the parameters such as classifier name etc. necessary for initializing the classifier.
         * @return the trained classifier wrapped in a ClassifierWritable object.
         */
        private ClassifierWritable trainModel(String value){
            String[] values = value.split(":");
            String classifierName = values[1];
            ClassifierWritable classifierWritable;
            //Get bagged data set
            Instances baggedDataSet = getBaggedDataSet(values);

            classifierWritable = getClassifier(values);

            try {
                logger.info("Training classifier "+ classifierName);
                long startTime = System.nanoTime();
                classifierWritable.getClassifier().buildClassifier(baggedDataSet);
                long endTime = System.nanoTime();
                double duration = (endTime - startTime)/60000000000.0;
                logger.info("Total time taken for training classifier " + classifierName + " is: " + duration + " minutes.");
            } catch (Exception e) {
                logger.error(e);
                String message = "Exception occurred while training the classifier" + classifierName;
                throw new BookRatingWekaException(message ,e);
            }
            return classifierWritable;
        }

        /**
         * Method for returning bagged data set.
         * @param values have all the parameters such as sampler percentage.
         * @return Object of {@link Instances} having bagged data set.
         */
        private Instances getBaggedDataSet(String... values){
            Instances baggedDataSet;
            Resample resample = new Resample();
            double sampleSizePercentage = Double.parseDouble(values[0]);
            resample.setSampleSizePercent(sampleSizePercentage);
            try {
                resample.setInputFormat(arffDataSet);
                baggedDataSet = Filter.useFilter(arffDataSet, resample);
                baggedDataSet.setClassIndex(baggedDataSet.numAttributes() - 1);
            } catch (Exception e) {
                logger.error(e);
                String message = "Exception occurred while creating bagging sample of data set.";
                throw new BookRatingWekaException(message, e);
            }
            return baggedDataSet;
        }

        /**
         * Method to get the classifier for fitting data set. Returns classifier based on parameters specified in
         * values parameter.
         * @param values have all the parameters such as classifier name etc. necessary for initializing the classifier.
         * @return Object of {@link ClassifierWritable} which is a wrapper around the {@link Classifier} object.
         */
        private ClassifierWritable getClassifier(String... values){
            ClassifierWritable classifierWritable = new ClassifierWritable();
            String classifierName = values[1];
            switch (classifierName) {
                case "J48":
                    Classifier j48 = getJ48Classifier(values);
                    classifierWritable.setClassifier(j48);
                    break;
                case "NaiveBayes":
                    NaiveBayes naiveBayes = new NaiveBayes();
                    classifierWritable.setClassifier(naiveBayes);
                    break;
                case "DecisionStump":
                    DecisionStump decisionStump = new DecisionStump();
                    classifierWritable.setClassifier(decisionStump);
                    break;
                case "IBk":
                    Classifier iBk = getIBkClassifier(values);
                    classifierWritable.setClassifier(iBk);
                    break;
                default:
                    String message = "Classifier " + classifierName + "is not supported";
                    logger.error(message);
                    throw new BookRatingWekaException(message);
            }
            return classifierWritable;
        }

        /**
         * Return {@link J48} classifier.
         * @param values have all the parameters such as classifier name etc. necessary for initializing the classifier.
         * @return Object of {@link Classifier}.
         */
        private Classifier getJ48Classifier(String... values){
            J48 j48 = new J48();
            String unPruned = values[2];
            if (unPruned.equals("-U") || unPruned.equals("-u")){
                j48.setUnpruned(true);
            }
            return j48;
        }

        /**
         * Return {@link IBk} classifier.
         * @param values have all the parameters such as classifier name etc. necessary for initializing the classifier.
         * @return Object of {@link Classifier}.
         */
        private Classifier getIBkClassifier(String... values){
            IBk iBk = new IBk();
            int k = Integer.parseInt(values[2]);
            iBk.setKNN(k);
            return iBk;
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        Boolean localRun = Boolean.parseBoolean(args[2]);
        String modelParameters = input+"/modelparameters";
        String trainingData = output+"/sampler";
        String ensemblemodeloutput = output+"/ensemblemodel";
        final Configuration conf = getConf();
        conf.set("mapreduce.task.timeout","3000000");
        conf.set("mapreduce.map.java.opts","-Xmx4096m");
        conf.set("mapreduce.map.memory.mb","4096");
        final Job job = Job.getInstance(conf, "Train Ensemble Model");
        job.setJarByClass(EnsembleTrain.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
        // Delete output directory, only to ease local development; will not work on AWS. ===========
        if(localRun){
            final FileSystem fileSystem = FileSystem.get(conf);
            WekaUtils.deleteCacheFiles(fileSystem, new Path(ensemblemodeloutput));
        }
        // ================
        job.setNumReduceTasks(0);

        job.setMapperClass(ClassifierMapper.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(ClassifierWritable.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);


        FileInputFormat.addInputPath(job, new Path(modelParameters));
        FileOutputFormat.setOutputPath(job, new Path(ensemblemodeloutput));

        Path trainingDataPath = new Path(trainingData);
        FileSystem fs = FileSystem.get(trainingDataPath.toUri(), conf);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(trainingDataPath, false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            URI uri = locatedFileStatusRemoteIterator.next().getPath().toUri();
            String path = uri.getPath();
            if(path.contains("TRAIN")){
                logger.info("Adding file " + path + " to distributed cache");
                job.addCacheFile(uri);
            }
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
