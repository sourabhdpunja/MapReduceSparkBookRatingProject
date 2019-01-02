package driver;

import accuracycalc.AccuracyCalculator;
import classifier.EnsembleTrain;
import exceptions.BookRatingException;
import org.apache.hadoop.util.Tool;
import prediction.EnsemblePredict;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import preprocess.PreProcessData;
import sampler.RandomSampler;

public class Driver {
    private static final Logger logger = LogManager.getLogger(Driver.class);

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Exactly two arguments required:\n<input-dir> <output-dir> <true or false>");
        }
        try {
            jobRunner(new PreProcessData(), args);
            jobRunner(new RandomSampler(),args);
            jobRunner(new EnsembleTrain(), args);
            jobRunner(new EnsemblePredict(), args);
            jobRunner(new AccuracyCalculator(), args);
        } catch (final Exception e) {
            logger.error("Mission Abort...", e);
        }
    }

    private static void jobRunner(Tool tool, String... args) throws Exception {
        String name = tool.toString();
        name = name.split("@")[0];
        int jobStatus;
        logger.info("Running "+ name +" job.");
        jobStatus = ToolRunner.run(tool, args);
        logger.info(name + " job finished.");
        if(jobStatus==1){
            throw new BookRatingException(name + " job not completed successfully. Aborting mission.");
        }
    }
}
