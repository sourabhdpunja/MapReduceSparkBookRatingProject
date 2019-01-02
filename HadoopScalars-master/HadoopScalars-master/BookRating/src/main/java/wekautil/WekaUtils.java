package wekautil;

import exceptions.BookRatingWekaException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.NumericToNominal;
import weka.filters.unsupervised.attribute.StringToNominal;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class WekaUtils {

    private static final Logger logger = LogManager.getLogger(WekaUtils.class);

    private WekaUtils(){
        throw new IllegalStateException("Utility class cannot be instantiated.");
    }

    /**
     * @param fileNames Names of cache files from which data need to be read.
     * @return object of {@link Instances} if method is able to convert the csv files to weka instances or empty if it
     * is not.
     * @throws IOException Exception during reading csv file.
     */
    public static Optional<Instances> readFromDistributedCache(List<String> fileNames) throws IOException {
        Instances instances = null;
        CSVLoader csvLoader = new CSVLoader();
        for (int i=0; i<fileNames.size(); i++){
            String fileName = fileNames.get(i);
            logger.info("Reading file " + fileName + " from distributed cache to convert to Instances.");
            File file = new File(fileName);
            csvLoader.setSource(file);

            if(i==0){
                instances = csvLoader.getDataSet();
            }
            else {
                instances.addAll(csvLoader.getDataSet());
            }
        }
        instances.setClassIndex(instances.numAttributes()-1);
        String[] options= new String[2];
        options[0]="-R";
        options[1]="8";
        instances = numericToNominal(instances, options);

        options[0]="-R";
        options[1]="first-last";
        instances = stringToNominal(instances, options);
        return Optional.ofNullable(instances);
    }

    /**
     * @param instances Weka data set on which {@link NumericToNominal} filter has to be applied.
     * @param options options for the filter.
     * @return the given instances after applying {@link NumericToNominal} filter.
     */
    private static Instances numericToNominal(Instances instances, String... options){
        NumericToNominal numericToNominal = new NumericToNominal();
        try {
            numericToNominal.setOptions(options);
            numericToNominal.setInputFormat(instances);
            instances  = Filter.useFilter(instances, numericToNominal);
        } catch (Exception e) {
            logger.error(e);
            String message = "Exception occurred while converting numeric class attribute to nominal.";
            throw new BookRatingWekaException(message ,e);
        }
        return instances;
    }

    /**
     * @param instances Weka data set on which {@link StringToNominal} filter has to be applied.
     * @param options options for the filter.
     * @return the given instances after applying {@link StringToNominal} filter.
     */
    private static Instances stringToNominal(Instances instances, String... options){
        StringToNominal stringToNominal = new StringToNominal();
        try {
            stringToNominal.setOptions(options);
            stringToNominal.setInputFormat(instances);
            instances = Filter.useFilter(instances, stringToNominal);
        } catch (Exception e) {
            logger.error(e);
            String message = "Exception occurred while converting string class attribute to nominal.";
            throw new BookRatingWekaException(message ,e);
        }
        return instances;
    }

    public static void deleteCacheFiles(FileSystem fileSystem, Path path) throws IOException {
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    public static List<String> getCacheFileNames(URI[] cacheFiles) {
        List<String> fileNames = new ArrayList<>();
        for (URI cacheFile : cacheFiles){
            String[] paths = cacheFile.getPath().split("/");
            String fileName = paths[paths.length-1];
            fileName = "./"+fileName;
            fileNames.add(fileName);
        }
        return fileNames;
    }
}
