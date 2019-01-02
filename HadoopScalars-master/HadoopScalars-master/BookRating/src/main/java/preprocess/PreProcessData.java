package preprocess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import customwritable.*;
import exceptions.BookRatingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import wekautil.WekaUtils;

public class PreProcessData extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(PreProcessData.class);

    public static class BookMapper extends Mapper<Object, Text, Text, MyGenericWritable> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final BookWritable bookWritable = bookFilter(value.toString());
            context.write(bookWritable.getIsbn(), new MyGenericWritable(bookWritable));
        }

        /**
         * Return {@link BookWritable} object after filtering the data. Year less then 0 and greater then 2006
         * are replaced with mean year value.
         * @param bookDetails String having all relevant book details.
         * @return {@link BookWritable} object.
         */
        private BookWritable bookFilter(String bookDetails){
            String[] bookDetailsArray = bookDetails.replace(",", "").replace("\";\"", ",")
                    .replace("\"", "").replace("\'", "")
                    .replace("&amp;", "and").split(",");
            Text isbn = new Text(bookDetailsArray[0]);
            Text title = new Text(bookDetailsArray[1]);
            Text author = new Text(bookDetailsArray[2]);
            int publishYear = Integer.parseInt(bookDetailsArray[3]);
            // Replace invalid publication year with mean year
            if(publishYear<=0 || publishYear>2006){
                publishYear = 1994;
            }
            IntWritable yearOfPublication = new IntWritable(publishYear);
            Text publisher = new Text(bookDetailsArray[4]);
            return new BookWritable(isbn, title, author, yearOfPublication, publisher);
        }
    }

    public static class BookRatingMapper extends Mapper<Object, Text, Text, MyGenericWritable> {

        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            final RatingWritable ratingWritable = ratingFilter(value.toString());
            if(null != ratingWritable){
                context.write(ratingWritable.getIsbn(), new MyGenericWritable(ratingWritable));
            }
        }

        /**
         * Return {@link RatingWritable} object after filtering the data. Records having 0 rating are not considered.
         * @param ratings String having all relevant book ratings details.
         * @return {@link RatingWritable} object.
         */
        private RatingWritable ratingFilter(String ratings){
            String[] bookRatingsArray = ratings.replace("\"", "").replace(",", "")
                    .split(";");
            int rating = Integer.parseInt(bookRatingsArray[2]);
            if(rating == 0){
                return null;
            }
            int id = Integer.parseInt(bookRatingsArray[0]);
            IntWritable userId = new IntWritable(id);
            Text isbn = new Text(bookRatingsArray[1]);
            IntWritable rating1 = new IntWritable(rating);
            return new RatingWritable(userId, isbn, rating1);
        }
    }

    public static class BookReducer extends Reducer<Text, MyGenericWritable, Text, Text> {

        private Map<Integer, UserWritable> users = new HashMap<>();

        @Override
        public void setup(Context context) throws IOException{
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0){
                for (URI uri : cacheFiles){
                    FileSystem fs = FileSystem.get(uri, context.getConfiguration());
                    Path path = new Path(uri);
                    try(BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))){
                        String line;
                        while(((line = reader.readLine()) != null)) {
                            UserWritable userWritable = userFilter(line);
                            users.put(userWritable.getUserId().get(), userWritable);
                        }
                    }
                    catch (Exception e){
                        String message = "Exception occurred while reading users file from distributed cache.";
                        throw new BookRatingException(message, e);
                    }
                }
            }
        }

        @Override
        public void reduce(final Text key, final Iterable<MyGenericWritable> values, final Context context) throws
                IOException, InterruptedException {
            BookWritable book = new BookWritable();
            boolean bookFlag = false;
            List<RatingWritable> ratings = new ArrayList<>();

            for(MyGenericWritable value : values){
                Writable rawValue = value.get();
                if(rawValue instanceof BookWritable){
                    book = new BookWritable((BookWritable) rawValue);
                    bookFlag = true;
                }
                if(rawValue instanceof RatingWritable){
                    ratings.add((RatingWritable) rawValue);
                }
            }

            if(bookFlag){
                Text isbn = book.getIsbn();
                for(RatingWritable rating : ratings){
                    if(users.containsKey(rating.getUserId().get())){
                        UserWritable user = users.get(rating.getUserId().get());
                        String joinedOutput = book.toString() + "," + user.toString() + "," + rating.toString();
                        Text joinedOut = new Text(joinedOutput);
                        context.write(isbn, joinedOut);
                    }
                }
            }
        }

        /**
         * Return {@link UserWritable} object after filtering the data. Records having 0 or null age are replaced
         * by average age.
         * @param userDetails String having all relevant user details.
         * @return {@link UserWritable} object.
         */
        private UserWritable userFilter(String userDetails){
            String[] userDetailsArray = userDetails.replace("\"", "").split(";");
            int id = Integer.parseInt(userDetailsArray[0]);
            IntWritable userId = new IntWritable(id);
            String loc = userDetailsArray[1];
            if(loc.contains("usa") || loc.contains("us") || loc.contains("america")){
                loc = "usa";
            }
            else {
                loc = "non-usa";
            }
            Text location = new Text(loc);
            int age = 35;
            try{
                age = Integer.parseInt(userDetailsArray[2]);
                if(age<=0){
                    age = 35;
                }
            }
            catch (Exception e){
                logger.error(e);
            }
            IntWritable userAge = new IntWritable(age);
            return new UserWritable(userId, location, userAge);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        String input = args[0];
        String userInput = input+"/preprocess/users";
        String bookInput = input+"/preprocess/books";
        String bookRatingInput = input+"/preprocess/bookratings";
        String output = args[1]+"/preprocess";
        Boolean localRun = Boolean.parseBoolean(args[2]);
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Pre Process Data");
        job.setJarByClass(PreProcessData.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        // Delete output directory, only to ease local development; will not work on AWS. ===========
        if (localRun) {
            final FileSystem fileSystem = FileSystem.get(conf);
            WekaUtils.deleteCacheFiles(fileSystem, new Path(output));
        }
        // ================

        MultipleInputs.addInputPath(job, new Path(bookInput), TextInputFormat.class, BookMapper.class);
        MultipleInputs.addInputPath(job, new Path(bookRatingInput), TextInputFormat.class, BookRatingMapper.class);
        job.setReducerClass(BookReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyGenericWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(output));

        Path usersPath = new Path(userInput);
        FileSystem fs = FileSystem.get(usersPath.toUri(), conf);
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(usersPath, false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            URI uri = locatedFileStatusRemoteIterator.next().getPath().toUri();
            job.addCacheFile(uri);
        }
        return job.waitForCompletion(true) ? 0 : 1;
    }
}