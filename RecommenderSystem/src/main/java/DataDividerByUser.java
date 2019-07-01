import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Step 1, pre-processing data
 * input format is User_id, Movie_id, Rating
 * output format is User_id, List<Movie_id : Rating>
 */
public class DataDividerByUser extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new DataDividerByUser(), args);

        if (exitCode != 0) {
            System.err.printf("Failed, DataDivideByUser causes the termination\n");
            System.exit(exitCode);
        }
    }

    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 2) {
            System.err.printf("Usage: hadoop jar RecommenderSystem-jar-with-dependencies.jar <input files> " +
                            "<UserMovieListOutput Directory> <Co-OccurrenceMatrixOutput Direcory> " +
                            "<Normalization Directory> <> <> [generic options]\n" +
                            "Here, the <input file> and <UserMovieListOutput Directory> are missing!\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJobName("Data Divide By User");
        job.setJarByClass(DataDividerByUser.class);

        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    /**
     * Read data from Input file, in which involves user_id, movie_id and rating.
     * Pre-process data in a new format, in which involve user_id and movie_id:rating.
     */
    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] user_movie_rating = value.toString().trim().split(",");
            if (user_movie_rating.length < 3)
                return;

            int userId = Integer.parseInt(user_movie_rating[0]);
            String movieId = user_movie_rating[1];
            String rating = user_movie_rating[2];

            context.write(new IntWritable(userId), new Text(movieId + ":" + rating));
        }

    }

    /**
     * Merge movie_id:rating with the same userId
     */
    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            StringBuilder data = new StringBuilder();
            Iterator<Text> allValues = values.iterator();

            if (allValues.hasNext()) {
                data.append(allValues.next());
            }

            while (allValues.hasNext()) {
                data.append("," + allValues.next());
            }

            context.write(key, new Text(data.toString()));
        }
    }

}