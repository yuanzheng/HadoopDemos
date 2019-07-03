import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class MatrixMultiplication extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new MatrixMultiplication(), args);

        if (exitCode != 0) {
            System.err.printf("Failed, MatrixMultiplication causes the termination\n");
            System.exit(exitCode);
        }
    }

    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 2) {
            System.err.printf("Usage: hadoop jar RecommenderSystem-jar-with-dependencies.jar <input files> " +
                            "<UserMovieListOutput Directory> <Co-OccurrenceMatrixOutput Direcory> " +
                            "<Normalization Directory> <Multiplication Directory> <> [generic options]\n" +
                            "Here, the <input file> and <MatrixMultiplication Output Directory> are missing!\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJobName("Matrix Multiplication MapReduce Job");
        job.setJarByClass(MatrixMultiplication.class);

        ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CooccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /** First Mapper loads Co-Occurrence matrix
     *
     */
    public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input: movieB \t movieA=relation
            String[] input = value.toString().trim().split("\t");
            if (input.length < 2) {
                return;
            }

            context.write(new Text(input[0]), new Text(input[1]));
        }
    }

    /** Second Mapper loads the raw data file in which it includes user_id, movie_id and Rating
     *
     */
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input[3]: userID,movieID,rating
            String[] input = value.toString().trim().split(",");
            if (input.length < 3) {
                return;
            }

            String movieId = input[1];
            String movieRating = input[0] + ":" + input[2];

            context.write(new Text(movieId), new Text(movieRating));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        /** collect the data for each movie, then do the multiplication
         *
         * @param key Movie ID
         * @param values <movieID_A=relation, movieID_C=relation... userID_A:rating, userID_B:rating...>
         * @param context key is userID:movieID, value is rating*relation
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Double> relationMap = new HashMap<>(); // movie id and relation
            Map<String, Double> ratingMap = new HashMap<>();   // user id and rating

            for (Text value : values) {
                String data = value.toString().trim();

                if (data.contains("=")) {
                    String[] movieRelation = data.split("=");
                    try {
                        Double ralation = Double.parseDouble(movieRelation[1]);
                        relationMap.put(movieRelation[0], ralation);
                    } catch (NumberFormatException e) {
                        continue;
                    }
                } else if (data.contains(":")) {
                    String[] userRating = data.split(":");

                    try {
                        Double rating = Double.parseDouble(userRating[1]);
                        ratingMap.put(userRating[0], rating);
                    } catch (NumberFormatException e) {
                        continue;
                    }
                }
            }

            for (Map.Entry<String, Double> movieRelation: relationMap.entrySet()) {

                String movieId = movieRelation.getKey();
                Double relation = movieRelation.getValue();

                for (Map.Entry<String, Double> userRating: ratingMap.entrySet()) {
                    String userId = userRating.getKey();
                    Double rating = userRating.getValue();

                    Double multiplication = relation * rating;
                    String outputKey = userId + ":" + movieId;
                    context.write(new Text(outputKey), new DoubleWritable(multiplication));
                }

            }

        }
    }
}
