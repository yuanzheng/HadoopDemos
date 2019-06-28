import java.io.IOException;

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


/**
 *
 */
public class CoOccurrenceMatrixGenerator extends Configured implements Tool {

    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /**
         *
         * @param key
         * @param value   user_id\tmovie_id:rating,movie_id:rating,....
         * @param context movieA:movieB 1
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] inputData = value.toString().trim().split("\t");
            if (inputData.length < 2) {
                return;
            }

            String movies = inputData[1];
            String[] movieAndRating = movies.trim().split(",");

            for (int i=0; i<movieAndRating.length; i++) {
                String movieA = movieAndRating[i].trim().split(":")[0];
                for (int j=0; j<movieAndRating.length; j++) {
                    String movieB = movieAndRating[j].trim().split(":")[0];

                    context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
                }
            }

        }
    }

    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 2) {
            System.err.printf("Usage: hadoop jar RecommenderSystem-jar-with-dependencies.jar <input files> " +
                            "<UserMovieListOutput Directory> <Co-OccurrenceMatrixOutput Direcory> " +
                            "<Normalization Directory> <> <> [generic options]\n" +
                            "Here, the <UserMovieListOutput Directory> and <Co-OccurrenceMatrixOutput Direcory> are missing!\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJobName("Co-occurrence Matrix Generator");

        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);

        job.setJarByClass(CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception{

        int exitCode = ToolRunner.run(new DataDividerByUser(), args);

        if (exitCode != 0) {
            System.err.printf("Failed, Co-Occurrence Matrix Generator causes the termination\n");
            System.exit(exitCode);
        }
    }
}