import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import java.util.HashMap;
import java.util.Map;

/** The relations in the Co-Occurrence matrix should be re-evaluated.
 *
 */
public class Normalization extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Normalization(), args);

        if (exitCode != 0) {
            System.err.printf("Failed, the Normalization causes the termination\n");
            System.exit(exitCode);
        }

    }

    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 2) {
            System.err.printf("Usage: hadoop jar RecommenderSystem-jar-with-dependencies.jar <input files> " +
                            "<UserMovieListOutput Directory> <Co-OccurrenceMatrixOutput Direcory> " +
                            "<Normalization Directory> <> <> [generic options]\n" +
                            "Here, the <Co-OccurrenceMatrixOutput Direcory> and <Normalization Directory> are missing!\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalization.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] input = value.toString().trim().split("\t");

            if (input.length != 2) {
                return;
            }

            String[] moviesId = input[0].trim().split(":");
            String movieA = moviesId[0];
            String movieB = moviesId[1];

            // movieA refers to the row, and movieB is the column
            context.write(new Text(movieA), new Text(movieB + "=" + input[1]));
        }

    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {

        /** Compute the average value of each relation.
         * The matrix cell in the same row are collected, however the key in the output should be the column
         *
         * @param key   the row
         * @param values   cells in the same row are gethered together
         * @param context  the key in the output is the column
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // a HashMap is used here because "Iterable" can be used once!
            Map<String, Integer> cache = new HashMap<>();

            // sum up all relations
            int sum = 0;
            for (Text value : values) {
                String[] movie = value.toString().trim().split("=");
                int relation = Integer.parseInt(movie[1]);
                sum += relation;

                cache.put(movie[0], relation);
            }

            // compute the average
            for (Map.Entry<String, Integer> entry : cache.entrySet()) {
                String outputKey = entry.getKey();
                int relation = entry.getValue();
                double normalized = (double) (relation / sum);
                String outputValue = key.toString() + "=" + normalized;

                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }
}
