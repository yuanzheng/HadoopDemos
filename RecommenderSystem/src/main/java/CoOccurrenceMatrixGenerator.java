import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 *
 */
public class CoOccurrenceMatrixGenerator {

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

    public static void main(String[] args) throws Exception{

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

        job.waitForCompletion(true);

    }
}