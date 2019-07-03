import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
import java.util.HashMap;
import java.util.Map;

public class CellSum extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new MatrixMultiplication(), args);

        if (exitCode != 0) {
            System.err.printf("Failed, CellSum causes the termination\n");
            System.exit(exitCode);
        }
    }

    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 2) {
            System.err.printf("Usage: hadoop jar RecommenderSystem-jar-with-dependencies.jar <input files> " +
                            "<UserMovieListOutput Directory> <Co-OccurrenceMatrixOutput Directory> " +
                            "<Normalization Directory> <Multiplication Directory> <CellSum Directory> [generic options]\n" +
                            "Here, the <MatrixMultiplication Output Directory> and <Cell Sum Output directory> are missing!\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration conf = getConf();

        Job job = Job.getInstance(conf);
        job.setJobName("Cell Sum MapReduce Job");
        job.setJarByClass(CellSum.class);

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] userMovieRelation = value.toString().trim().split("\t");
            if (userMovieRelation.length < 2) {
                return;
            }

            Double rating = Double.parseDouble(userMovieRelation[1]);
            context.write(new Text(userMovieRelation[0]), new DoubleWritable(rating));
        }

    }

    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            for (DoubleWritable value: values) {
                sum += value.get();
            }

            context.write(key, new DoubleWritable(sum));

        }
    }

}
