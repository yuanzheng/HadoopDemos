
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * PageRank is implemented by computing the multiplication of Transition matrix and PR matrix.
 * It includes two MapReduce jobs, cell multiplication job and cell sum up job.
 */
public class Driver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 3) {
            System.err.printf("Usage: hadoop jar PageRank-jar-with-dependencies.jar <input files> " +
                            " [generic options]\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        String subPRMatrix = args[2];
        int count = Integer.parseInt(args[3]); //times of convergence

        final int normalTermination = 0;
        int status = normalTermination;

        /** TODO whether stopping mapreduce job when one of Mapreduce job terminates abnormally? */
        // after several loop, get the convergence
        for (int i = 0; i < count; i++) {

            // first MapReduce job
            status = cellMultiplicationJob(transitionMatrix, prMatrix + i, subPRMatrix + i);

            // if mapreduce terminates abnormally, quit immediately.
            if (status != normalTermination) {
                break;
            }

            // second MapReduce job: prMatrix + (i+1) is the output of reducer. It is the input PR matrix in next iteration.
            status = SumUpJob(subPRMatrix + i, prMatrix + (i+1));

            // if mapreduce terminates abnormally, quit immediately.
            if (status != normalTermination) {
                break;
            }
        }


        return status;
    }

    /**
     * Transition Matrix cell * PR Matrix cell.
     *
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    private int cellMultiplicationJob(String transitionMatrix, String prMatrix, String subPRMatrix)
            throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CellMultiplication.class);

        /* Chain two mappers: Maper for Transition matrix and Mapper for PR matrix */
        ChainMapper.addMapper(job, CellMultiplication.TransitionMapper.class,
                Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, CellMultiplication.PRMapper.class,
                Object.class, Text.class, Text.class, Text.class, conf);
        // set reduce class
        job.setReducerClass(CellMultiplication.MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        /* TODO Read input file from */
        MultipleInputs.addInputPath(job, new Path(transitionMatrix), TextInputFormat.class, CellMultiplication.TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(prMatrix), TextInputFormat.class, CellMultiplication.PRMapper.class);

        /* TODO Indicate a file directory for subPR */
        FileOutputFormat.setOutputPath(job, new Path(subPRMatrix));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * Sum up cell for each webpage.
     *
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    private int SumUpJob(String subPRMatrix, String prMatrix)
            throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(CellSum.class);

        job.setMapperClass(CellSum.PassMapper.class);
        job.setReducerClass(CellSum.SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        /* TODO */
        FileInputFormat.addInputPath(job, new Path(subPRMatrix));
        FileOutputFormat.setOutputPath(job, new Path(prMatrix));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * PageRank main
     *
     * @param args args0: dir of transition.txt
     *             args1: dir of PageRank.txt
     *             args2: dir of unitMultiplication result
     *             args3: times of convergence
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Driver(), args);

        System.exit(exitCode);
    }

}
