
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.Chain;
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

    private String subPRMatrix = "\\output";   // By Default. It's for the output of first MapReduce

    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 3) {
            System.err.printf("Usage: hadoop jar PageRank-jar-with-dependencies.jar <transition directory> " +
                    " <PR directory> <times of convergence>" +
                            " [generic options]\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        String transitionMatrix = args[0];
        String prMatrix = args[1];
        //String subPRMatrix = args[2];  //first mapreduce job's output
        int count = Integer.parseInt(args[2]); //times of convergence

        final int normalTermination = 0;
        int status = normalTermination;

        // after several loop, get the convergence
        for (int i = 0; i < count; i++) {

            // first MapReduce job， prMatrix+i is the directory of each PRn, subPRMatrix + i is the output of first MR
            status = cellMultiplicationJob(transitionMatrix, prMatrix, i);

            // if mapreduce terminates abnormally, quit immediately.
            if (status != normalTermination) {
                /** TODO whether stopping mapreduce job when one of Mapreduce job terminates abnormally? */
                break;
            }

            // second MapReduce job: prMatrix is the directory prefix of pr beta Map. For example: SumUpJob(0,/pagerank)
            status = SumUpJob(i, prMatrix);

            // if mapreduce terminates abnormally, quit immediately.
            if (status != normalTermination) {
                /** TODO whether stopping mapreduce job when one of Mapreduce job terminates abnormally? */
                break;
            }
        }

        return status;
    }

    /**
     * Transition Matrix cell * PR Matrix cell.
     *
     * @param transitionMatrix transition data directory
     * @param prMatrix the directory prefix of PageRank, for example: /PageRank, however actually is /PageRank0, /PageRank1 ...
     * @param index the tag of current iteration
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    private int cellMultiplicationJob(String transitionMatrix, String prMatrix, int index)
            throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.addResource("reference.xml");

        // the output of first MapReduce should be stored in subPRMatrix, for example /Output0, /Output1 ...
        if (conf.get("firstMROutput") != null) {
            // 自定义 property: the directory of multiplication result
            subPRMatrix = conf.get("firstMROutput") + index;
        } else {
            subPRMatrix = subPRMatrix + index;
        }

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

        /* Read input file from, for example: /transition and /pagerank0 */
        MultipleInputs.addInputPath(job, new Path(transitionMatrix), TextInputFormat.class, CellMultiplication.TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(prMatrix + index), TextInputFormat.class, CellMultiplication.PRMapper.class);

        /* Indicate a file directory for subPR, for example: /output0 */
        FileOutputFormat.setOutputPath(job, new Path(subPRMatrix));

        return job.waitForCompletion(true) ? 0 : 1;
    }


    /**
     * Sum up cell for each webpage.
     *
     * @param index  the tag of current iteration
     * @param prMatrix current PR matrix
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    private int SumUpJob(int index, String prMatrix)
            throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.addResource("reference.xml");

        // Read the output of first MapReduce job, for example: /Output0, /Output1 ...
        if (conf.get("firstMROutput") != null) {
            // 自定义 property: the directory of multiplication result
            subPRMatrix = conf.get("firstMROutput") + index;
        } else {
            subPRMatrix = subPRMatrix + index;
        }

        Job job = Job.getInstance(conf);
        job.setJarByClass(CellSum.class);

        ChainMapper.addMapper(job, CellSum.PassMapper.class,
                Object.class, Text.class, Text.class, DoubleWritable.class, conf);

        // Second Mapper's input value should match first Mapper's output value
        ChainMapper.addMapper(job, CellSum.PRBetaMapper.class,
                Text.class, DoubleWritable.class, Text.class, DoubleWritable.class, conf);

        job.setReducerClass(CellSum.SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        /* Read input file from, for example /output0 */
        MultipleInputs.addInputPath(job, new Path(subPRMatrix), TextInputFormat.class, CellSum.PassMapper.class);
        /* Read PR matrix, beta * PR , for example for /pagerank0 */
        MultipleInputs.addInputPath(job, new Path(prMatrix + index), TextInputFormat.class, CellSum.PRBetaMapper.class);

        /* produce a new PR matrix file for next iteration. for example: /pagerank1*/
        FileOutputFormat.setOutputPath(job, new Path(prMatrix + (index + 1)));

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
