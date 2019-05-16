
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import java.io.IOException;

/** PageRank is implemented by computing the multiplication of Transition matrix and PR matrix.
 *  It includes two MapReduce jobs, cell multiplication job and cell sum up job.
 *
 *
 */
public class Driver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        if (args.length < 2) {
            System.err.printf("Usage: hadoop jar PageRank-jar-with-dependencies.jar <input files> " +
                            " [generic options]\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }


        int status = 0;

        status = cellMultiplicationJob();


        status = SumUpJob();


        return status;
    }

    /** Transition Matrix cell * PR Matrix cell.
     *
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    private int cellMultiplicationJob() throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);




        return job.waitForCompletion(true) ? 0 : 1;
    }


    /** Sum up cell for each webpage.
     *
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    private int SumUpJob() throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();


        Job job = Job.getInstance(conf);




        return job.waitForCompletion(true) ? 0 : 1;
    }


    /** PageRank main
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Driver(), args);

        System.exit(exitCode);
    }

}
