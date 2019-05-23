import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Transition matrix cell * PR matrix cell
 */
public class CellMultiplication {


    /**
     * Generate transition matrix cell
     */
    public static class TransitionMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Log LOG = LogFactory.getLog(TransitionMapper.class);

        /**
         * @param key
         * @param value
         * @param context <fromId, toId=probability>
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LOG.debug("Started TransitionMapper");

            String[] input = value.toString().trim().split("\t");
            if (input.length < 2) {
                return;
            }
            String fromPage = input[0].trim();
            String[] toPages = input[1].trim().split(",");

            // TODO think about Edge cases
            if (toPages.length < 1) {
                return;
            }

            // 平均分配 probability
            double probability = (double) 1 / toPages.length;

            for (String toPage : toPages) {
                String toValue = toPage + "=" + probability;
                context.write(new Text(fromPage), new Text(toValue));
            }

            LOG.debug("End TransitionMapper");

        }
    }

    /**
     * Get the page rank of each website
     */
    public static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {

        private static final Log LOG = LogFactory.getLog(PRMapper.class);

        /**
         * @param key
         * @param value
         * @param context <websiteId, weight>
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LOG.debug("Started PRMapper");


            String[] keyValuePair = value.toString().trim().split("\t");
            if (keyValuePair.length < 2) {
                return;
            }

            String website = keyValuePair[0];
            double weight = Double.parseDouble(keyValuePair[1]);

            context.write(new Text(website), new Text(String.valueOf(weight)));
            LOG.debug("End PRMapper");

        }
    }

    /**
     * Transition matrix cell * PR matrix cell
     */
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        private static final Log LOG = LogFactory.getLog(MultiplicationReducer.class);

        private float beta; // By default, 0.15f the possibility to open a new site

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();

            beta = conf.getFloat("beta", 0.15f);

        }

        /**
         * @param key     from website
         * @param values  to website probabilities and from website weight
         * @param context <toId, probability * weightß>
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            LOG.debug("Started MultiplicationReducer");

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            List<String> transitionUnit = new ArrayList<String>();
            double pr = 0;  // last element is the pr

            for (Text value : values) {
                // either b=0.3, or 0.25
                String probability = value.toString().trim();
                if (probability.contains("=")) {
                    transitionUnit.add(probability);
                } else {
                    pr = Double.parseDouble(probability);
                }
            }

            /* Multiply transition cell and PR(n-1) cell */
            for (String each : transitionUnit) {
                String[] data = each.split("=");
                String to = data[0];
                Double probability = Double.parseDouble(data[1]);
                Double weight = probability * pr * (1 - beta);

                context.write(new Text(to), new Text(String.valueOf(weight)));
            }

            LOG.debug("End MultiplicationReducer");

        }
    }
}
