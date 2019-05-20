import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/** Transition matrix cell * PR matrix cell
 */
public class CellMultiplication {

    /** Generate transition matrix cell
     *
     */
    public static class TransitionMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         *
         * @param key
         * @param value
         * @param context <fromId, toId=probability>
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

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

        }
    }

    /** Get the page rank of each website
     *
     *
     */
    public static class PRMapper extends Mapper<LongWritable, Text, Text, Text> {

        /**
         *
         * @param key
         * @param value
         * @param context <websiteId, weight>
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] keyValuePair = value.toString().trim().split("\t");
            if (keyValuePair.length < 2) {
                return;
            }

            String website = keyValuePair[0];
            double weight = Double.parseDouble(keyValuePair[1]);

            context.write(new Text(website), new Text(String.valueOf(weight)));
        }
    }

    /** Transition matrix cell * PR matrix cell
     *
     */
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        /**
         *
         * @param key from website
         * @param values to website probabilities and from website weight
         * @param context <toId, probability * weightß>
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

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
                Double weight = probability * pr;

                context.write(new Text(to), new Text(String.valueOf(weight)));
            }
        }
    }
}
