
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Aggregate each multiplication in the same row to generate PageRank
 */
public class CellSum {

    public static class PassMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private static final Log LOG = LogFactory.getLog(PassMapper.class);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            LOG.debug("Started PassMapper");


            String[] pageAndWeight = value.toString().trim().split("\t");
            if (pageAndWeight.length != 2) {
                return;
            }

            context.write(new Text(pageAndWeight[0]), new DoubleWritable(Double.parseDouble(pageAndWeight[1])));

            LOG.debug("End PassMapper");
        }
    }

    /**
     * Sum up cell for each multiplication in the same row index
     */
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private static final Log LOG = LogFactory.getLog(SumReducer.class);

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            LOG.debug("Started SumReducer");


            //input key = toPage value = <unitMultiplication>
            //target: sum!
            double sum = 0;
            for (DoubleWritable each : values) {
                sum += each.get();
            }

            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));

            context.write(key, new DoubleWritable(sum));

            LOG.debug("End SumReducer");

        }
    }

}
