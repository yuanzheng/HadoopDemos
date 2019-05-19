
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Aggregate each multiplication in the same row to generate PageRank
 */
public class CellSum {

    public static class PassMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] pageAndWeight = value.toString().trim().split("\t");
            if (pageAndWeight.length != 2) {
                return;
            }

            context.write(new Text(pageAndWeight[0]), new DoubleWritable(Double.parseDouble(pageAndWeight[1])));
        }
    }

    /**
     * Sum up cell for each multiplication in the same row index
     */
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            //input key = toPage value = <unitMultiplication>
            //target: sum!
            double sum = 0;
            for (DoubleWritable each : values) {
                sum += each.get();
            }

            //TODO Decimal format?

            context.write(key, new DoubleWritable(sum));
        }
    }

}
