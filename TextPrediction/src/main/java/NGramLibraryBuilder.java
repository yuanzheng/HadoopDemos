

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/** Build n-gram library
 *
 */
public class NGramLibraryBuilder {

    /**
     *
     * LongWritable, Text: 输入 文件特别大，很多行！所以KEY 是 LongWritable. Text 是每一行
     *
     * Text, IntWritable： 输出 的是N-GRAM，key是 text 出现的phrase, value 是IntWritable 出现的次数
     *
     * 数据用writable 类包裹，大量的数据进进出出
     *      1. 可以直接serialization 和 deserialization
     *      2. 实现了comparable class, 所以可以做sorting
     */
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int noGram;

        @Override
        public void setup(Context context) {

        }

        /** map method, 处理输入文件中的data
         *
         * @param key  OFFSET of input
         * @param value  the context of the current line
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // read sentence by sentence


            // split sentence in to 2-gram ... n-gram
        }

    }


    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

        }

    }

}

