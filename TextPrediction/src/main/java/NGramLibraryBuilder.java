

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Build n-gram library
 */
public class NGramLibraryBuilder {

    /**
     * LongWritable, Text: 输入 文件特别大，很多行！所以KEY 是 LongWritable. Text 是每一行
     * <p>
     * Text, IntWritable： 输出 的是N-GRAM，key是 text 出现的phrase, value 是IntWritable 出现的次数 (1)
     * <p>
     * 数据用writable 类包裹，大量的数据进进出出
     * 1. 可以直接serialization 和 deserialization
     * 2. 实现了comparable class, 所以可以做sorting
     */
    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int noGram;

        /** Initialize noGram
         *  Value has been set by configuration in the Driver.
         *
         * @param context, gets configuration settings
         */
        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            noGram = configuration.getInt("noGram", 5);
        }

        /**
         * map method, 处理输入文件中的data。生成 N-Gram Library 的 key。
         * 需要 同时 创建 2-Gram, 3-Gram, ... 直到 N-gram
         *
         * @param key     OFFSET of input
         * @param value   the context of the current line
         * @param context object is able to write output
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // read sentence by sentence, system does it automatically by the settings of configuration
            // split sentence in to 2-gram ... n-gram
            // Preprocessing: Clean data, e.g. any non-alphabet character will be replaced by empty spaces.
            String sentence = value.toString().toLowerCase().replace("[^a-z]", " ");
            // split a string by empty spaces
            String[] words = sentence.split("\\s+");

            if (words.length < 2) {
                return;
            }

            StringBuilder stringBuilder;

            for (int i = 0; i < words.length - 1; i++) {
                stringBuilder = new StringBuilder();

                // add the first word
                stringBuilder.append(words[i]);

                // add the second word, third word, ... till the Nth
                for (int j = 1; j + i < words.length && j < noGram; j++) {
                    stringBuilder.append(" ");
                    stringBuilder.append(words[i + j]);

                    // output
                    context.write(new Text(stringBuilder.toString().trim()), new IntWritable(1));
                }
            }

        }

    }


    /** Count the number of N-Gram appearance.
     *
     */
    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // key = nGram, value = <1,1,1,1...>
            int sum = 0;

            // calculate the total sum of nGram, Iterable cannot give the length, so have to loop it
            for (IntWritable value : values) {
                sum += value.get();
            }

            // output format: key + \t + value of IntWritable, e.g. this is\t100
            context.write(key, new IntWritable(sum));
        }

    }

}

