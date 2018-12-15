
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModelBuilder {

    public static class LanguageModelMap extends Mapper<LongWritable, Text, Text, Text> {

        int threshold;

        @Override
        public void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            threshold = configuration.getInt("threshold", 20);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // read input
            // split into phrase and value "this is Big data\t100"
            String[] ngramCount = value.toString().split("\t");
            // 100
            int count = Integer.valueOf(ngramCount[1]);

            // 优化，选出现次数多于 threshold 的！
            if (count < threshold) {
                return;
            }

            // split the phrase
            String[] words = ngramCount[0].split(" ");

            StringBuilder strBuilder = new StringBuilder();
            // concatenate each words
            for (int i = 0; i < words.length - 1; i++) {
                strBuilder.append(words[i] + " ");
            }

            String outputKey = strBuilder.toString().trim();
            String outputValue = words[words.length - 1] + "=" + count;

            // output:  "this is Big" => "data=100"
            context.write(new Text(outputKey), new Text(outputValue));

            // Shuffle by key: "this is Big", all values will be put into list: {"data=100", "apple=120", "bird=12122"}

        }

    }


    public static class LanguageModelReduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {

        int topk;

        @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            topk = conf.getInt("topk", 5);
        }

        /**
         *
         * @param key  Text  e.g "this is Big"
         * @param values  Iterable<Text> { "data=100", "apple=102", "world=2" ....}
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // Read data, Design 实现topk : key = this is Big, value = TreeMap <frequency, List<following_word> e.g. <100, List("data", "bird")>
            // Sort on frequency

            TreeMap<Integer, List<String>> treeMap = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
            for (Text value : values) {
                String[] parts = value.toString().trim().split("=");
                String followingWord = parts[0].trim();
                int freqency = Integer.parseInt(parts[1].trim());

                //check TreeMap, all word with the same frequency will be added into the same Arraylist
                if (treeMap.containsKey(freqency)) {
                    treeMap.get(freqency).add(followingWord);
                } else {
                    List<String> tmp = new ArrayList<String>();
                    tmp.add(followingWord);

                    treeMap.put(freqency, tmp);
                }
            }

            // Select top K
            Iterator<Integer> iter = treeMap.keySet().iterator();
            for (int j=0; iter.hasNext() && j < topk; ) {

                int count = iter.next();
                List<String> words = treeMap.get(count);

                for (String eachWord : words) {
                    context.write(new DBOutputWritable(key.toString(), eachWord, count), NullWritable.get());
                    j++;
                }
            }

            // TODO write to database
        }

    }


}
