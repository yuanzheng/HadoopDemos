import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver {

    private static String inputDir = "./";               // Default
    private static String nGramLib;
    private static String numberOfNGram = "5";           // Default
    private static String threshold = "10";              // Default
    private static String topk = "3";  // Default


    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        // Read arguments from command line
        // input file
        inputDir = args[0];
        nGramLib = args[1];
        // indicates the largest n-gram (2, 3, ..., N)
        numberOfNGram = args[2];

        //the word with frequency under threshold will be discarded
        threshold = args[3];
        topk = args[4];

        // start job 1
        jobOne();

        jobTwo();
    }

    private static void jobOne() throws ClassNotFoundException, IOException, InterruptedException {

        /** Configuration 有默认参数，用set 方法 从新定义
         *
         */
        Configuration conf1 = new Configuration();

        /** Define the job to read data sentence by sentence
         *  重新定义系统的property delimiter
         */
        conf1.set("textinputformat.record.delimiter", ".");
        // 自定义 property: noGram, 在MapReduce中会用到。 通过configuration 传递
        conf1.set("noGram", numberOfNGram);

        Job job1 = Job.getInstance(conf1);
        job1.setJobName("NGram");
        job1.setJarByClass(Driver.class);

        job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // 选择默认的文件处理方式
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        // Read data from "inputDir" file and Write data into "nGramLib" file
        TextInputFormat.setInputPaths(job1, new Path(inputDir));
        TextOutputFormat.setOutputPath(job1, new Path(nGramLib));
        job1.waitForCompletion(true);

    }


    private static void jobTwo() throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf2 = new Configuration();
        // 自定义 property:
        conf2.set("threshold", threshold);
        conf2.set("topk", topk);

        DBConfiguration.configureDB(conf2,
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://192.168.1.98:8889/test",
                "root",
                "root");

        Job job2 = Job.getInstance(conf2);
        job2.setJobName("Model");
        job2.setJarByClass(Driver.class);

        /** Add an external dependency to current job
         *  1. upload dependency to hdfs
         *  2. use "addArchiveToClassPath" method to define the dependency path on hdfs
         *
         *  这个JAR 包帮助 链接 database
         *
         *  所以需要 hadoop-mapreduce-client-core
         */
        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));

        job2.setMapperClass(LanguageModelBuilder.LanguageModelMap.class);
        job2.setReducerClass(LanguageModelBuilder.LanguageModelReduce.class);

        /** Because map output key and value are inconsistent with reducer output key and value
         *  setMapOutputKeyClass, setMapOutputValueClass, setOutputKeyClass, setOutputValueClass
         *  会出错，如果不明确指出类型
         */
        // set Mapper的 Output 类型,
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // set Reducer的 Output 类型，write data into database
        job2.setOutputKeyClass(DBOutputWritable.class);
        job2.setOutputValueClass(NullWritable.class);

        // 选择默认的文件 input 处理方式, 但Output是 database
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);

        // Read data from "nGramLib" file
        TextInputFormat.setInputPaths(job2, nGramLib);

        // define the table: table name and columns
        DBOutputFormat.setOutput(job2, "output", new String[]{"starting_phrase", "following_word", "count"});


        job2.waitForCompletion(true);

    }

}