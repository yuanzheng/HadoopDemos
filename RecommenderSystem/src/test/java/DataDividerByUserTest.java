
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;


public class DataDividerByUserTest {

    //Specification of Mapper
    MapDriver<LongWritable, Text, IntWritable, Text> mapper;

    //Specification of Reduce
    ReduceDriver<IntWritable, Text, IntWritable, Text> reducer;

    //Specification of MapReduce program
    MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        DataDividerByUser.DataDividerMapper mapDriver = new DataDividerByUser.DataDividerMapper();
        DataDividerByUser.DataDividerReducer reducerDriver = new DataDividerByUser.DataDividerReducer();

        mapper = MapDriver.newMapDriver(mapDriver);
        reducer = ReduceDriver.newReduceDriver(reducerDriver);

        //Setup MapReduce job
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapDriver, reducerDriver);

    }


    @Test
    public void DataDividerMapperTest() {
        String input = "1,1001,5.0";

        mapper.withInput(new LongWritable(0), new Text(input));
        mapper.withOutput(new IntWritable(1), new Text("1001:5.0"));
        try {
            mapper.runTest();

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    @Test
    public void DataDividerReducerTest() {
        String inputValue1 = "1001:5.0";
        String inputValue2 = "1012:3.5";

        List<Text> values = new ArrayList<Text>();
        values.add(new Text(inputValue1));
        values.add(new Text(inputValue2));

        IntWritable key = new IntWritable(1);
        String outputValue = "1001:5.0,1012:3.5";
        reducer.withInput(key, values);
        reducer.withOutput(key, new Text(outputValue));

        try {
            reducer.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void MapReducerTest() {
        String inputValue1 = "1,1001,5.0";
        String inputValue2 = "1,1012,3.5";
        String inputValue3 = "2,1002,4.0";
        List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
        inputs.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text(inputValue1)));
        inputs.add(new Pair<LongWritable, Text>(new LongWritable(1), new Text(inputValue2)));
        inputs.add(new Pair<LongWritable, Text>(new LongWritable(2), new Text(inputValue3)));
        mapReduceDriver.addAll(inputs);

        List<Pair<IntWritable, Text>> outputs = new ArrayList<Pair<IntWritable, Text>>();
        String outputValue1 = "1001:5.0,1012:3.5";
        String outputValue2 = "1002:4.0";
        IntWritable key1 = new IntWritable(1);
        IntWritable key2 = new IntWritable(2);
        outputs.add(new Pair<IntWritable, Text>(key1,new Text(outputValue1)));
        outputs.add(new Pair<IntWritable, Text>(key2,new Text(outputValue2)));
        mapReduceDriver.withAllOutput(outputs);

        try {
            mapReduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testDriver() {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);
        Path rawInput = new Path("test/rawData/input");
        Path output = new Path("test/rawData/output");
        Path expectedOutput = new Path("test/rawData/expected");

        try {
            FileSystem fs = FileSystem.getLocal(conf);
            fs.delete(output, true); // delete old output
            DataDividerByUser driver = new DataDividerByUser();
            driver.setConf(conf);
            int exitCode = driver.run(new String[]{rawInput.toString(), output.toString()});

            assertThat(exitCode, is(0));

            checkOutput(conf, output, expectedOutput);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkOutput(Configuration conf, Path output, Path expectedOutput) throws IOException {
        FileSystem fs = FileSystem.getLocal(conf);
        Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new OutputLogFilter()));
        Path[] expectedFiles = FileUtil.stat2Paths(fs.listStatus(expectedOutput, new OutputLogFilter()));
        assertThat(outputFiles.length, is(1));

        BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
        BufferedReader expected = asBufferedReader(fs.open(expectedFiles[0]));
        String expectedLine;
        String actualLine;
        while ((expectedLine = expected.readLine()) != null && (actualLine = actual.readLine()) != null) {
            assertThat(actualLine, is(expectedLine));
        }

        //String tmp ="2\t1002:4.0";   cannnot use 'tab' in the .txt file, '\t' should be used in data and copy paste to .txt
        assertThat(actual.readLine(), nullValue());
        assertThat(expected.readLine(), nullValue());
        actual.close();
        expected.close();

    }

    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }

    public static class OutputLogFilter implements PathFilter {
        public boolean accept(Path path) {
            return !path.getName().startsWith("_");
        }
    }
}