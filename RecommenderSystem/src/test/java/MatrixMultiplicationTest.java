
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputLogFilter;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;


public class MatrixMultiplicationTest {

    //Specification of Mapper
    MapDriver<LongWritable, Text, Text, Text> mapDriver1;
    MapDriver<LongWritable, Text, Text, Text> mapDriver2;

    //Specification of Reduce
    ReduceDriver<Text, Text, Text, DoubleWritable> reduceDriver;

    //Specification of MapReduce program
    MapReduceDriver<LongWritable, Text, Text, Text, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        MatrixMultiplication.CooccurrenceMapper cooccurrenceMapper = new MatrixMultiplication.CooccurrenceMapper();
        MatrixMultiplication.RatingMapper ratingMapper = new MatrixMultiplication.RatingMapper();

        mapDriver1 = MapDriver.newMapDriver(cooccurrenceMapper);
        mapDriver2 = MapDriver.newMapDriver(ratingMapper);

        MatrixMultiplication.MultiplicationReducer reducer = new MatrixMultiplication.MultiplicationReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        //Setup MapReduce 1 job
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(cooccurrenceMapper, reducer);
    }


    @Test
    public void CooccurrenceMapperTest() throws InterruptedException {
        String input = "79\t1=0.010251630941286114";

        mapDriver1.withInput(new LongWritable(0), new Text(input));
        mapDriver1.withOutput(new Text("79"), new Text("1=0.010251630941286114"));

        try {
            mapDriver1.runTest();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void RatingMapperTest() throws InterruptedException {

        String input = "1,79,5.0";
        mapDriver2.withInput(new LongWritable(0), new Text(input));
        mapDriver2.withOutput(new Text("79"), new Text("1:5.0"));

        try {
            mapDriver2.runTest();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void MultiplicationReducerTest() throws InterruptedException {

        String inputKey = "79";
        List<Text> inputValue = new ArrayList<>();
        inputValue.add(new Text("2=0.010251630941286114"));
        inputValue.add(new Text("1:5.0"));

        reduceDriver.withInput(new Text(inputKey), inputValue);

        Double relation = Double.parseDouble("0.010251630941286114");
        Double rating = Double.parseDouble("5.0");
        Double outputValue = relation * rating;
        String outputKey = "1:2";

        reduceDriver.withOutput(new Text(outputKey), new DoubleWritable(outputValue));

        try {
            reduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
//
//    @Test
//    public void MultiplicationMapReduceTest() throws InterruptedException {
//
//        String inputValue1 = "79\t2=0.010251630941286114";
//        String inputValue2 = "1,79,5.0";
//        List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
//        inputs.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text(inputValue1)));
//        inputs.add(new Pair<LongWritable, Text>(new LongWritable(1), new Text(inputValue2)));
//        mapReduceDriver.with(inputs);
//
//        Double relation = Double.parseDouble("0.010251630941286114");
//        Double rating = Double.parseDouble("5.0");
//        Double outputValue = relation * rating;
//        String outputKey = "1:2";
//
//        List<Pair<Text, DoubleWritable>> output = new ArrayList<>();
//        output.add(new Pair(new Text(outputKey), new DoubleWritable(outputValue)));
//        mapReduceDriver.withAllOutput(output);
//
//        try {
//            mapReduceDriver.runTest();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }

    @Test
    public void testDriver() {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.setInt("mapreduce.task.io.sort.mb", 1);
        Path inputCoOccurrenceMatrix = new Path("test/multiplication/input");
        Path rawInput = new Path("test/rawData/input");
        Path output = new Path("test/multiplication/output");
        //Path expectedOutput = new Path("test/multiplication/expected");

        try {
            FileSystem fs = FileSystem.getLocal(conf);
            fs.delete(output, true); // delete old output
            MatrixMultiplication driver = new MatrixMultiplication();
            driver.setConf(conf);
            int exitCode = driver.run(new String[]{inputCoOccurrenceMatrix.toString(),
                                                   rawInput.toString(), output.toString()});

            assertThat(exitCode, is(0));

            //checkOutput(conf, output, expectedOutput);
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
        assertThat(outputFiles.length, is(2));

        BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
        BufferedReader expected = asBufferedReader(fs.open(expectedFiles[0]));
        String expectedLine;
        String actualLine;
        while ((expectedLine = expected.readLine()) != null && (actualLine = actual.readLine()) != null) {
            // 4 spaces !!!
            actualLine = actualLine.replaceAll("\t", "    ");

            assertThat(actualLine, is(expectedLine));
        }
        assertThat(actual.readLine(), nullValue());
        assertThat(expected.readLine(), nullValue());
        actual.close();
        expected.close();

    }

    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }

}