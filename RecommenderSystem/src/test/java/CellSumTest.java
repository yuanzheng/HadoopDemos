import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class CellSumTest {

    //Specification of Mapper
    MapDriver<LongWritable, Text, Text, DoubleWritable> mapper;

    //Specification of Reduce
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reducer;

    //Specification of MapReduce program
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        CellSum.SumMapper mapDriver = new CellSum.SumMapper();
        CellSum.SumReducer reducerDriver = new CellSum.SumReducer();

        mapper = MapDriver.newMapDriver(mapDriver);
        reducer = ReduceDriver.newReduceDriver(reducerDriver);

        //Setup MapReduce job
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapDriver, reducerDriver);
    }


    @Test
    public void sumMapperTest() {
        String input = "305344:79\t0.002419177479656917";

        double value = Double.parseDouble("0.002419177479656917");

        mapper.withInput(new LongWritable(0), new Text(input));

        mapper.withOutput(new Text("305344:79"), new DoubleWritable(value));
        try {
            mapper.runTest();

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    @Test
    public void sumReducerTest() {
        List<DoubleWritable> input = new ArrayList<>();
        input.add(new DoubleWritable(Double.parseDouble("0.002419177479656917")));
        input.add(new DoubleWritable(Double.parseDouble("0.002492485888131369")));
        input.add(new DoubleWritable(Double.parseDouble("0.0019060186203357526")));
        input.add(new DoubleWritable(Double.parseDouble("0.0011729345355912322")));

        reducer.withInput(new Text("305344:79"), input);

        double sum = 0.0;
        for (DoubleWritable each : input) {
             sum += each.get();
        }

        reducer.withOutput(new Text("305344:79"), new DoubleWritable(sum));

        try {
            reducer.runTest();
        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    @Test
    public void sumMapReduceTest() {
        List<String> data = new ArrayList<>();
        // user 1346432
        data.add("1346432:79\t0.009676709918627667");
        data.add("1346432:79\t0.2004984971776263");
        data.add("1346432:79\t0.3770984531925812");
        // user 305344
        data.add("305344:79\t0.002419177479656917");
        data.add("305344:79\t0.002492485888131369");
        data.add("305344:79\t0.0019060186203357526");
        data.add("305344:79\t0.0011729345355912322");
        data.add("305344:80\t0.003477396920019871");
        data.add("305344:80\t0.037754595131644315");


        List<Pair<LongWritable, Text>> input = new ArrayList<>();
        Map<String, Double> sum = new HashMap<>();  // the Expected result
        int i = 0;
        for (String each : data) {
            // build up the Input for testing
            input.add(new Pair(new LongWritable(i++), new Text(each)));

            // build up expected output
            String[] idAndRate = each.split("\t");
            if (sum.containsKey(idAndRate[0])) {
                double tmp = sum.get(idAndRate[0]) + Double.parseDouble(idAndRate[1]);
                sum.put(idAndRate[0], tmp);
            } else {
                sum.put(idAndRate[0], Double.parseDouble(idAndRate[1]));
            }
        }

        mapReduceDriver.withAll(input);

        try {
            List<Pair<Text, DoubleWritable>> result = mapReduceDriver.run();
            assertThat(result.size(), is(sum.size()));

            // compare to the expected results
            for (Pair<Text, DoubleWritable> each : result) {
                // contains key
                assertThat(sum.containsKey(each.getFirst().toString()), is(Boolean.TRUE));
                // the actual key and value pairs matches the expects'
                assertThat(each.getSecond().get(), is(sum.get(each.getFirst().toString())));
                System.out.println("debug actual result: " + each.getFirst().toString() + ":" + sum.get(each.getFirst().toString()));
            }

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
        Path rawInput = new Path("test/sum/input");
        Path output = new Path("test/sum/output");
        Path expectedOutput = new Path("test/sum/expected");

        try {
            FileSystem fs = FileSystem.getLocal(conf);
            fs.delete(output, true); // delete old output
            CellSum driver = new CellSum();
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
        Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(output, new DataDividerByUserTest.OutputLogFilter()));
        Path[] expectedFiles = FileUtil.stat2Paths(fs.listStatus(expectedOutput, new DataDividerByUserTest.OutputLogFilter()));
        assertThat(outputFiles.length, is(1));

        BufferedReader actual = asBufferedReader(fs.open(outputFiles[0]));
        BufferedReader expected = asBufferedReader(fs.open(expectedFiles[0]));
        String expectedLine;
        String actualLine;
        List<String> expectedResult = new ArrayList<>();
        while ((expectedLine = expected.readLine()) != null) {
            expectedResult.add(expectedLine);
        }
        expected.close();

        List<String> actualResult = new ArrayList<>();
        while ((actualLine = actual.readLine()) != null) {
            actualResult.add(actualLine);
        }
        actual.close();

        // same size
        assertThat(actualResult.size(), is(expectedResult.size()));

        Collections.sort(expectedResult);
        Collections.sort(actualResult);
        // compare each element of actual data to expected data
        for (int i = 0; i<actualResult.size(); i++) {
            assertThat(actualResult.get(i), is(expectedResult.get(i)));
        }

    }

    private BufferedReader asBufferedReader(InputStream in) throws IOException {
        return new BufferedReader(new InputStreamReader(in));
    }

}