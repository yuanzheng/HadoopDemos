import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CoOccurrenceMatrixGeneratorTest {

    //Specification of Mapper
    MapDriver<LongWritable, Text, Text, IntWritable> mapper;

    //Specification of Reduce
    ReduceDriver<Text, IntWritable, Text, IntWritable> reducer;

    //Specification of MapReduce program
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        CoOccurrenceMatrixGenerator.MatrixGeneratorMapper mapDriver = new CoOccurrenceMatrixGenerator.MatrixGeneratorMapper();
        CoOccurrenceMatrixGenerator.MatrixGeneratorReducer reducerDriver = new CoOccurrenceMatrixGenerator.MatrixGeneratorReducer();

        mapper = MapDriver.newMapDriver(mapDriver);
        reducer = ReduceDriver.newReduceDriver(reducerDriver);

        //Setup MapReduce job
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapDriver, reducerDriver);
    }


    @Test
    public void MatrixGeneratorMapperTest() {

        String input = "1\t1:5.0,2:3.5";
        mapper.withInput(new LongWritable(0), new Text(input));
        List<Pair<Text, IntWritable>> output = new ArrayList<>();

        output.add(new Pair<>(new Text("1:1"), new IntWritable(1)));
        output.add(new Pair<>(new Text("1:2"), new IntWritable(1)));
        output.add(new Pair<>(new Text("2:1"), new IntWritable(1)));
        output.add(new Pair<>(new Text("2:2"), new IntWritable(1)));

        mapper.addAllOutput(output);

        try {
            mapper.runTest();

        } catch (IOException e) {
            e.printStackTrace();

        }

    }

    @Test
    public void MatrixGeneratorReducerTest() {

        List<IntWritable> values = new ArrayList<>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        Text key = new Text("1:1");

        Pair<Text, List<IntWritable>> input = new Pair<>(key, values);

        reducer.withInput(input).withOutput(key, new IntWritable(3));

        try {
            reducer.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void MatrixGeneratorMapReducerTest() {

        String line1 = "1\t1:5.0,2:3.5";
        String line2 = "2\t1:4.5,2:4.5,4:2.0";

        List<Pair<LongWritable, Text>> input = new ArrayList<>();
        input.add(new Pair(new LongWritable(0), new Text(line1)));
        input.add(new Pair(new LongWritable(1), new Text(line2)));
        mapReduceDriver.addAll(input);

        List<Pair<Text, IntWritable>> output = new ArrayList<>();
        output.add(new Pair<>(new Text("1:1"), new IntWritable(2)));
        output.add(new Pair<>(new Text("1:2"), new IntWritable(2)));
        output.add(new Pair<>(new Text("1:4"), new IntWritable(1)));
        output.add(new Pair<>(new Text("2:1"), new IntWritable(2)));
        output.add(new Pair<>(new Text("2:2"), new IntWritable(2)));
        output.add(new Pair<>(new Text("2:4"), new IntWritable(1)));
        output.add(new Pair<>(new Text("4:1"), new IntWritable(1)));
        output.add(new Pair<>(new Text("4:2"), new IntWritable(1)));
        output.add(new Pair<>(new Text("4:4"), new IntWritable(1)));

        mapReduceDriver.withAllOutput(output);

        try {
            mapReduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}