import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NormalizationTest {

    //Specification of Mapper
    MapDriver<LongWritable, Text, Text, Text> mapper;

    //Specification of Reduce
    ReduceDriver<Text, Text, Text, Text> reducer;

    //Specification of MapReduce program
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() throws Exception {

        Normalization.NormalizeMapper mapDriver = new Normalization.NormalizeMapper();
        Normalization.NormalizeReducer reduceDriver = new Normalization.NormalizeReducer();

        mapper = MapDriver.newMapDriver(mapDriver);
        reducer = ReduceDriver.newReduceDriver(reduceDriver);

        //Setup MapReduce job
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapDriver, reduceDriver);
    }


    @Test
    public void NormalizeMapperTest() {
        String input = "100:1\t8";
        mapper.withInput(new LongWritable(0), new Text(input));
        mapper.withOutput(new Text("100"), new Text("1=8"));

        try {
            mapper.runTest();

        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    @Test
    public void NormalizeReducerTest() {
        String inputKey = "100";
        String inputValue1 = "1=8";
        String inputValue2 = "10=9";

        List<Text> values = new ArrayList<Text>();
        values.add(new Text(inputValue1));
        values.add(new Text(inputValue2));
        reducer.withInput(new Text(inputKey), values);

        int sum = 8 + 9;
        double average1 = (double) 8 / sum;
        double average2 = (double) 9 / sum;
        List<Pair<Text, Text>> output = new ArrayList<>();
        output.add(new Pair<>(new Text("1"), new Text("100=" + average1)));
        output.add(new Pair<>(new Text("10"), new Text("100=" + average2)));
        reducer.withAllOutput(output);

        try {
            reducer.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void NormalizationMapReduceTest() {

        String inputValue1 = "100:16\t17";
        String inputValue2 = "100:17\t13";
        String inputValue3 = "100:18\t12";
        List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
        inputs.add(new Pair<>(new LongWritable(0), new Text(inputValue1)));
        inputs.add(new Pair<>(new LongWritable(1), new Text(inputValue2)));
        inputs.add(new Pair<>(new LongWritable(2), new Text(inputValue3)));
        mapReduceDriver.addAll(inputs);

        int sum = 17 + 13 + 12;
        double average1 = (double) 17 / sum;
        double average2 = (double) 13 / sum;
        double average3 = (double) 12 / sum;
        List<Pair<Text, Text>> output = new ArrayList<>();
        output.add(new Pair<>(new Text("16"), new Text("100=" + average1)));
        output.add(new Pair<>(new Text("17"), new Text("100=" + average2)));
        output.add(new Pair<>(new Text("18"), new Text("100=" + average3)));
        mapReduceDriver.withAllOutput(output);

        try {
            mapReduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}