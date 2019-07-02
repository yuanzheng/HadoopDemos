
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


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

    @Test
    public void MultiplicationMapReduceTest() throws InterruptedException {

        String inputValue1 = "79\t1=0.010251630941286114";
        String inputValue2 = "1,79,5.0";
        List<Pair<LongWritable, Text>> inputs = new ArrayList<Pair<LongWritable, Text>>();
        inputs.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text(inputValue1)));
        inputs.add(new Pair<LongWritable, Text>(new LongWritable(1), new Text(inputValue2)));
        mapReduceDriver.addAll(inputs);
    }

}