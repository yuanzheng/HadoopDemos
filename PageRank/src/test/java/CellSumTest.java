import org.apache.hadoop.io.DoubleWritable;
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

import static org.junit.Assert.*;

public class CellSumTest {

    //Specification of Mapper
    MapDriver<LongWritable, Text, Text, DoubleWritable> mapDriver;

    //Specification of Reduce
    ReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reduceDriver;

    //Specification of MapReduce program
    MapReduceDriver<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> mapReduceDriver;

    @Before
    public void setUp() throws Exception {
        //Setup Mapper
        CellSum.PassMapper passMapper = new CellSum.PassMapper();

        mapDriver = MapDriver.newMapDriver(passMapper);

        //Setup Reduce
        CellSum.SumReducer reducer = new CellSum.SumReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        //Setup MapReduce job
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(passMapper, reducer);

    }


    @Test
    public void PassMapperTest() throws IOException, InterruptedException {

        String message = "b\t0.08333333333333333";
        mapDriver.withInput(new LongWritable(0), new Text(message));

        String[] data = message.split("\t");
        mapDriver.withOutput(new Text(data[0]), new DoubleWritable(Double.parseDouble(data[1])));
        try {
            mapDriver.runTest();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void SumReducerTest() throws InterruptedException {

        double pageRankA = 0.25;
        double pageRankD = 0.25;
        double probabilityA = (double) 1/3;
        double probabilityD = (double) 1/2;
        List<DoubleWritable> values = new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(probabilityA * pageRankA));
        values.add(new DoubleWritable(probabilityD * pageRankD));

        double sum = probabilityA * pageRankA + probabilityD * pageRankD;

        reduceDriver.withInput(new Text("b"), values);
        reduceDriver.withOutput(new Text("b"), new DoubleWritable(sum));

        try {
            reduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mapreduceCellSumTest() throws InterruptedException {

        double pageRankA = 0.25;
        double pageRankD = 0.25;
        double probabilityA = (double) 1/3;
        double probabilityD = (double) 1/2;

        String message1 = "b\t" + pageRankA * probabilityA;
        String message2 = "b\t" + pageRankD * probabilityD;
        double sum = probabilityA * pageRankA + probabilityD * pageRankD;

        List<Pair<LongWritable, Text>> input = new ArrayList<Pair<LongWritable, Text>>();
        input.add(new Pair<LongWritable, Text>(new LongWritable(), new Text(message1)));
        input.add(new Pair<LongWritable, Text>(new LongWritable(), new Text(message2)));

        mapReduceDriver.addAll(input);

        mapReduceDriver.withOutput(new Text("b"), new DoubleWritable(sum));

        try {
            mapReduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}