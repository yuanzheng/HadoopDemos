
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

import static org.junit.Assert.*;

public class CellMultiplicationTest {

    String transitionRowA = "a\tb,c,d";
    String transitionRowB = "b\ta,d";
    String transitionRowC = "c\ta";
    String transitionRowD = "d\tb,c";

    //Specification of Mapper
    MapDriver<LongWritable, Text, Text, Text> mapDriver1;
    MapDriver<LongWritable, Text, Text, Text> mapDriver2;

    //Specification of Reduce
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() throws Exception {

        //Setup Mapper
        CellMultiplication.TransitionMapper transitionMapper = new CellMultiplication.TransitionMapper();
        CellMultiplication.PRMapper prMapper = new CellMultiplication.PRMapper();

        mapDriver1 = MapDriver.newMapDriver(transitionMapper);
        mapDriver2 = MapDriver.newMapDriver(prMapper);

        //Setup Reduce
        CellMultiplication.MultiplicationReducer reducer = new CellMultiplication.MultiplicationReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

    }


    @Test
    public void transitionMapperTest() throws IOException, InterruptedException {

        mapDriver1.withInput(new LongWritable(0), new Text(transitionRowA));

        List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
        double probabilityA  = (double) 1/3;
        /*
        double probabilityB  = (double) 1/2;
        double probabilityC  = (double) 1;
        double probabilityD  = (double) 1/2;
        */
        output.add(new Pair<Text, Text>(new Text("a"), new Text("b=" + probabilityA)));
        output.add(new Pair<Text, Text>(new Text("a"), new Text("c=" + probabilityA)));
        output.add(new Pair<Text, Text>(new Text("a"), new Text("d=" + probabilityA)));

        /*
        output.add(new Pair<Text, Text>(new Text("b"), new Text("a=" + probabilityB)));
        output.add(new Pair<Text, Text>(new Text("b"), new Text("d=" + probabilityB)));
        output.add(new Pair<Text, Text>(new Text("c"), new Text("a=" + probabilityC)));
        output.add(new Pair<Text, Text>(new Text("d"), new Text("b=" + probabilityD)));
        output.add(new Pair<Text, Text>(new Text("d"), new Text("c=" + probabilityD)));
        */
        mapDriver1.withAllOutput(output);

        try {
            mapDriver1.runTest();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void prMapperTest() throws IOException, InterruptedException {

    }

    @Test
    public void reducerTest() throws IOException, InterruptedException {

    }


}