
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

    //Specification of Mapper
    MapDriver<LongWritable, Text, Text, Text> mapDriver1;
    MapDriver<LongWritable, Text, Text, Text> mapDriver2;

    //Specification of Reduce
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    //Specification of MapReduce program
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver1;
    //MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver2;

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

        //Setup MapReduce 1 job
        mapReduceDriver1 = MapReduceDriver.newMapReduceDriver(transitionMapper, reducer);
        //Setup MapReduce 2 job
        //mapReduceDriver2 = MapReduceDriver.newMapReduceDriver(prMapper, reducer);
    }


    @Test
    public void transitionMapperTest() throws InterruptedException {

        String transitionRowA = "a\tb,c,d";
        /*
        String transitionRowB = "b\ta,d";
        String transitionRowC = "c\ta";
        String transitionRowD = "d\tb,c";
        */

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
    public void prMapperTest() throws InterruptedException {

        String prMatrixRow1 = "a\t0.25";
        mapDriver2.withInput(new LongWritable(0), new Text(prMatrixRow1));

        mapDriver2.withOutput(new Text("a"), new Text("0.25"));

        try {
            mapDriver2.runTest();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void reducerTest() throws InterruptedException {

        /* Mapper:
        String transitionRowA = "a\tb,c,d";
        String prMatrixRow1 = "a\t0.25";
        */
        double probability = (double) 1/3;
        double pagerank = 0.25;

        List<Text> values = new ArrayList<Text>();
        values.add(new Text("b=" + probability));
        values.add(new Text("c=" + probability));
        values.add(new Text("d=" + probability));
        values.add(new Text(String.valueOf(pagerank)));

        List<Pair<Text, Text>> output = new ArrayList<Pair<Text, Text>>();
        output.add(new Pair<Text, Text>(new Text("b"), new Text(String.valueOf(probability * pagerank))));
        output.add(new Pair<Text, Text>(new Text("c"), new Text(String.valueOf(probability * pagerank))));
        output.add(new Pair<Text, Text>(new Text("d"), new Text(String.valueOf(probability * pagerank))));

        reduceDriver.withInput(new Text("a"), values);

        reduceDriver.withAllOutput(output);
        try {
            reduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test   // test MapReduce pipeline
    public void mapReducerMultipleTest() throws IOException, InterruptedException {

        String transitionRowA = "a\tb,c,d";
        String prMatrixRow1 = "a\t0.25";
        double probabilityA  = (double) 0;

        /*
        Output 的顺序是随机的 不是按照 我们的逻辑
         */
        mapReduceDriver1.withInput(new LongWritable(), new Text(transitionRowA))
                .withOutput(new Text("b"), new Text(String.valueOf(probabilityA)))
                .withOutput(new Text("c"), new Text(String.valueOf(probabilityA)))
                .withOutput(new Text("d"), new Text(String.valueOf(probabilityA)));


        try {
            mapReduceDriver1.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}