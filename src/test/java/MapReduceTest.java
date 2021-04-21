import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    private String metrics_line = "1618917289000 1-Node1CPU 2-Node2CPU 3-Node1RAMmb 4-Node2RAMmb";
    private String test1 = "1, 1618917289305, 66\n";
    private String test2 = "1, 1618917289991, 99\n";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

        mapDriver.getConfiguration().set("time_unit", "s");
        mapDriver.getConfiguration().set("time_window", "5");
        mapDriver.getConfiguration().set("metrics_line", metrics_line);

        mapReduceDriver.getConfiguration().set("time_unit", "s");
        mapReduceDriver.getConfiguration().set("time_window", "5");
        mapReduceDriver.getConfiguration().set("metrics_line", metrics_line);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(test1))
                .withInput(new LongWritable(), new Text(test2))
                .withOutput(new Text("Node1CPU, 1618917289000, 5s"), new IntWritable(66))
                .withOutput(new Text("Node1CPU, 1618917289000, 5s"), new IntWritable(99))
                .runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver
                .withInput(new Text("Node1CPU, 1618917289000, 5s"), values)
                .withOutput(new Text("Node1CPU, 1618917289000, 5s"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver
                .withInput(new LongWritable(), new Text(test1))
                .withInput(new LongWritable(), new Text(test2))
                .withOutput(new Text("Node1CPU, 1618917289000, 5s"), new IntWritable(82))
                .runTest();
    }
}
