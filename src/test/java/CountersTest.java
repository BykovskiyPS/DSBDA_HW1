import eu.bitwalker.useragentutils.UserAgent;
import bdtc.lab1.CounterType;
import bdtc.lab1.HW1Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;


public class CountersTest {

    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    private String metrics_line = "1618917289000 1-Node1CPU 2-Node2CPU 3-Node1RAMmb 4-Node2RAMmb";
    private final String testBrokenLine = "unknown str%!ing 12!%)31\n";
    private String validLine = "1, 1618917289305, 66\n";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        mapDriver.getConfiguration().set("time_unit", "s");
        mapDriver.getConfiguration().set("time_window", "5");
        mapDriver.getConfiguration().set("metrics_line", metrics_line);
    }

    @Test
    public void testMapperCounterOne() throws IOException  {
        mapDriver
                .withInput(new LongWritable(), new Text(testBrokenLine))
                .runTest();
        assertEquals("Expected 1 counter increment", 1, mapDriver.getCounters()
                .findCounter(CounterType.MALFORMED).getValue());
    }

//    @Test
//    public void testMapperCounterZero() throws IOException {
//        UserAgent userAgent = UserAgent.parseUserAgentString(validLine);
//        mapDriver
//                .withInput(new LongWritable(), new Text(validLine))
//                .withOutput(new Text(userAgent.getBrowser().getName()), new IntWritable(1))
//                .runTest();
//        assertEquals("Expected 1 counter increment", 0, mapDriver.getCounters()
//                .findCounter(CounterType.MALFORMED).getValue());
//    }
//
//    @Test
//    public void testMapperCounters() throws IOException {
//        UserAgent userAgent = UserAgent.parseUserAgentString(testIP);
//        mapDriver
//                .withInput(new LongWritable(), new Text(testIP))
//                .withInput(new LongWritable(), new Text(testMalformedIP))
//                .withInput(new LongWritable(), new Text(testMalformedIP))
//                .withOutput(new Text(userAgent.getBrowser().getName()), new IntWritable(1))
//                .runTest();
//
//        assertEquals("Expected 2 counter increment", 2, mapDriver.getCounters()
//                .findCounter(CounterType.MALFORMED).getValue());
//    }
}

