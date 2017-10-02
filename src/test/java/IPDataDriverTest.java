package mapreduce;

import mapper.IPDataMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import reducer.IPDataReducer;
import utilsClasses.CustomValue;

import java.util.ArrayList;
import java.util.List;

public class IPDataDriverTest {
    final String string = "ip37 - - [24/Apr/2011:06:18:08 -0400] \"GET /sun_ipc/IPC_led.jpg HTTP/1.0\" 200 2197 \"http://host2/sun_ipc/\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";

    MapReduceDriver<LongWritable, Text, Text, CustomValue, Text, Text> mapReduceDriver;
    IPDataMapper mapper;
    IPDataReducer reducer;
    @Before
    public void setUp() throws Exception {
        mapper = new IPDataMapper();
        reducer = new IPDataReducer();
        mapReduceDriver = mapReduceDriver.newMapReduceDriver(mapper,reducer);
    }

    @Test
    public void mapreduceTest() throws Exception {
        mapReduceDriver.withInput(new LongWritable(), new Text(string));
        mapReduceDriver.withOutput(new Text("ip37"),new Text(2197.0+","+2197));
        mapReduceDriver.runTest();
    }
}
