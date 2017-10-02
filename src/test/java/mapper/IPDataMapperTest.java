package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import utilsClasses.CustomValue;

import java.io.IOException;

public class IPDataMapperTest {
    final String string = "ip37 - - [24/Apr/2011:06:18:08 -0400] \"GET /sun_ipc/IPC_led.jpg HTTP/1.0\" 200 2197 \"http://host2/sun_ipc/\" \"Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.2.16) Gecko/20110319 Firefox/3.6.16\"";


    MapDriver<LongWritable, Text, Text, CustomValue> mapDriver;

    @Before
    public void setUp() throws Exception {
        IPDataMapper mapper = new IPDataMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                string));
        CustomValue customValue = new CustomValue(2197,1);
        mapDriver.withOutput(new Text("ip37"), customValue);
        mapDriver.runTest();
    }




}
