package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import utilsClasses.CustomValue;

import java.util.ArrayList;
import java.util.List;

public class IPDataReducerTest {

    ReduceDriver<Text, CustomValue, Text, Text> reduceDriver;

    @Before
    public void setUp() throws Exception {
        IPDataReducer reduceForWords = new IPDataReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reduceForWords);
    }

    @Test
    public void reduceTest() throws Exception {
        List<CustomValue> values = new ArrayList<>();
        values.add(new CustomValue(300,1));
        values.add(new CustomValue(600,4));
        Text ip = new Text("ip37");

        reduceDriver.withInput(ip, values);
        reduceDriver.withOutput(ip, new Text(180.0+","+900));
        reduceDriver.runTest();
    }
}
