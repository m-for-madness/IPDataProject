package combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import utilsClasses.CustomValue;

import java.util.ArrayList;
import java.util.List;

public class IPDataCombinerTest {
    ReduceDriver<Text, CustomValue, Text, CustomValue> reduceDriver;

    @Before
    public void setUp() throws Exception {
        IPDataCombiner reduceIPData = new IPDataCombiner();
        reduceDriver = ReduceDriver.newReduceDriver(reduceIPData);
    }

    @Test
    public void reduceTest() throws Exception {
        List<CustomValue> values = new ArrayList<>();
        values.add(new CustomValue(300,1));
        values.add(new CustomValue(700,3));
        values.add(new CustomValue(2560,9));
        values.add(new CustomValue(600,5));

        reduceDriver.withInput(new Text("ip3"), values).withOutput( new Text("ip9"), values.get(0))
                .withOutput( new Text("ip317"), values.get(1))
                .withOutput(new Text("ip37"), values.get(2))
                .withOutput(new Text("ip17"), values.get(3));
    }
}
