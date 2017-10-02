import combiner.IPDataCombiner;
import mapper.IPDataMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import parser.IPDataParser;
import reducer.IPDataReducer;
import utilsClasses.CustomValue;

import java.io.IOException;

public class IPDataDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        c.set("mapred.textoutputformat.separatorText", ",");
        //c.set("mapred.compress.map.output","true");
       // c.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Job j = new Job(c, "IPDataProject");
        j.setJarByClass(IPDataDriver.class);
        j.setMapperClass(IPDataMapper.class);
        j.setCombinerClass(IPDataCombiner.class);
        j.setReducerClass(IPDataReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CustomValue.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);
        // j.setOutputFormatClass(SequenceFileOutputFormat.class);
        // SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        // SequenceFileOutputFormat.setCompressOutput(job, true);
        j.setNumReduceTasks(1);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

}
