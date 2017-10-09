import combiner.IPDataCombiner;
import mapper.IPDataMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parser.IPDataParser;
import reducer.IPDataReducer;
import scala.util.regexp.Base;
import utilsClasses.CustomValue;
import utilsClasses.CustomValueComparator;
import utilsClasses.KeyPartitioner;

import java.io.IOException;

public class IPDataDriver extends Configured implements Tool {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        try {
            int status = ToolRunner.run(new IPDataDriver(), args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        String type_of_file;
        // by default it will be printed as sequence file
        if (args.length == 2) {
            type_of_file = "seq";
        } else if (args.length > 3 || args.length < 2) {
            System.err.printf("Usage: %s [generic options] <input> <output> <type_of_file>\n", new String("IPDataDriver.class"));
        }
        type_of_file = args[2];
        Configuration c = new Configuration();

        if (type_of_file.equals("csv"))
            c.set("mapred.textoutputformat.separator", ",");
        //paths
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        Job j = new Job(c, "IPDataProject");

        j.setJarByClass(IPDataDriver.class);
        // Raw Comparator
        j.setSortComparatorClass(CustomValueComparator.class);


        j.setMapperClass(IPDataMapper.class);
        j.setCombinerClass(IPDataCombiner.class);
        j.setReducerClass(IPDataReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CustomValue.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);


        j.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(j, input);

        if (type_of_file.equals("seq")) {
            j.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(j, output);

        } else if (type_of_file.equals("csv")) {
            j.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(j, output);
        } else if (type_of_file.equals("txt")) {
            j.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(j, output);
        }

        return j.waitForCompletion(true) ? 0 : 1;
    }
}
