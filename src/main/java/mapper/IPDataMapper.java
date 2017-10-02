package mapper;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import parser.IPDataParser;
import utilsClasses.Browsers;
import utilsClasses.CustomValue;

import java.io.IOException;

public class IPDataMapper extends Mapper<LongWritable, Text, Text, CustomValue> {

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, CustomValue>.Context con) throws IOException, InterruptedException {
        IPDataParser ipDataParser = new IPDataParser();
        ipDataParser.matchLine(value.toString());
        String ip = ipDataParser.getIp();
        Long total = ipDataParser.getBytes();
        //CustomValue customValue = new CustomValue(total, 1);

        String count_browsers = UserAgent.parseUserAgentString(ipDataParser.getUserAgent()).getBrowser().getGroup().getName();
        Browsers browsers = Browsers.find(count_browsers);
        if (browsers != null) {
            con.getCounter(browsers).increment(1);
        }
        if (ip != null && total != null) {
            CustomValue customValue = new CustomValue(total, 1);
            con.write(new Text(ip), customValue);
        }
    }
}



