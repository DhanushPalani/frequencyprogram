# Ex.No.5. Implement word count/frequency programs using MapReduce
## Aim
To implement a word count program using the MapReduce framework.

## Procedure

1.Set up a Hadoop MapReduce environment.
2.Write a Mapper class to map words to intermediate key-value pairs.
3.Write a Reducer class to aggregate the counts of each word.
4.Compile, package, and execute the program using the Hadoop framework.
## Code/Steps
## Mapper Code
```
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import java.io.IOException;

public class WCMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter rep) throws IOException {
        String line = value.toString();
        for (String word : line.split(" ")) {
            output.collect(new Text(word), new IntWritable(1));
        }
    }
}
```
## Reducer Code:
```
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class WCReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter rep) throws IOException {
        int count = 0;
        while (values.hasNext()) {
            count += values.next().get();
        }
        output.collect(key, new IntWritable(count));
    }
}
```
## Driver Code:
```
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

public class WCDriver extends Configured implements Tool {
    public int run(String args[]) throws Exception {
        JobConf conf = new JobConf(WCDriver.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setMapperClass(WCMapper.class);
        conf.setReducerClass(WCReducer.class);
        return JobClient.runJob(conf).waitForCompletion() ? 0 : 1;
    }
}
```
## Result
Successfully implemented a word count program using MapReduce.

