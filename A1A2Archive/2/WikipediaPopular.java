import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool{
    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable>{   
    private final static LongWritable sum = new LongWritable(1L);
    private Text ts = new Text();

@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

StringTokenizer splitted = new StringTokenizer(value.toString());

    String date = splitted.nextToken();
    String language = splitted.nextToken();
    String title = splitted.nextToken();
    String viewer= splitted.nextToken();

    if((language.equals("en")) && (!title.equals("Main_Page")) && (!title.startsWith("Special:"))){
        ts.set(date);
        sum.set(Long.valueOf(viewer));
        context.write(ts,sum);
    }
}
}

public static class MaximumReducer
extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable result = new LongWritable();

@Override
public void reduce(Text date, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
long max = 0L;
for (LongWritable val : values) {
long long_val = val.get();
if(max < long_val) {
max = long_val;
}
}
result.set(max);
context.write(date, result);
}}

public static void main(String[] args) throws Exception {
int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
System.exit(res);
}
@Override
public int run(String[] args) throws Exception {
Configuration conf = this.getConf();
Job job = Job.getInstance(conf, "Wiki popular");
job.setJarByClass(WikipediaPopular.class);
job.setInputFormatClass(TextInputFormat.class);
job.setMapperClass(TokenizerMapper.class);
job.setReducerClass(MaximumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(LongWritable.class);
job.setOutputFormatClass(TextOutputFormat.class);
TextInputFormat.addInputPath(job, new Path(args[0]));
TextOutputFormat.setOutputPath(job, new Path(args[1]));
return job.waitForCompletion(true) ? 0 : 1;
}}