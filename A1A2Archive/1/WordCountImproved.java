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
import java.util.regex.Pattern;

public class WordCountImproved extends Configured implements Tool {
public static class TokenizerMapper
extends Mapper<LongWritable, Text, Text, LongWritable>{
private final static LongWritable one = new LongWritable(1);
private Text word = new Text();

@Override
public void map(LongWritable key, Text value, Context context
) throws IOException, InterruptedException {

Pattern word_seperate = Pattern.compile("[\\p{Punct}\\s]+");
String[] seperate_words_array = word_seperate.split(value.toString());
// StringTokenizer itr = new StringTokenizer(value.toString());
for (String eachWord:seperate_words_array){
String lowercaseWord = eachWord.toLowerCase();

word.set(lowercaseWord);
context.write(word, one); }

}
}

public static void main(String[] args) throws Exception {
int result = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
System.exit(result);
}

@Override
public int run(String[] args) throws Exception {
Configuration configuration = this.getConf();
Job job = Job.getInstance(configuration, "word count");
job.setJarByClass(WordCountImproved.class);
job.setInputFormatClass(TextInputFormat.class);
job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(LongSumReducer.class);
job.setReducerClass(LongSumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(LongWritable.class);
job.setOutputFormatClass(TextOutputFormat.class);
TextInputFormat.addInputPath(job, new Path(args[0]));
TextOutputFormat.setOutputPath(job, new Path(args[1]));
return job.waitForCompletion(true) ? 0 : 1;
}
}
