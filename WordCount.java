import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class GuildListMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text user = new Text();
    private Text guild = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
      StringTokenizer token;
      while (lines.hasMoreTokens()) {
        token = new StringTokenizer(lines.nextToken());
        user.set(token.nextToken());
        guild.set(token.nextToken());
        if (!guild.toString().equals("N/A")) { context.write(guild, user); }
      }
    }
  }

  public static class GuildListReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String users = "";
      for (Text val : values) {
        users += val.toString() + " ";
      }
      result.set(users);
      context.write(key, result);
    }
  }

  public static class FriendSymmetryMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text user = new Text();
    private Text guild = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer lines = new StringTokenizer(value.toString(), "\n");
      StringTokenizer token;
      while (lines.hasMoreTokens()) {
        token = new StringTokenizer(lines.nextToken());
        user.set(token.nextToken());
        guild.set(token.nextToken());
        if (!guild.toString().equals("N/A")) { context.write(guild, user); }
      }
    }
  }

  public static class FriendSymmetryReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String users = "";
      for (Text val : values) {
        users += val.toString() + " ";
      }
      result.set(users);
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    if (args.length != 3 || (!args[0].equals("T1") && !args[0].equals("T2"))) {
      System.out.println ("usage: hadoop jar [jarfile] WordCount [T1/T2] [input file] [output file]");
      return;
    }
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    if (args[0].equals("T1")) {
      job.setMapperClass(GuildListMapper.class);
      job.setCombinerClass(GuildListReducer.class);
      job.setReducerClass(GuildListReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
    } else {
      job.setMapperClass(FriendSymmetryMapper.class);
      job.setCombinerClass(FriendSymmetryReducer.class);
      job.setReducerClass(FriendSymmetryReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
    }
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
