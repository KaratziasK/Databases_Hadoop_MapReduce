package dit.hua.gr.databases.it2022120;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text fileTag = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = context.getInputSplit().toString(); // Λήψη αρχείου εισόδου
            String tag = fileName.contains("pg46.txt") ? "file1" : "file2"; // Ταυτότητα αρχείου
            fileTag.set(tag);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", "")); // Κανονικοποίηση λέξεων
                context.write(word, fileTag);
            }
        }
    }

    public static class DifferenceReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> files = new HashSet<>();
            for (Text val : values) {
                files.add(val.toString());
            }
            if (files.contains("file1") && !files.contains("file2")) {
                result.set("unique");
                context.write(key, null);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word difference");
        job.setJarByClass(Q2.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DifferenceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
