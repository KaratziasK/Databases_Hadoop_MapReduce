package dit.hua.gr.databases.it2022120;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        private Text fileAndCount = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filePath = ((FileSplit) context.getInputSplit()).getPath().getName();
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String token = itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z]", "");
                if (token.length() >= 4) { // Έλεγχος μήκους λέξης
                    word.set(token);
                    fileAndCount.set(filePath + "=1"); // Κωδικοποίηση αρχείου και πλήθους
                    context.write(word, fileAndCount);
                }
            }
        }
    }

    public static class CountReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> fileCounts = new HashMap<>();
            HashSet<String> fileSet = new HashSet<>();

            // Συγκέντρωση πλήθους ανά αρχείο
            for (Text val : values) {
                String[] parts = val.toString().split("=");
                String fileName = parts[0];
                int count = Integer.parseInt(parts[1]);
                fileCounts.put(fileName, fileCounts.getOrDefault(fileName, 0) + count);
                fileSet.add(fileName);
            }

            // Κριτήριο: Εμφάνιση σε τουλάχιστον 2 αρχεία
            if (fileSet.size() >= 2) {
                int countPg100 = fileCounts.getOrDefault("pg100.txt", 0);
                int countPg46 = fileCounts.getOrDefault("pg46.txt", 0);
                int countElQuijote = fileCounts.getOrDefault("el_quijote.txt", 0);
                String result = countPg100 + "\t" + countPg46 + "\t" + countElQuijote;
                context.write(key, new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count per file");
        job.setJarByClass(Q4.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
