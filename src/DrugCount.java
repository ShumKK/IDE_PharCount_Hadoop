import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DrugCount {

    public static class DrugSplit extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split("\t");

            String keyString = tokens[2].trim();
            String valueString = "1" + "\t" + tokens[3].trim();

            context.write(new Text(keyString), new Text(valueString));
        }

    }

    public static class DrugCollection extends
            Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            while(values.iterator().hasNext()) {
                String[] tokens = values.iterator().next().toString().split("\t");

                count += Integer.parseInt(tokens[0]);
                sum += Float.parseFloat(tokens[1]);
            }

            String valueString = String.valueOf(count) + "\t" + String.valueOf(sum);
            context.write(key, new Text(valueString));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "pharmacy count");
        job.setJarByClass(DrugCount.class);

        job.setMapperClass(DrugSplit.class);
        job.setCombinerClass(DrugCollection.class);
        job.setReducerClass(DrugCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}