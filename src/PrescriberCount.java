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

public class PrescriberCount {
    public static class DrugSplit extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");

            if (tokens[0].equals("id")) {
                return;
            }

            if (tokens.length == 5) {

                // value: DrugCost
                String drugCost = tokens[4].trim();

                try {
                    Float.parseFloat(drugCost);
                } catch (Exception e) {
                    return;
                }

                // key: LName + FName + DrugCost
                String keyString = tokens[1].trim().toUpperCase() + "\t" +
                        tokens[2].trim().toUpperCase() + "\t" +
                        tokens[3].trim().toUpperCase();

                context.write(new Text(keyString), new Text(drugCost));
            }
        }

    }

    public static class PrescriberCollection extends
            Reducer<Text, Text, Text, Text> {

        private Text costSum = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float sum = 0;

            while(values.iterator().hasNext()) {
                sum += Float.parseFloat(values.iterator().next().toString());
            }

            costSum.set(String.valueOf(sum));
            context.write(key, costSum);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "prescriber count");
        job.setJarByClass(PrescriberCount.class);
        job.setMapperClass(DrugSplit.class);
        job.setCombinerClass(PrescriberCollection.class);
        job.setReducerClass(PrescriberCollection.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
