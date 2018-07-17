import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OrderByCost {

    public static class InverseDrugCostMapper
            extends Mapper<LongWritable, Text, FloatWritable, Text> {

        private FloatWritable costSum = new FloatWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");

            costSum.set(Float.parseFloat(tokens[2].trim()));

            context.write(costSum, new Text(tokens[0] + "\t" + tokens[1]));
        }
    }

    public static class DescFloatComparator extends WritableComparator {

        public DescFloatComparator() {
            super(FloatWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            FloatWritable key1 = (FloatWritable) w1;
            FloatWritable key2 = (FloatWritable) w2;

            return -1 * key1.compareTo(key2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Order by Cost Desc");
        job.setJarByClass(OrderByCost.class);

        job.setMapperClass(InverseDrugCostMapper.class);
        job.setSortComparatorClass(DescFloatComparator.class);

        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
