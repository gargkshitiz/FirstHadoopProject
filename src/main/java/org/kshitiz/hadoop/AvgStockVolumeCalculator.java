package org.kshitiz.hadoop;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvgStockVolumeCalculator{

	public static class AvgStockVolumeMapper extends
			Mapper<Object, Text, Text, DoubleWritable> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			if(((LongWritable)key).get()==0){
				return;
			}
			String[] values = value.toString().split("\t");
			Text stockSymbol = new Text(values[1]);
			DoubleWritable stockVolume = new DoubleWritable(Long.valueOf(values[7]).longValue());
			context.write(stockSymbol, stockVolume);
		}
	}

	public static class AvgStockVolumeReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable avgStockVolume = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0d;
			double noOfValuesWithAKey = 1d;
			for (DoubleWritable val : values) {
				sum += val.get();
				noOfValuesWithAKey++;
			}
			avgStockVolume.set(sum/noOfValuesWithAKey);
			context.write(key, avgStockVolume);
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "word count");
		job.setJarByClass(AvgStockVolumeCalculator.class);
		job.setMapperClass(AvgStockVolumeMapper.class);
		//job.setCombinerClass(AvgStockVolumeReducer.class);
		job.setReducerClass(AvgStockVolumeReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,
				new Path(args[1] + "/" + UUID.randomUUID()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}