package mapred;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Trend {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "Statistiche");
		job.setJarByClass(Trend.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TrendMapper1.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TrendMapper2.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(TrendReducer.class);
		job.setNumReduceTasks(1);
		
		job.waitForCompletion(true);
		
	}

}
