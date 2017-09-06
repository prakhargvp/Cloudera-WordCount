import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WordCount extends Configured implements Tool{
	public static void main(String[] args) throws Exception{
		int status=ToolRunner.run(new WordCount(), args);
		System.exit(status);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		if(arg0.length<2){
			System.out.println("Provide I/O Path");
			return -1;
		}
		JobConf conf = new JobConf(WordCount.class);
		FileInputFormat.setInputPaths(conf, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(conf, new Path(arg0[1]));
		conf.setMapperClass(WordMapper.class);
		conf.setReducerClass(WordReducer.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);
		JobClient.runJob(conf);
		return 0;
	}
}
