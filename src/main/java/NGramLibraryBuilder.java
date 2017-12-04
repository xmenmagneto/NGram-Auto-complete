
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			Configuration configuration = context.getConfiguration();
			noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//read sentence by sentence
			//split sentence into ngram

			String sentence = value.toString();
			//把所有非字母的字符用空格代替
			sentence = sentence.trim().toLowerCase().replace("[^a-z]", " ");
			String[] words = sentence.split("\\s"); //split by space

			if (words.length < 2) {  //只有1个单词, 1 ngam不要
				return;
			}

			//i love big data
			//i love, love big, big data
			//write to disk等待被reducer收集并且合并
			StringBuilder phrase;
			for (int i = 0; i < words.length; i++) {
				phrase = new StringBuilder();
				phrase.append(words[i]);
				for (int j = 1; i + j < words.length && j < noGram; j++) { //j表示粘了几个
					phrase.append(" ");
					phrase.append(words[i + j]);
					context.write(new Text(phrase.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//key = big data
			//value = <1,1,1,1,1,1...>
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

}