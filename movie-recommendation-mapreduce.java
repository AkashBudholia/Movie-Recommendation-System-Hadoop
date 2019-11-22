


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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;


public class reco
{

//.................................................MAPPER-1...................................................................................

	public static class Map1 extends Mapper<LongWritable, Text,LongWritable,Text>
	{
		private String item_rating_pair="";
		private LongWritable user_id = new LongWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			item_rating_pair="";
			StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
			user_id.set(Long.parseLong(itr.nextToken()));
			item_rating_pair += itr.nextToken()+","+itr.nextToken();
			System.out.println(item_rating_pair);
			context.write(user_id,new Text(item_rating_pair));
		}
	}

//...................................................REDUCER-1................................................................................

	public static class Reduce1 extends Reducer<LongWritable,Text,LongWritable,Text>
	{
		private Text result = new Text();
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String translations = "";
			for(Text val : values)
			{
				translations += val.toString() + ",";
				//System.out.println(translations);
			}
			result.set(translations);
			System.out.println(translations);
			context.write(key, result);
		}
	}

//..................................................MAPPER-2...................................................................................

	public static class Map2 extends Mapper<LongWritable, Text, Text,Text>
	{
		private String item_pair;
		private String rating_pair;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			item_pair="";
			rating_pair="";
			String[] values=value.toString().split("\t");
			System.out.println(values.length);
			String[] split1 = values[1].toString().split(",");
			System.out.println(split1.length);
			for(int i=0; i<(split1.length) ; i=i+2)
			for(int j=0; j<(split1.length); j=j+2)
			{
				System.out.println(i);
				System.out.println(j);
				if(i<j)
				{
					item_pair="";
					rating_pair="";
					//System.out.println(hiii);
					if(split1[i].compareTo(split1[j])<0)
					{
						item_pair += split1[i]+","+split1[j];
						System.out.println(item_pair);
						rating_pair+=split1[i+1]+","+split1[j+1];
					}
					else
					{
						item_pair += split1[j]+","+split1[i];
						System.out.println(item_pair);
						rating_pair+=split1[j+1]+","+split1[i+1];
					}
					System.out.println(rating_pair);
					context.write(new Text(item_pair),new Text(rating_pair));
				}
			}
		}
	}

//.............................................REDUCER-2......................................................................................

	public static class Reduce2 extends Reducer<Text,Text,Text,Text>
	{

		private String result;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			result="";
			int sum_xx=0, sum_xy=0, sum_yy=0, sum_x=0, sum_y=0,item_x=0,item_y=0;
			int n=0;
			double similarity=0.0;
			String translations = "";
			for (Text val : values)
			{
				translations = val.toString();
				String[] rating=translations.split(",");
				System.out.println(rating.length);
				item_x=Integer.parseInt(rating[0]);
				item_y=Integer.parseInt(rating[1]);
				sum_xx += item_x * item_x;
				sum_yy += item_y * item_y;
				sum_xy += item_x * item_y;
				sum_y += item_y;
				sum_x += item_x;
				n += 1;
			}
			System.out.println(sum_xx);
			System.out.println(sum_yy);
			System.out.println(sum_xy);
			System.out.println(sum_x);
			System.out.println(sum_y);
			System.out.println(n);
			similarity = normalized_correlation(n, sum_xy, sum_x, sum_y,sum_xx, sum_yy);
			//if(similarity>0)
			//similarity=-similarity;
			System.out.println(similarity);
			System.out.println(n);
			result += Double.toString(similarity)+","+Integer.toString(n);
			System.out.println(result);
			context.write(key,new Text(result));
		}
	}

	//..............................FIND CORRELATION BETWEEN TWO VECTOR......................

	public static double normalized_correlation(int n,int sum_xy,int sum_x,int sum_y,int sum_xx,int sum_yy)
	{
		System.out.println(sum_xx);
		System.out.println(sum_yy);
		System.out.println(sum_xy);
		System.out.println(sum_x);
		System.out.println(sum_y);
		System.out.println(n);
		if(n==1)
		return 0.0;
		double simi=0.0,a,b,c,d;
		a=(n*sum_xy);
		b=(sum_x*sum_y);
		c=Math.sqrt((n*sum_xx) - (Math.pow(sum_x,2)));
		d=Math.sqrt((n*sum_yy) - (Math.pow(sum_y,2)));
		System.out.println(a);
		System.out.println(b);
		System.out.println(c);
		System.out.println(d);
		simi=(a-b)/(c*d);

		return simi;
	}

 //................MAIN METHOD............................

	public static void main(String[] args) throws Exception
	{

		String input,output;

//......................................RUNNING FIRST MAPREDUCE-1 JOB........................................................................

		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1, "reco");
		job1.setJarByClass(reco.class);
		job1.setMapperClass(Map1.class);
		//job.setCombinerClass(IntSumReducer.class);
		job1.setReducerClass(Reduce1.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1,new Path(args[0])); //...INPUT FILE PATH.........
		FileOutputFormat.setOutputPath(job1,new Path(args[1])); //.........OUTPUT FILE PATH................

		//.......WAIT FOR JOB COMPLETION..............................
		job1.waitForCompletion(true);

//......................................RUNNING SECOND MAPREDUCE-2 JOB........................................................................


		input="/user/hduser/reco/output1";       //....OUTPUT OF FIRST INPUT TO SECOND.........
		output="/user/hduser/reco/output2";      //....OUTPUT OF SECOND.................

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "reco");
		job2.setJarByClass(reco.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2,new Path(input));   //....INPUT FILE PATH....
		FileOutputFormat.setOutputPath(job2,new Path(output));  //..OUTPUT FILE PATH.........

		//.........WAIT FOR JOB COMPLETION............................
		job2.waitForCompletion(true);



	}
}
