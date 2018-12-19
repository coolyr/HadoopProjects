package cn.pku.coolyr.sort;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Sort extends Configured implements Tool
{
	public int run(String[] args) throws Exception
	{
		String inputPath = args[0];
		String outputPath = args[1];

		Configuration conf = new Configuration();
		Job job = null;

		// 根据不同参数组装Mapper和Reducer
		// assembleJob(job, conf, args);

		int length = args.length;
		if (2 == length)
		{
			// 参数格式：inputPath outputPath
			// 功能： 默认按id排序
			job = Job.getInstance(conf);
			job.setMapperClass(MyMapperID.class);
			job.setReducerClass(MyReducerID.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
		}
		else if (3 == length)
		{
			if (!args[2].startsWith("["))
			{
				// 参数格式：inpath outpath columnID
				// 功能： 按输入的列id进行排序，如果该列的值相同再按id排序
				System.out.println("**************************************");
				System.out.println(args[2]);
				System.out.println("**************************************");
				conf.set("columnID", args[2]);// conf.set()必须在job实例化之前!!!!!
				System.out.println("**************************************");
				System.out.println(conf.get("columnID"));
				System.out.println("**************************************");
				job = Job.getInstance(conf);// 实例化job，需要传入的参数必须已经传入了conf
				job.setMapperClass(MyMapperColumnID.class);
				job.setReducerClass(MyReducerColumnID.class);
				job.setMapOutputKeyClass(SortBean.class);
				job.setMapOutputValueClass(Text.class);
			}
			else
			{
				// 参数格式：inpath outpath [column0,column1...]
				// 功能： 按输入的列id进行排序，权重依次递减，如果列的值都相同再按id排序
				System.out.println("**************************************");
				System.out.println(args[2]);
				System.out.println("**************************************");
				String agrsIDs = args[2].substring(args[2].indexOf("[") + 1, args[2].indexOf("]"));
				conf.set("columnIDs", agrsIDs);// conf.set()必须在job实例化之前！！！
				System.out.println("**************************************");
				System.out.println(conf.get("columnIDs"));
				System.out.println("**************************************");
				job = Job.getInstance(conf);
				job.setMapperClass(MyMapperColumnIDs.class);
				job.setReducerClass(MyReducerColumnID.class);
				job.setMapOutputKeyClass(SortBean.class);
				job.setMapOutputValueClass(Text.class);

			}
		}
		job.setJobName(Sort.class.getSimpleName());
		job.setJarByClass(Sort.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * 组装job
	 * 
	 * @param job
	 * @param conf
	 * @param args
	 * @throws IOException
	 */
	public static void assembleJob(Job job, Configuration conf, String[] args) throws IOException
	{
		int length = args.length;
		if (2 == length)
		{
			job = Job.getInstance(conf);
			job.setMapperClass(MyMapperID.class);
			job.setReducerClass(MyReducerID.class);
		}
		else if (3 == length)
		{
			// ...
		}
	}

	public static void main(String[] args) throws Exception
	{
		// 参数格式
		// inpath outpath [column0,column1..]
		ToolRunner.run(new Sort(), args);
	}

	// #################################################################################
	// #################################################################################
	/**
	 * 只输入inpath outpath 默认按id排序
	 * 
	 * @author Administrator
	 */
	static class MyMapperID extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text k = new Text();
		private Text v = new Text();

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{
			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			String contents = line.substring(line.indexOf("\t")).trim();
			k.set(id);
			v.set(contents);
			context.write(k, v);
		};
	}

	static class MyReducerID extends Reducer<Text, Text, Text, Text>
	{
		private Text v = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException
		{
			for (Text line : values)
			{
				v.set(line.toString().trim());
				context.write(key, v);
			}
		};
	}

	// #################################################################################
	// #################################################################################
	/**
	 * 参数格式：inpath outpath columnID 功能： 按输入的列id进行排序，如果该列的值相同再按id排序
	 * 
	 * @author Administrator
	 */
	static class MyMapperColumnID extends Mapper<LongWritable, Text, SortBean, Text>
	{
		private SortBean kBean = new SortBean();
		private Text v = new Text();
		private int columnID;
		private double field1 = 0;

		protected void setup(Mapper<LongWritable, Text, SortBean, Text>.Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration configuration = context.getConfiguration();
			columnID = Integer.parseInt(configuration.get("columnID", "-1").trim());
		};

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{

			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			String contents = line.substring(line.indexOf("\t")).trim();
			String[] fields = contents.split("\t");
			if (fields.length >= columnID && columnID > 0)
				field1 = Double.parseDouble(fields[columnID - 1].trim());
			kBean.set(id, field1, Double.MIN_VALUE);
			v.set(contents);
			context.write(kBean, v);

		}

	}

	static class MyReducerColumnID extends Reducer<SortBean, Text, Text, Text>
	{
		private Text k = new Text();
		private Text v = new Text();

		protected void reduce(SortBean kBean, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException
		{
			k.set(kBean.getId());
			for (Text line : values)
			{

				v.set(line.toString().trim());
				context.write(k, v);
			}
		};
	}

	// #################################################################################
	// #################################################################################
	/**
	 * 参数格式：inpath outpath [column0,column1...] 功能： 按输入的列id进行排序，权重依次递减，如果列的值都相同再按id排序
	 * 
	 * @author Administrator
	 */
	static class MyMapperColumnIDs extends Mapper<LongWritable, Text, SortBean, Text>
	{
		private SortBean kBean = new SortBean();
		private Text v = new Text();
		private ArrayList<Integer> idArrayList = new ArrayList<Integer>();
		private ArrayList<Double> columnArrayList = new ArrayList<Double>();

		protected void setup(Mapper<LongWritable, Text, SortBean, Text>.Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration configuration = context.getConfiguration();
			String columnIDsString = configuration.get("columnIDs", "-1").trim();
			String[] ids = columnIDsString.split(",");
			for (int i = 0; i < ids.length; ++i)
			{
				idArrayList.add(Integer.parseInt(ids[i].trim()));
				columnArrayList.add((double) 0);// 初始化
			}
		};

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{

			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			String contents = line.substring(line.indexOf("\t")).trim();
			String[] fields = contents.split("\t");

			for (int i = 0; i < idArrayList.size(); i++)
			{
				if (fields.length >= idArrayList.get(i) && idArrayList.get(i) > 0)
					// 注意使用set重置，非add添加!!!
					columnArrayList.set(i, Double.parseDouble(fields[idArrayList.get(i) - 1]));
			}
			if (columnArrayList.size() >= 2)
				kBean.set(id, columnArrayList.get(0), columnArrayList.get(1));
			else
				kBean.set(id, columnArrayList.get(0), Double.MIN_VALUE);

			v.set(contents);
			context.write(kBean, v);

		}
	}
}
