package cn.pku.coolyr.sum;

import java.io.IOException;
import java.util.HashMap;

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

public class Sum extends Configured implements Tool
{
	public int run(String[] args) throws Exception
	{
		String inputPath = args[0];
		String outputPath = args[1];

		Configuration conf = new Configuration();
		Job job = null;

		// ���ݲ�ͬ������װMapper��Reducer
		// assembleJob(job, conf, args);

		int length = args.length;
		if (2 == length)
		{
			// ������ʽ��inputPath outputPath
			// ���ܣ� ͳ�����в�ͬid����
			job = Job.getInstance(conf);
			job.setMapperClass(MyMapperAll.class);
			job.setReducerClass(MyReducer.class);
		}
		else if (3 == length)
		{
			if (!args[2].startsWith("["))
			{
				// ������ʽ��inpath outpath id
				// ���ܣ� ͳ�������id�������е�����
				System.out.println("**************************************");
				System.out.println(args[2]);
				System.out.println("**************************************");
				conf.set("id", args[2]);// conf.set()������jobʵ����֮ǰ!!!!!
				System.out.println("**************************************");
				System.out.println(conf.get("id"));
				System.out.println("**************************************");
				job = Job.getInstance(conf);// ʵ����job����Ҫ����Ĳ��������Ѿ�������conf
				job.setMapperClass(MyMapperId.class);
				job.setReducerClass(MyReducer.class);
			}
			else
			{
				// ������ʽ��inpath outpath [id0,id1,id2...]
				// ���ܣ� ͳ�������[id0,id1,id2...]�������е�����
				System.out.println("**************************************");
				System.out.println(args[2]);
				System.out.println("**************************************");
				String agrsIDs = args[2].substring(args[2].indexOf("[") + 1, args[2].indexOf("]"));
				conf.set("IDs", agrsIDs);// conf.set()������jobʵ����֮ǰ������
				System.out.println("**************************************");
				System.out.println(conf.get("IDs"));
				System.out.println("**************************************");
				job = Job.getInstance(conf);
				job.setMapperClass(MyMapperIDs.class);
				job.setReducerClass(MyReducer.class);

			}
		}
		else if (4 <= length)
		{
			// ������ʽ:inpath outpath id [column0,column1..]
			// ���ܣ� ͳ�������id��[column0,column1..]�е���
			if (!args[2].startsWith("["))
			{

				System.out.println("**************************************");
				System.out.println(args[2]);
				System.out.println("**************************************");
				conf.set("id", args[2]);// conf.set()������jobʵ����֮ǰ������
				System.out.println("**************************************");
				System.out.println(conf.get("id"));
				System.out.println("**************************************");

				System.out.println("**************************************");
				System.out.println(args[3]);
				System.out.println("**************************************");
				String columnIDs = args[3].substring(args[3].indexOf("[") + 1, args[3].indexOf("]"));
				conf.set("columnIDs", columnIDs);// conf.set()������jobʵ����֮ǰ������
				System.out.println("**************************************");
				System.out.println(conf.get("columnIDs"));
				System.out.println("**************************************");

				job = Job.getInstance(conf);
				job.setMapperClass(MyMapperIdColumns.class);
				job.setReducerClass(MyReducer.class);
			}
			else
			{
				// ������ʽ��(1) inpath outpath [id0,id1,id2...] [column0,column1..]
				// 			(2) inpath outpath [*] [column0,column1..]
				// ���ܣ� (1) �������[id0,id1,id2...]�������[column0,column1..]�еĺ�
				// ���ܣ� (2) �����е������[column0,column1..]�еĺ�
				System.out.println("**************************************");
				System.out.println(args[2]);
				System.out.println("**************************************");
				String agrsIDs = args[2].substring(args[2].indexOf("[") + 1, args[2].indexOf("]"));
				conf.set("IDs", agrsIDs.trim());// conf.set()������jobʵ����֮ǰ������
				System.out.println("**************************************");
				System.out.println(conf.get("IDs"));
				System.out.println("**************************************");

				System.out.println("**************************************");
				System.out.println(args[3]);
				System.out.println("**************************************");
				String columnIDs = args[3].substring(args[3].indexOf("[") + 1, args[3].indexOf("]"));
				conf.set("columnIDs", columnIDs);// conf.set()������jobʵ����֮ǰ������
				System.out.println("**************************************");
				System.out.println(conf.get("columnIDs"));
				System.out.println("**************************************");

				job = Job.getInstance(conf);
				if ("*".equals(conf.get("IDs")))
					job.setMapperClass(MyMapperColumns.class);
				else
					job.setMapperClass(MyMapperIDsColumns.class);
				job.setReducerClass(MyReducer.class);

			}
		}

		job.setJobName(Sum.class.getSimpleName());
		job.setJarByClass(Sum.class);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);

		return 0;
	}

	/**
	 * ��װjob
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
			job.setMapperClass(MyMapperAll.class);
			job.setReducerClass(MyReducer.class);
		}
		else if (3 == length)
		{
			// ...
		}
	}

	public static void main(String[] args) throws Exception
	{
		// ������ʽ
		// inpath outpath [id] [column][...]
		ToolRunner.run(new Sum(), args);
	}

	// #################################################################################
	// #################################################################################
	/**
	 * ֻ����inpath outpath ͳ���������id�����е���
	 * 
	 * @author Administrator
	 */
	static class MyMapperAll extends Mapper<LongWritable, Text, Text, Text>
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

	static class MyReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text v = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, InterruptedException
		{
			HashMap<Integer, Double> mapSum = new HashMap<Integer, Double>();
			int index;
			Boolean isFirst = true;
			for (Text line : values)
			{
				index = 0;
				String[] fields = line.toString().split("\t");
				if (isFirst)// �����һ������ʱ���������ݵ�������ʼ��HashMap
				{
					// map���ʼ��Ϊnull,��0
					for (int i = 0; i < fields.length; ++i)
						mapSum.put(i, (double) 0);
					isFirst = false;
				}
				for (String f : fields)
				{
					mapSum.put(index, mapSum.get(index) + Double.parseDouble(f.trim()));
					index++;
				}
			}
		
			StringBuilder stringBuilder = new StringBuilder();
			for(Double num : mapSum.values())
			{
				stringBuilder.append(String.valueOf(num) + '\t');
			}
			v.set(stringBuilder.toString().trim());
			context.write(key, v);
		};
	}

	// #################################################################################
	// #################################################################################
	/**
	 * ����inpath outpath id ͳ���������id�����е���
	 * 
	 * @author Administrator
	 */
	static class MyMapperId extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text k = new Text();
		private Text v = new Text();
		private String sumID = null;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration configuration = context.getConfiguration();
			sumID = configuration.get("id", "100").trim();
		};

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{

			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			// ������Ҫͳ�Ƶ�idʱ������������¼
			if (!(sumID).equals(id))
				return;
			String contents = line.substring(line.indexOf("\t")).trim();
			k.set(id);
			v.set(contents);
			context.write(k, v);
		}

	}

	// #################################################################################
	// #################################################################################
	/**
	 * ����inpath outpath [id0,id1,id2...] ͳ�����������id�����е���
	 * 
	 * @author Administrator
	 */
	static class MyMapperIDs extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text k = new Text();
		private Text v = new Text();
		private HashMap<String, Boolean> IDsMap = new HashMap<String, Boolean>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.setup(context);
			Configuration configuration = context.getConfiguration();
			String idString = configuration.get("IDs", "100");
			String[] ids = idString.split(",");
			for (int i = 0; i < ids.length; ++i)
			{
				IDsMap.put(ids[i].trim(), true);
			}
		};

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{

			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			// ������Ҫͳ�Ƶ�idʱ������������¼
			if (!IDsMap.containsKey(id))
				return;
			String contents = line.substring(line.indexOf("\t")).trim();
			k.set(id);
			v.set(contents);
			context.write(k, v);
		}
	}

	// #################################################################################
	// #################################################################################
	/**
	 * ����inpath outpath id [column,...]ͳ���������id������Ķ����
	 * 
	 * @author Administrator
	 */
	static class MyMapperIdColumns extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text k = new Text();
		private Text v = new Text();
		private String sumID = null;
		private HashMap<Integer, Boolean> columnsIDMap = new HashMap<Integer, Boolean>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.setup(context);
			Configuration configuration = context.getConfiguration();
			sumID = configuration.get("id", "100").trim();
			String columnString = configuration.get("columnIDs");
			String[] columnsID = columnString.split(",");
			for (int i = 0; i < columnsID.length; ++i)
			{
				columnsIDMap.put(Integer.parseInt(columnsID[i].trim()), true);
			}
		};

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{
			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			// ������Ҫͳ�Ƶ�idʱ������������¼
			if (!(sumID).equals(id))
				return;
			String contents = line.substring(line.indexOf("\t")).trim();
			String[] columns = contents.split("\t");
					
			StringBuilder stringBuilder = new StringBuilder();
			for(int i=0; i<columns.length; ++i)
			{
				if (columnsIDMap.containsKey(i + 1))
					stringBuilder.append(columns[i] + '\t');
			}
			
			k.set(id);
			v.set(stringBuilder.toString().trim());
			context.write(k, v);
		}
	}

	// #################################################################################
	// #################################################################################
	/**
	 * ����inpath outpath [id,..] [column,..] ͳ�����������id������Ķ����
	 * 
	 * @author Administrator
	 */
	static class MyMapperIDsColumns extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text k = new Text();
		private Text v = new Text();
		private HashMap<Integer, Boolean> columnsIDMap = new HashMap<Integer, Boolean>();
		private HashMap<String, Boolean> IDsMap = new HashMap<String, Boolean>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.setup(context);
			Configuration configuration = context.getConfiguration();

			String idString = configuration.get("IDs", "100");
			String[] ids = idString.split(",");
			for (int i = 0; i < ids.length; ++i)
			{
				IDsMap.put(ids[i].trim(), true);
			}

			String columnString = configuration.get("columnIDs");
			String[] columnsID = columnString.split(",");
			for (int i = 0; i < columnsID.length; ++i)
			{
				columnsIDMap.put(Integer.parseInt(columnsID[i].trim()), true);
			}
		};

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{
			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			// ������Ҫͳ�Ƶ�idʱ������������¼
			if (!IDsMap.containsKey(id))
				return;
			String contents = line.substring(line.indexOf("\t")).trim();
			String[] columns = contents.split("\t");
						
			StringBuilder stringBuilder = new StringBuilder();
			for(int i=0; i<columns.length; ++i)
			{
				if (columnsIDMap.containsKey(i + 1))
					stringBuilder.append(columns[i] + '\t');
			}
			
			k.set(id);
			v.set(stringBuilder.toString().trim());
			context.write(k, v);
		}
	}

	// #################################################################################
	// #################################################################################
	/**
	 * ����inpath outpath [*] [column,..] ͳ���������id������Ķ����
	 * 
	 * @author Administrator
	 */
	static class MyMapperColumns extends Mapper<LongWritable, Text, Text, Text>
	{
		private Text k = new Text();
		private Text v = new Text();
		private HashMap<Integer, Boolean> columnsIDMap = new HashMap<Integer, Boolean>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			Configuration configuration = context.getConfiguration();

			String columnString = configuration.get("columnIDs");
			String[] columnsID = columnString.split(",");
			for (int i = 0; i < columnsID.length; ++i)
			{
				columnsIDMap.put(Integer.parseInt(columnsID[i].trim()), true);
			}
		};

		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException
		{
			String line = value.toString();
			String id = line.substring(0, line.indexOf("\t")).trim();
			String contents = line.substring(line.indexOf("\t")).trim();
			String[] columns = contents.split("\t");
					
			StringBuilder stringBuilder = new StringBuilder();
			for(int i=0; i<columns.length; ++i)
			{
				if (columnsIDMap.containsKey(i + 1))
					stringBuilder.append(columns[i] + '\t');
			}
			
			k.set(id);
			v.set(stringBuilder.toString().trim());
			context.write(k, v);
		}
	}

}
