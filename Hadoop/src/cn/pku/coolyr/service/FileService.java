package cn.pku.coolyr.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

public class FileService
{
	public HashMap<String, Object> analysisFile(File path)
	{
		FileReader fileReader = null;
		BufferedReader buffReader = null;
		HashMap<String, Object> map = new HashMap<>();
		try
		{
			fileReader = new FileReader(path);
			buffReader = new BufferedReader(fileReader);
			Long lineNum = (long) 1;
			ArrayList<String> contentList = new ArrayList<>();
			int neadNum = 22;

			String line = buffReader.readLine();
			StringTokenizer tokenizer = new StringTokenizer(line);
			int columnsNum = tokenizer.countTokens();
			
			contentList.add(line);
			while ((line = buffReader.readLine()) != null)
			{
				if (lineNum <= neadNum)
					contentList.add(line);
				lineNum++;
			}


			map.put("columnNum", columnsNum + "");
			map.put("lineNum", lineNum + "");
			map.put("contentList", contentList);

		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();

		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		finally
		{
			try
			{
				buffReader.close();
				fileReader.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		return map;
	}
}
