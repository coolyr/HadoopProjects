package test;

import java.io.File;
import java.util.HashMap;

import cn.pku.coolyr.service.FileService;

public class test
{

	public static void main(String[] args)
	{
		// TODO 自动生成的方法存根
		FileService fileService = new FileService();
		HashMap<String, Object> map = fileService.analysisFile(new File("D:\\Javar\\apache-tomcat-7.0.63\\webapps\\Hadoop\\file\\info.txt"));
		System.out.println(map.get("linesNum"));
		System.out.println(map.get("columsNum"));

	}

}
