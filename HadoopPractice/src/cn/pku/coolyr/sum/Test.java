package cn.pku.coolyr.sum;

import java.util.ArrayList;


public class Test
{
	public static int i = 5;

	public static void main(String[] args)
	{
		// TODO Auto-generated method stub
		
//		String s = "123\t12\t12";
//		
//		System.out.println(s.substring(0, s.indexOf("\t")).length());
//		System.out.println(s.substring(s.indexOf("\t")).trim());
		
//		HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
//		System.out.println(map.get(1));
		
//		Inner inner = new Inner();
//		inner.show();
//		
//		System.out.println("a".equals("a"));
//		
//		String valueString = "\t8\t*";
//		System.out.println(valueString);
//		System.out.println(valueString.length());
//		System.out.println(valueString.trim());
//		System.out.println(valueString.length());
//		 valueString = valueString.substring(0,valueString.lastIndexOf("\t"));
//		System.out.println(valueString);
		
//		String s = "[1,2,3]";
//		
//		System.out.println(s.substring(s.indexOf("[")+1, s.indexOf("]")));
//		System.out.println(s.substring(1, s.indexOf("]")).length());
		
		//System.out.println(s.substring(s.indexOf("\t")).trim());
		
		
//		System.out.println("102".compareTo("11"));
		ArrayList<Integer> arrayList = new ArrayList<Integer>();
		arrayList.add(1);
		arrayList.add(3);
		arrayList.add(2);
		arrayList.set(1, 10);
//		arrayList.add(1, 10);
		arrayList.set(3, 10);
		
//		arrayList.set(index, element)
		
		for(int i=0; i<arrayList.size(); i++)
		{
			System.out.println(arrayList.size() +  "    " + arrayList.get(i));
		}
	}

	static class Inner
	{

		public static void show()
		{

			System.out.println(Test.i);
		}
	}

}
