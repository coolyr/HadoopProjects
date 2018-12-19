package cn.pku.coolyr.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//public interface WritableComparable<T> extends Writable, Comparable<T>
public class SortBean implements WritableComparable<SortBean>
{

	private String id;// �˺�

	private double field1;// �����ֶ�һ

	private double field2;// �����ֶζ�

	/**
	 * - �����һ��set�����������ö����ֵ��������췽���� ��Ϊ����Ĵ���һ����map����������
	 * 
	 * @param id
	 * @param field1
	 * @param field2
	 */
	public void set(String id, double field1, double field2)
	{
		this.id = id;
		this.field1 = field1;
		this.field2 = field2;
	}

	@Override
	public String toString()
	{
		return this.id + "\t" + this.field1 + "\t" + this.field2;
	}

	/**
	 * serialize
	 */
	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(id);
		out.writeDouble(field1);
		out.writeDouble(field2);
	}

	/**
	 * deserialize
	 */
	public void readFields(DataInput in) throws IOException
	{
		this.id = in.readUTF();
		this.field1 = in.readDouble();
		this.field2 = in.readDouble();
	}

	public int compareTo(SortBean o)
	{

		if (this.field1 != o.getField1())
			return this.field1 > o.getField1() ? 1 : -1;
		else
		{
			if (this.field2 != o.getField2())
				return this.field2 > o.getField2() ? 1 : -1;
			else
				return this.id.compareTo(o.getId());
		}

	}

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public double getField1()
	{
		return field1;
	}

	public void setField1(double field1)
	{
		this.field1 = field1;
	}

	public double getField2()
	{
		return field2;
	}

	public void setField2(double field2)
	{
		this.field2 = field2;
	}

}
