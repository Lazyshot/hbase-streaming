package com.louddoor.hbase_streaming;

import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public abstract class Serializer {
	protected OutputStream out;
	protected byte[] newLine = Bytes.toBytes("\n");
	
	public Serializer(OutputStream out) {
		this.out = out;
	}
	
	public void writeLine(byte[] data)
	{
		
		try {
			this.out.write(data);
			this.out.write(newLine);
			this.out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public abstract void writeMap(ImmutableBytesWritable rowKey, Result in) throws Exception;
	public abstract void writeReduce(Text rowKey, Iterable<Text> values) throws Exception;
	
}