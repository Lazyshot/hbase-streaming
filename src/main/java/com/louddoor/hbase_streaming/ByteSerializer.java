package com.louddoor.hbase_streaming;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class ByteSerializer extends Serializer {
	DataOutput dout;
	protected byte[] tab = Bytes.toBytes("\t");

	public ByteSerializer(OutputStream out) {
		super(out);
		dout = new DataOutputStream(out);
	}

	@Override
	public void writeMap(ImmutableBytesWritable rowKey, Result in) throws Exception {
		rowKey.write(dout); //writes bytes.length + bytes
		/*
		 * totalLengthOfResult(int)
		 * foreach keyvalue = lengthOfKV + KV.bytes
		 * 
		 * KV.bytes:
		 * keyLength(int) + valueLength(int) + rowLength(short) + rowKey(bytes) + familyLength(byte) + family(bytes) + qualifier(bytes) + timestamp(long) + type(byte) + value(bytes)
		 * 
		 */
		in.write(this.dout); 
		dout.write(newLine);
	}

	@Override
	public void writeReduce(Text rowKey, Iterable<Text> values) throws Exception {
		rowKey.write(dout); //writes bytes.length + bytes

		for (Text val : values)
		{
			val.write(dout); //writes bytes.length + bytes
		}
		
		dout.write(newLine);
	}



}