/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package power;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class PowerMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] SingleTurbineData = valueString.split(",");
                Integer pg = new Integer(SingleTurbineData[5]);
		output.collect(new Text(SingleTurbineData[3]), new IntWritable(pg));
	}
}
