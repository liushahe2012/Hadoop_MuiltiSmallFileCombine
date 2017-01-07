package MuiltiSmallFileCombine;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class MyInputFormat extends CombineFileInputFormat<Text, Text>{
	
	/*
     * 重写方法,直接返回false,对所有文件都不进行切割,保持完整
     */
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    } 
	/*
	 * 重写此方法,返回的CombineFileRecordReader为处理每个分片的recordReader,
	 * 在构造函数中设置自定义的RecordReader对象
	 */	
	public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new CombineFileRecordReader<Text, Text>((CombineFileSplit) inputSplit, taskAttemptContext, MyRecordReader.class);
    }
}
