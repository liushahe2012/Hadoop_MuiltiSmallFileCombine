package MuiltiSmallFileCombine;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/*
 * 自定义的RecordReader类,用来处理CombineFileInputSplit返回的每个分片
 */
public class MyRecordReader extends RecordReader<Text, Text>{

	private CombineFileSplit combineFileSplit; // 当前处理的分片
	private Configuration conf; //系统信息
	private int curIndex;//当前处理到第几个分片
	
	private Text curKey = new Text();//当前key
	private Text curValue = new Text();//当前value
	private boolean isRead = false;//是否已经读取过该分分片
	private float currentProgress = 0;//当前读取进度
	
	private FSDataInputStream fsInputStream;//HDFS文件读取流
	
	/*
	 * 构造函数必须的三个参数,自定义的InputFormat类每次读取新的分片时,
	 * 都会实例化自定义的RecordReader类对象来对其进行读取
     * @param combineFileSplit   当前读取的分片
     * @param taskAttemptContext 系统上下文环境
     * @param index              当前分片中处理的文件索引
	 */
	public MyRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext taskAttemptContext, Integer index)
	{
		this.combineFileSplit = combineFileSplit;
		this.conf = taskAttemptContext.getConfiguration();
		this.curIndex = index;
	}
	
	@Override
	public void close() throws IOException {
		if (fsInputStream != null) {
			fsInputStream.close();
		}
		
	}

	/**
     * 返回当前key的方法
     */
	public Text getCurrentKey() throws IOException, InterruptedException {
		return curKey;
	}

	/**
     * 返回当前value的方法
     */
	public Text getCurrentValue() throws IOException, InterruptedException {
		return curValue;
	}

	/**
     * 返回当前的处理进度
     */
	public float getProgress() throws IOException, InterruptedException {
		//获得当前分片中的总文件数
		int splitFileNum = combineFileSplit.getPaths().length;
		if (curIndex > 0 && curIndex < splitFileNum) {
			//当前处理的文件索引除以文件总数得到处理的进度
			currentProgress = (float)curIndex / splitFileNum;
		}
		return currentProgress;
	}

	/*
	 * 初始化RecordReader的一些设置(non-Javadoc)
	 */
	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	/*
	 * 返回true就取出key和value,之后index前移,返回false就结束循环表示没有文件内容可读取了
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		//没被读取过的文件才进行读取
		if (!isRead) {
			//只实现了读取输入目录下的文件，没有实现读取输入目录下子目录里的文件
			//默认的TextFileInputFormat里调用的RecoderReader,是可以读取子目录里的文件的
			
			//根据当前的文件索引从当前分片中找到对应的文件路径
			Path path = combineFileSplit.getPath(curIndex);
			
			//获取父目录名作为Key值
			curKey.set(path.getParent().getName());
			//从当前分片中获得当前文件的长度
			byte[] content = new byte[(int) combineFileSplit.getLength(curIndex)];
			try {
				//读取该文件内容
				FileSystem fs = path.getFileSystem(conf);
				FileStatus st = fs.getFileStatus(path);
				if (!st.isFile()) {
					return false;
				}
				fsInputStream = fs.open(path);
				fsInputStream.readFully(content);
			} catch (Exception ignored) {
            }finally {
				if (fsInputStream != null) {
					fsInputStream.close();
				}
			}
			//整个文件内容作为value值
			curValue.set(content);
			isRead = true;
			return true;
		}
		return false;
	}

}
