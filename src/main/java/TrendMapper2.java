import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrendMapper2 extends Mapper<Object, Text, Text, Text>{
	
	
	private Text ticker = new Text(); 
	private Text sector = new Text(); 
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] parts = value.toString().split(",");
		ticker.set(parts[HistoricalStockConstants.TICKER]);
		
		try {
			sector.set(parts[HistoricalStockConstants.SECTOR]);
		}catch(Exception e) {
			
		}
		
		context.write(ticker, sector);	
	}

}
