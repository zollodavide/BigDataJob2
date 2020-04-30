import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrendMapper2 extends Mapper<Object, Text, Text, Text>{
	
	
	private Text ticker = new Text(); 
	private Text settore_azienda = new Text(); 
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] parts = value.toString().split(",");
		ticker.set(parts[HistoricalStockConstants.TICKER]);
		
		try {
			settore_azienda.set(parts[HistoricalStockConstants.SECTOR]+","+parts[HistoricalStockConstants.NAME]);
		}catch(Exception e) {
			
		}
		
		context.write(ticker, settore_azienda);	
	}

}
