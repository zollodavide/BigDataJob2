package mapred;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import constants.HistoricalStockPricesConstants;


public class TrendMapper1 extends Mapper<Object, Text, Text, Text> {
		
	private Text ticker = new Text();
	private Text openPrices = new Text();
	private Text closePrices = new Text();
	private Text volume = new Text();
	private Integer anno;
	private Integer mese;
	private Integer giorno;

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] parts = value.toString().split(",");
		ticker.set(parts[HistoricalStockPricesConstants.TICKER]);
		
		String[] data = parts[HistoricalStockPricesConstants.DATE].split("-");
		try {
			
			openPrices.set(parts[HistoricalStockPricesConstants.OPEN]);
			closePrices.set(parts[HistoricalStockPricesConstants.CLOSE]);
			volume.set(parts[HistoricalStockPricesConstants.VOLUME]);
		
		} catch(Exception e) {
			context.write(ticker, new Text("ERRORE2"));

		}
	
		try {

			if(Integer.parseInt(data[0])>=2008) {
				
				anno = Integer.parseInt(data[0]);
				mese = Integer.parseInt(data[1]);
				giorno = Integer.parseInt(data[2]);
				String all = volume+","+openPrices+","+closePrices+","+anno+","+mese+","+giorno;
				context.write(ticker, new Text(all));
			}
		} catch(Exception e) {
			
		}		
	}
	
}
