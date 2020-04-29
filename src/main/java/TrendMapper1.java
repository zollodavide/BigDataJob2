import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TrendMapper1 extends Mapper<Object, Text, Text, Text> {
		
	private Text ticker = new Text();
	private Text openPrices = new Text();
	private Text closePrices = new Text();
	private Text volume;
	private Integer anno;
	private Integer mese;
	private Integer giorno;

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] parts = value.toString().split(",");
		ticker.set(parts[HistoricalStockPricesConstants.TICKER]);
		
		try {
			String[] data = parts[HistoricalStockPricesConstants.DATE].split("-");
			
			openPrices.set(parts[HistoricalStockPricesConstants.OPEN]);
			closePrices.set(parts[HistoricalStockPricesConstants.CLOSE]);
			volume.set(parts[HistoricalStockPricesConstants.VOLUME]);
	

			if(Integer.parseInt(data[0])>=2008) {
				
				anno = Integer.parseInt(data[0]);
				mese = Integer.parseInt(data[1]);
				giorno = Integer.parseInt(data[2]);
				String all = volume+","+openPrices+","+closePrices+","+anno+","+mese+","+giorno;
				
				context.write(ticker, new Text(all));
				
				
			}
		} catch(Exception e) {
			//ANCHE IN QUESTO CASO LA SOLUZIONE È SALTARE L'INPUT.
		}
		
		
	}
	
}
