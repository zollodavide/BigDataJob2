import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrendReducer extends Reducer<Text, Text, Text, Text>{
	
	private Map<String, String> ticker2sectors = new HashMap<String, String>();
	private Map<String, List<CustomStock>> ticker2stocks = new HashMap<String, List<CustomStock>>();
	private Map<String, List<CustomStock>> sector2stocks = new HashMap<String, List<CustomStock>>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		List<CustomStock> stocks;

		for(Text value : values) {
			
			String[] parts = value.toString().split(",");
//			context.write(value, new Text());

			if(parts.length > 1) {
				CustomStock custom = new CustomStock();
				
				try {					
					custom.setTicker(key.toString());
					custom.setVolume(Integer.parseInt(parts[ReducerConstants.VOLUME]));
					custom.setOpen(Double.parseDouble(parts[ReducerConstants.OPEN]));
					custom.setClose(Double.parseDouble(parts[ReducerConstants.CLOSE]));
					custom.setAnno(Integer.parseInt(parts[ReducerConstants.ANNO]));
					custom.setMese(Integer.parseInt(parts[ReducerConstants.MESE]));
					custom.setGiorno(Integer.parseInt(parts[ReducerConstants.GIORNO]));
					
					if(ticker2stocks.containsKey(key.toString()))
						stocks = ticker2stocks.get(key.toString());
					else
						stocks = new ArrayList<CustomStock>();
	
					stocks.add(custom);
					ticker2stocks.put(key.toString(),stocks);
				} catch(Exception e ) {
					
				}
				//////
//				Text n = new Text(stocks.toString());
//				context.write(n, new Text());
			}
			
			else if (parts.length == 1)
				ticker2sectors.put(key.toString(), parts[0].toString());
		}		
	}
	
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		List<CustomStock> stocks;
		
		for(String ticker: ticker2stocks.keySet()) {
			String sector = ticker2sectors.get(ticker);
			
			if(sector2stocks.containsKey(sector))
				stocks = sector2stocks.get(sector);
			else
				stocks = new ArrayList<CustomStock>();
			
			stocks.addAll(ticker2stocks.get(ticker));
			sector2stocks.put(sector,stocks);			
		}	
		
//		Integer s = sector2stocks.size();
//		Text n = new Text(s.toString());
//		Integer ss = ticker2stocks.size();
//		Text nn = new Text(ss.toString());
//		context.write(n, nn);
		
		for(String sector: sector2stocks.keySet()) {
			
			List<CustomStock> stc = sector2stocks.get(sector);
			
			int anno;
			for(anno=2008; anno<2019; anno++) {
				
				Integer volumeAnnuale =0;
				Integer count=0;
				
				for(CustomStock stock: stc) {
					if(stock.getAnno() == anno) {
						volumeAnnuale+=stock.getVolume();
						count++;
					}
				}
				
				if(count!=0) {
					
					Double mediaVolumeAnnuale = (double) (volumeAnnuale/count);
					context.write(new Text(sector+ " " +anno), new Text(mediaVolumeAnnuale.toString()));
				}
				else
					context.write(new Text(sector+ " " +anno), new Text("0"));

			}
			


		}
	
	}
	
	

}
