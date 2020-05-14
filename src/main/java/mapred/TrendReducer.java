package mapred;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import constants.ReducerConstants;
import models.CustomStock;
import utility.TrendUtility;

public class TrendReducer extends Reducer<Text, Text, Text, Text>{
	
	private Map<String, String> ticker2sectors = new HashMap<String, String>();
	private Map<String, String> ticker2azienda = new HashMap<String, String>();
	private Map<String, List<CustomStock>> ticker2stocks = new HashMap<String, List<CustomStock>>();
	private Map<String, List<CustomStock>> sector2stocks = new HashMap<String, List<CustomStock>>();
	private List<CustomStock> stocks = new ArrayList<CustomStock>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		List<CustomStock> stocks;

		for(Text value : values) {
			
			String[] parts = value.toString().split(",");

			if(parts.length > 2) {
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
	
					this.stocks.add(custom);
					stocks.add(custom);
					ticker2stocks.put(key.toString(),stocks);
				} catch(Exception e ) {
					
				}
				
			}
			
			else if (parts.length == 2)
				ticker2sectors.put(key.toString(), parts[0].toString().replace('"', ' ').trim());
				ticker2azienda.put(key.toString(), parts[1].toString().replace('"', ' ').trim());
		}		
	}
	
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {

		for(CustomStock c : this.stocks) {
			String ticker = c.getTicker();
			c.setSector(ticker2sectors.get(ticker));
			c.setAzienda(ticker2azienda.get(ticker));
		}
		
		Map<String, HashSet<String>> sect2aziende= new HashMap<String, HashSet<String>>();
		Map<String,List<CustomStock>> sectAnno2stocks = new HashMap<String, List<CustomStock>>();
		
		for(CustomStock c : this.stocks) {
			String sector = c.getSector();
			String anno = c.getAnno().toString();
			String key = sector + " " + anno;
			String azienda = c.getAzienda();
			
			//SECT2AZIENDA
			HashSet<String> aziende = new HashSet<String>();
			if(sect2aziende.containsKey(sector))
				aziende = sect2aziende.get(sector);
			aziende.add(azienda);
			sect2aziende.put(sector, aziende);
			
			//SECTANNO2STOCKS
			List<CustomStock> lst = new ArrayList<CustomStock>();
			if(sectAnno2stocks.containsKey(key)) 
				lst = sectAnno2stocks.get(key);
			lst.add(c);
			sectAnno2stocks.put(key, lst);
		}
		
		for(String sectAnno : sectAnno2stocks.keySet()) {
			
			CustomStock minStock = null;
			
			CustomStock maxStock = null;
			Double totalVolume = 0.;
			Double totalClose = 0.;
			for(CustomStock c : sectAnno2stocks.get(sectAnno)) {
				
				if(minStock == null || TrendUtility.dataMinore(c, minStock))
					minStock = c;
				if(maxStock == null || TrendUtility.dataMaggiore(c, maxStock))
					maxStock = c;
				
				totalVolume += c.getVolume();

				totalClose += c.getClose();
			}
			
			Double meanQuot = totalClose/sectAnno2stocks.get(sectAnno).size();
			
			Integer meanDiff = (int) (((maxStock.getClose() - minStock.getClose())/minStock.getClose())*100);
			
			String sector = sectAnno.split(" ")[0];
			if(sect2aziende.containsKey(sector)) {
				Integer count = sect2aziende.get(sector).size();
				Double meanVolume = totalVolume/count;
				context.write(new Text(sectAnno), new Text(meanVolume.toString() + ", " + meanDiff + ", " + meanQuot));
			}
		}
	}
}
	

