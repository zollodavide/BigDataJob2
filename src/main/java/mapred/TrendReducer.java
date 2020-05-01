package mapred;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

		
		for(String sector: sector2stocks.keySet()) {
			
			List<CustomStock> stc = sector2stocks.get(sector);
			
			
			
			for(int anno=2008; anno<2019; anno++) {
				Double mediaVolumeAnnuale = 0.;
				Double mediaDiffPerc = 0.;
				Double mediaGiornalieraSettore=0.;
				long volumeAnnuale =0;
				Integer count=0;

				Map<String,Double> azienda2mediaGiornaliera= new HashMap<String, Double>();
				Map<String, CustomStock> azienda2dataIn = new HashMap<String, CustomStock>();
				Map<String, CustomStock> azienda2dataFin = new HashMap<String, CustomStock>();
				
				for(CustomStock stock: stc) {
					
					if(stock.getAnno() == anno) {
						
						/* VOLUME */
						volumeAnnuale+=stock.getVolume();
						count++;
					
					

						/* DIFFERENZA PERCENTUALE */
						
						//DATA INIZIALE
						String azienda = ticker2azienda.get(stock.getTicker());
						
						if(azienda2dataIn.containsKey(azienda)) {
							if(TrendUtility.dataMinore(stock,azienda2dataIn.get(azienda)))
								azienda2dataIn.put(azienda, stock);							
						}
						else 
							azienda2dataIn.put(azienda, stock);
						
						
						//DATA FINALE
						if(azienda2dataFin.containsKey(azienda)) {
							if(TrendUtility.dataMaggiore(stock,azienda2dataFin.get(azienda)))
								azienda2dataFin.put(azienda, stock);							
						}
						else 
							azienda2dataFin.put(azienda, stock);
						
						
						
						
						/* MEDIA GIORNALIERA */
		
						double quotazioneGiornaliera = (stock.getOpen() + stock.getClose())/2;
						if(azienda2mediaGiornaliera.containsKey(azienda)) {
							Double tmp = azienda2mediaGiornaliera.get(azienda);
							azienda2mediaGiornaliera.put(azienda, (tmp+quotazioneGiornaliera)/2);
						}
						else 
							azienda2mediaGiornaliera.put(azienda, quotazioneGiornaliera);
						
					}
				}
				
				/* VOLUME */
				
				if(count!=0) {
					
					mediaVolumeAnnuale = (double) (volumeAnnuale/count);
				}
				
				
				/* DIFFERENZA PERCENTUALE */
				
				double cumul = 0;
				
				for(String k: azienda2dataIn.keySet() ) {
					Double prezzoIN = azienda2dataIn.get(k).getClose();
					Double prezzoFIN = azienda2dataFin.get(k).getClose();
					
					cumul += ((prezzoIN - prezzoFIN)/prezzoIN);
				}
				
				if(azienda2dataIn.keySet().size()>0)
					mediaDiffPerc = (double) Math.round((cumul/azienda2dataIn.keySet().size())*100);
				
				
				/* MEDIA GIORNALIERA */


				if(azienda2mediaGiornaliera.size()!=0) {
					Double sum=0.;
					for(Double med : azienda2mediaGiornaliera.values())
						sum+=med;
					
					mediaGiornalieraSettore = (sum/azienda2mediaGiornaliera.size());
				}
				
				
				String stringOutput = "Mean Volume: "+mediaVolumeAnnuale.toString() + 
						", Mean Diff: " +mediaDiffPerc+"%" + 
						", Mean Daily Quotation: " + String.format("%.2f", mediaGiornalieraSettore);
				context.write(new Text(sector+ " " +anno), new Text(stringOutput));

			
			}
			
		}	
	
		


	}
	
	
}
	

