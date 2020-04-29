
public class CustomStock {

	private String ticker;
	private String sector;
	private Double close;
	private Double open;
	private Integer volume;
	private Integer anno;
	private Integer mese;
	private Integer giorno;
	
	public CustomStock() {
		
	}

	public CustomStock(String ticker, String sector, Double close, Double open, Integer volume, Integer anno,
			Integer mese, Integer giorno) {
		this.ticker = ticker;
		this.sector = sector;
		this.close = close;
		this.open = open;
		this.volume = volume;
		this.anno = anno;
		this.mese = mese;
		this.giorno = giorno;
	}

	public String getTicker() {
		return ticker;
	}

	public void setTicker(String ticker) {
		this.ticker = ticker;
	}

	public String getSector() {
		return sector;
	}

	public void setSector(String sector) {
		this.sector = sector;
	}

	public Double getClose() {
		return close;
	}

	public void setClose(Double close) {
		this.close = close;
	}

	public Double getOpen() {
		return open;
	}

	public void setOpen(Double open) {
		this.open = open;
	}

	public Integer getVolume() {
		return volume;
	}

	public void setVolume(Integer volume) {
		this.volume = volume;
	}

	public Integer getAnno() {
		return anno;
	}

	public void setAnno(Integer anno) {
		this.anno = anno;
	}

	public Integer getMese() {
		return mese;
	}

	public void setMese(Integer mese) {
		this.mese = mese;
	}

	public Integer getGiorno() {
		return giorno;
	}

	public void setGiorno(Integer giorno) {
		this.giorno = giorno;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((anno == null) ? 0 : anno.hashCode());
		result = prime * result + ((giorno == null) ? 0 : giorno.hashCode());
		result = prime * result + ((mese == null) ? 0 : mese.hashCode());
		result = prime * result + ((ticker == null) ? 0 : ticker.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CustomStock other = (CustomStock) obj;
		if (anno == null) {
			if (other.anno != null)
				return false;
		} else if (!anno.equals(other.anno))
			return false;
		if (giorno == null) {
			if (other.giorno != null)
				return false;
		} else if (!giorno.equals(other.giorno))
			return false;
		if (mese == null) {
			if (other.mese != null)
				return false;
		} else if (!mese.equals(other.mese))
			return false;
		if (ticker == null) {
			if (other.ticker != null)
				return false;
		} else if (!ticker.equals(other.ticker))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CustomStock [ticker=" + ticker + ", sector=" + sector + ", close=" + close + ", open=" + open
				+ ", volume=" + volume + ", anno=" + anno + ", mese=" + mese + ", giorno=" + giorno + "]";
	}

	
	

	
}
