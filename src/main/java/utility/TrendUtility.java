package utility;
import models.CustomStock;

public class TrendUtility {
	
	public static boolean dataMaggiore(CustomStock st1, CustomStock st2) {
		if(st1.getMese() > st2.getMese())
			return true;
		else if(st1.getMese() == st2.getMese()) {
			if(st1.getGiorno() > st2.getGiorno())
				return true;
			else 
				return false;	
		}
		else 
			return false;
	}
	
	public static boolean dataMinore(CustomStock st1, CustomStock st2) {
		if(st1.getMese() < st2.getMese())
			return true;
		else if(st1.getMese() == st2.getMese()) {
			if(st1.getGiorno() < st2.getGiorno())
				return true;
			else 
				return false;	
		}
		else 
			return false;
	}
	
}
