package uk.ac.imperial.lsds.saber.cql.operators;

public enum AggregationType {

	MAX, MIN, CNT, SUM, AVG;

	public static AggregationType fromString (String s) {
		
		if (s.toLowerCase().contains("avg"))
			return AVG;
		else 
		if (s.toLowerCase().contains("sum"))
			return SUM;
		else 
		if (s.toLowerCase().contains("cnt"))
			return CNT;
		else 
		if (s.toLowerCase().contains("min"))
			return MIN;
		else 
		if (s.toLowerCase().contains("max"))
			return MAX;
		else 
			throw new IllegalArgumentException ("error: unknown aggregation type");
	}

	public String asString (String s) {
		return this.toString() + "(" + s + ")";
	}
}
