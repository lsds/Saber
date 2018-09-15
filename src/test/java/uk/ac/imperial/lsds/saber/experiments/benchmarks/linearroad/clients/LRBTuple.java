package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad.clients;

public class LRBTuple {
	
	private int         type; /* {0,2,3,4} */
	private long   timestamp; /* 0.. 10799 */
	private int    vehicleId; /* 0..   N-1 */
	private float      speed; /* 0..   100 */
	private int      highway; /* 0..   L-1 */
	private int         lane; /* 0..     4 */
	private int    direction; /* 0..     1 */
	private int      segment; /* 0..    99 */
	private int     position; /* 0..527999 */ /* 528000 / 1760*/
	private int      queryId; /* 0..       */
	private int startSegment; /* 0..       */ 
	private int   endSegment; /* 0..       */
	private int      weekday; /* 1..     7 */
	private int       minute; /* 1..  1440 */
	private int          day; /* 1..    69 */
	
	public LRBTuple () {
		/* Initialise all attributes */
		reset ();
	}
	
	private void reset () {
		this.type         = 0;
		this.timestamp    = 0L;
		this.vehicleId    = 0;
		this.speed        = 0;
		this.highway      = 0;
		this.lane         = 0;
		this.direction    = 0;
		this.segment      = 0;
		this.position     = 0;
		this.queryId      = 0;
		this.startSegment = 0;
		this.endSegment   = 0;
		this.weekday      = 0;
		this.minute       = 0;
		this.day          = 0;
	}
	
	public static void parse (String line, LRBTuple t) {
		String [] s = line.split(",");
		/* assert s.length == 15: "error: invalid line format" */
		t.type         = Integer.parseInt(s[ 0]);
		t.timestamp    = Long.parseLong  (s[ 1]) + 1; /* Start time from 1 rather 0 */
		t.vehicleId    = Integer.parseInt(s[ 2]);
		t.speed        = Float.parseFloat(s[ 3]);
		t.highway      = Integer.parseInt(s[ 4]);
		t.lane         = Integer.parseInt(s[ 5]);
		t.direction    = Integer.parseInt(s[ 6]);
		t.segment      = Integer.parseInt(s[ 7]);
		t.position     = Integer.parseInt(s[ 8]);
		t.queryId      = Integer.parseInt(s[ 9]);
		t.startSegment = Integer.parseInt(s[10]);
		t.endSegment   = Integer.parseInt(s[11]);
		t.weekday      = Integer.parseInt(s[12]);
		t.minute       = Integer.parseInt(s[13]);
		t.day          = Integer.parseInt(s[14]);
	}
	
	public int   getType()         { return         type; }
	public long  getTimestamp()    { return    timestamp; }
	public int   getVehicleId()    { return    vehicleId; }
	public float getSpeed()        { return        speed; }
	public int   getHighway()      { return      highway; }
	public int   getLane()         { return         lane; }
	public int   getDirection()    { return    direction; }
	public int   getSegment()      { return      segment; }
	public int   getPosition()     { return     position; }
	public int   getQueryId()      { return      queryId; }
	public int   getStartSegment() { return startSegment; }
	public int   getEndSegment()   { return   endSegment; }
	public int   getWeekday()      { return      weekday; }
	public int   getMinute()       { return       minute; }
	public int   getDay()          { return          day; }
	
}

