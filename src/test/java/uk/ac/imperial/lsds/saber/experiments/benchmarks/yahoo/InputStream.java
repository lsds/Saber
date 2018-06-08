package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import java.nio.ByteBuffer;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public abstract class InputStream implements YahooBenchmarkQuery {
	
	ITupleSchema schema = null;
	QueryApplication application = null;
	
	private boolean isV2 = false;
	
	public InputStream () {
		this(false);
	}
	
	public InputStream (boolean isV2) {
		this.isV2 = isV2;
	}
	
	public QueryApplication getApplication () {
		return application;
	}
	
	public abstract void createApplication (QueryConf queryConf, boolean isExecuted);
	public abstract void createApplication (QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns);
	
	public ITupleSchema getSchema () {
		if (schema == null) {
			if (this.isV2)
				createSchemaV2();
			else
				createSchema ();
		}
		return schema;
	}
	
	// create schema for SABER
	public void createSchema () {
		
		int [] offsets = new int [7];
		
		offsets[0] =  0; /* Event Time Timestamp:	long */
		offsets[1] =  8; /* User Id:   				uuid */		
		offsets[2] = 24; /* Page Id: 				uuid */
		offsets[3] = 40; /* Ad Id:   				uuid */  
		offsets[4] = 56; /* Ad Type:                 int */  // (0, 1, 2, 3, 4): ("banner", "modal", "sponsored-search", "mail", "mobile") 
		                                                     // => 16 bytes required if UTF-8 encoding is used
		offsets[5] = 60; /* Event Type:              int */  // (0, 1, 2)      : ("view", "click", "purchase")
		                                                     // => 8 bytes required if UTF-8 encoding is used
		offsets[6] = 64; /* IP Address:              int */  // 255.255.255.255 => Either 4 bytes (IPv4) or 16 bytes (IPv6)
		
		/* Additional fields to simulate the strings*/
		//offsets[7] = 76; /*                        long */ // 8 more bytes for the ad_type column		
		
		//offsets[8] = 84; /*                         int */ // 4 more bytes for the event_type column
		
		//offsets[9] = 88; /*                        long */ // 8 more bytes for ip_address column 
				
		schema = new TupleSchema (offsets, 68);
		
		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
		schema.setAttributeType (0, PrimitiveType.LONG 	   );
		schema.setAttributeType (1, PrimitiveType.LONGLONG );
		schema.setAttributeType (2, PrimitiveType.LONGLONG );
		schema.setAttributeType (3, PrimitiveType.LONGLONG );
		schema.setAttributeType (4, PrimitiveType.INT  	   );
		schema.setAttributeType (5, PrimitiveType.INT      );
		schema.setAttributeType (6, PrimitiveType.INT      );
/*		schema.setAttributeType (7, PrimitiveType.LONG     );
		schema.setAttributeType (8, PrimitiveType.INT      );
		schema.setAttributeType (9, PrimitiveType.LONG     );*/
		
		schema.setAttributeName (0, "timestamp"); // timestamp
		schema.setAttributeName (1, "user_id");
		schema.setAttributeName (2, "page_id");
		schema.setAttributeName (3, "ad_id");
		schema.setAttributeName (4, "ad_type");
		schema.setAttributeName (5, "event_type");
		schema.setAttributeName (6, "ip_address");	
		
		//schema.setName("InputStream");
	}	

	public void createSchemaV2 () {
		
		int [] offsets = new int [7];
		
		offsets[0] =  0; /* Event Time Timestamp:	long */
		offsets[1] =  8; /* User Id:   				uuid */		
		offsets[2] = 16; /* Page Id: 				uuid */
		offsets[3] = 24; /* Ad Id:   				uuid */  
		offsets[4] = 32; /* Ad Type:                 int */  // (0, 1, 2, 3, 4): ("banner", "modal", "sponsored-search", "mail", "mobile") 
		                                                     // => 16 bytes required if UTF-8 encoding is used
		offsets[5] = 36; /* Event Type:              int */  // (0, 1, 2)      : ("view", "click", "purchase")
		                                                     // => 8 bytes required if UTF-8 encoding is used
		offsets[6] = 40; /* IP Address:              int */  // 255.255.255.255 => Either 4 bytes (IPv4) or 16 bytes (IPv6)
				
		schema = new TupleSchema (offsets, 44);
		
		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong*/
		schema.setAttributeType (0, PrimitiveType.LONG );
		schema.setAttributeType (1, PrimitiveType.LONG );
		schema.setAttributeType (2, PrimitiveType.LONG );
		schema.setAttributeType (3, PrimitiveType.LONG );
		schema.setAttributeType (4, PrimitiveType.INT  );
		schema.setAttributeType (5, PrimitiveType.INT  );
		schema.setAttributeType (6, PrimitiveType.INT  );

		
		schema.setAttributeName (0, "timestamp"); // timestamp
		schema.setAttributeName (1, "user_id");
		schema.setAttributeName (2, "page_id");
		schema.setAttributeName (3, "ad_id");
		schema.setAttributeName (4, "ad_type");
		schema.setAttributeName (5, "event_type");
		schema.setAttributeName (6, "ip_address");	
	}	

}
