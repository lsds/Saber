package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;

public abstract class LRB implements BenchmarkQuery {
	
	ITupleSchema schema = null;
	QueryApplication application = null;
	
	public QueryApplication getApplication () {
		return application;
	}
	
	public abstract void createApplication (QueryConf queryConf);
	
	public ITupleSchema getSchema () {
		if (schema == null)
			createSchema ();
		return schema;
	}
	
	public void createSchema () {
		
		int [] offsets = new int [7];
		
		offsets[0] =  0; /* timestamp:  long */
		offsets[1] =  8; /*   vehicle:   int */ 
		offsets[2] = 12; /*     speed: float */
		offsets[3] = 16; /*   highway:   int */
		offsets[4] = 20; /*      lane:   int */
		offsets[5] = 24; /* direction:   int */
		offsets[6] = 28; /*  position:   int */
				
		schema = new TupleSchema (offsets, 32);
		
		/* 0:undefined 1:int, 2:float, 3:long */
		schema.setAttributeType (0, PrimitiveType.LONG );
		schema.setAttributeType (1, PrimitiveType.INT  );
		schema.setAttributeType (2, PrimitiveType.FLOAT);
		schema.setAttributeType (3, PrimitiveType.INT  );
		schema.setAttributeType (4, PrimitiveType.INT  );
		schema.setAttributeType (5, PrimitiveType.INT  );
		schema.setAttributeType (6, PrimitiveType.INT  );
		
		schema.setAttributeName (0, "timestamp");
		schema.setAttributeName (1, "vehicleId");
		schema.setAttributeName (2,     "speed");
		schema.setAttributeName (3,   "highway");
		schema.setAttributeName (4,      "lane");
		schema.setAttributeName (5, "direction");
		schema.setAttributeName (6,  "position");
		
		//schema.setName("PosSpeedStr");
	}
}
