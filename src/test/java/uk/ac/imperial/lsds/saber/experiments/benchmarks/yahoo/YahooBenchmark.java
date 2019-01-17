package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.YahooBenchmarkOp;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.IntComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.CampaignGenerator;
import uk.ac.imperial.lsds.saber.processors.HashMap;


public class YahooBenchmark extends InputStream {
	
	private long[][] ads;
	private int adsPerCampaign;
	private boolean isV2 = false;
	
	public YahooBenchmark (QueryConf queryConf, boolean isExecuted) {
		this(queryConf, isExecuted, null, false);
	}
	
	public YahooBenchmark (QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns, boolean isV2) {
		adsPerCampaign = 10;
		this.isV2 = isV2;
		if (this.isV2)
			createSchemaV2();
		else
			createSchema ();
		
		createApplication (queryConf, isExecuted, campaigns);
	}
	
	public void createApplication (QueryConf queryConf, boolean isExecuted) {
		this.createApplication(queryConf, isExecuted, null);
	}
	
	public void createApplication(QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns) {
		/* Set execution parameters */
		long timestampReference = System.nanoTime();
		boolean realtime = true;
		int windowSize = 10000;//realtime? 10000 : 10000000;

		
		/* Create Input Schema */
		ITupleSchema inputSchema = schema;
		

		/* FILTER (event_type == "view") */
		/* Create the predicates required for the filter operator */
		IPredicate selectPredicate = new IntComparisonPredicate
				(IntComparisonPredicate.EQUAL_OP, new IntColumnReference(5), new IntConstant(0));
		
		/* PROJECT (ad_id, event_time) */
		/* Define which fields are going to be projected */
		Expression [] expressions = new Expression [2];
		expressions[0] = new LongColumnReference(0); 	   // event_time
		
		if (isV2)
			expressions[1] = new LongColumnReference (3);
		else 
			expressions[1] = new LongLongColumnReference (3);  // ad_id	


		/* JOIN (ad_id, ad_id) */
		/* Define which fields are going be used for the Join operator */
		IPredicate joinPredicate = null;
		if (isV2)
			joinPredicate = new LongComparisonPredicate
				(LongComparisonPredicate.EQUAL_OP , new LongColumnReference(1), new LongColumnReference(0));
		else
			joinPredicate = new LongLongComparisonPredicate
			(LongLongComparisonPredicate.EQUAL_OP , new LongLongColumnReference(1), new LongLongColumnReference(0));


		/* Generate Campaigns' ByteBuffer and HashTable */
		// WindowHashTable relation = CampaignGenerator.generate();
		CampaignGenerator campaignGen = null;
		if (campaigns == null)
			campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate, this.isV2);
		else
			campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate, campaigns);

		ITupleSchema relationSchema = campaignGen.getCampaignsSchema();
		IQueryBuffer relationBuffer = campaignGen.getRelationBuffer();
		this.ads = campaignGen.getAds();
		HashMap hashMap = campaignGen.getHashMap();

		Expression [] expressions2 = new Expression [3];
		expressions2[0] = new LongColumnReference(0); 	   // event_time

		if (isV2) {
			expressions2[1] = new LongColumnReference (1);
			expressions2[2] = new LongColumnReference (2);
		}
		else {
			expressions2[1] = new LongLongColumnReference(1);  // ad_id
			expressions2[2] = new LongLongColumnReference(3);  // campaign_id
		}


		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);


		// Create and initialize the operator for computing the Benchmark's data.
		IOperatorCode cpuCode = new YahooBenchmarkOp (
				inputSchema, 
				selectPredicate, 
				expressions,
				expressions2,
				joinPredicate, 
				relationSchema,
				relationBuffer,
				hashMap,
				windowDefinition, null, null, null
				,isV2
				);
		
		IOperatorCode gpuCode = null;
		
		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);
		
		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);				
		
		Query query1 = new Query (0, operators, inputSchema, windowDefinition, null, null, queryConf, timestampReference);
		
		Set<Query> queries = new HashSet<Query>();
		queries.add(query1);

		ITupleSchema joinSchema = ((YahooBenchmarkOp) cpuCode).getOutputSchema();

		if (isExecuted) {
			application = new QueryApplication(queries);
			application.setup();
		}

		return;
	}
	
	public long[][] getAds () {
		return this.ads;
	}
	
	public int getAdsPerCampaign () {
		return this.adsPerCampaign;
	}
}
