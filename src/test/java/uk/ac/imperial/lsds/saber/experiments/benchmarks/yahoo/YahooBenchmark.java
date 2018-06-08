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


		// AGGREGATE (count("*") as count, max(event_time) as 'lastUpdate) 
		// GROUP BY Campaign_ID with 10 seconds tumbling window
		WindowDefinition windowDefinition = new WindowDefinition (WindowType.RANGE_BASED, windowSize, windowSize);
		AggregationType [] aggregationTypes = new AggregationType [2];

		System.out.println("[DBG] aggregation type is COUNT(*)" );
		aggregationTypes[0] = AggregationType.CNT;
		System.out.println("[DBG] aggregation type is MAX(0)" );
		aggregationTypes[1] = AggregationType.MAX;
		
		FloatColumnReference[] aggregationAttributes = new FloatColumnReference [2];
		aggregationAttributes[0] = new FloatColumnReference(1);
		aggregationAttributes[1] = new FloatColumnReference(0);
		
		Expression [] groupByAttributes = null;
		if (isV2)
			groupByAttributes = new Expression [] {new LongColumnReference(3)};
		else
			groupByAttributes = new Expression [] {new LongLongColumnReference(3)};

		
		// Create and initialize the operator for computing the Benchmark's data.
		IOperatorCode cpuCode = new YahooBenchmarkOp (
				inputSchema, 
				selectPredicate, 
				expressions, 
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
		
		
		// Create the aggregate operator
		ITupleSchema joinSchema = ((YahooBenchmarkOp) cpuCode).getOutputSchema();
		cpuCode = new Aggregation (windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);

		IPredicate selectPredicate2 = new IntComparisonPredicate
				(IntComparisonPredicate.NONEQUAL_OP, new IntColumnReference(1), new IntConstant(0));
		operator = new QueryOperator (cpuCode, gpuCode);
		operators = new HashSet<QueryOperator>();
		operators.add(operator);
		Query query2 = new Query (1, operators, joinSchema, windowDefinition, null, null, queryConf, timestampReference);
		

		// Connect the two queries
		queries.add(query2);
		query1.connectTo(query2);

		if (isExecuted) {
			application = new QueryApplication(queries);
			application.setup();
		
			/* The path is query -> dispatcher -> handler -> aggregator */
			if (SystemConf.CPU)
				query2.setAggregateOperator((IAggregateOperator) cpuCode);
			else
				query2.setAggregateOperator((IAggregateOperator) gpuCode);
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
