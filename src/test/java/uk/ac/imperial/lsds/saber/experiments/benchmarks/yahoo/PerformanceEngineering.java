package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo;

import uk.ac.imperial.lsds.saber.*;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.ComparisonPredicate;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.udfs.PerformanceEngineeringUDF;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.cql.predicates.LongLongComparisonPredicate;
import uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils.CampaignGenerator;
import uk.ac.imperial.lsds.saber.processors.HashMap;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;


public class PerformanceEngineering extends InputStream {

    private long[][] ads;
    private int adsPerCampaign;
    private boolean isV2 = false;
    private int queryNum;


    public PerformanceEngineering(QueryConf queryConf, boolean isExecuted, int queryNum) {
        this(queryConf, isExecuted, null, false, queryNum);
    }

    public PerformanceEngineering(QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns, boolean isV2, int queryNum) {
        this.adsPerCampaign = 10;
        this.isV2 = isV2;
        if (this.isV2)
            createSchemaV2();
        else
            createSchema();
        this.queryNum = queryNum;
        createApplication(queryConf, isExecuted, campaigns);
    }

    public void createApplication(QueryConf queryConf, boolean isExecuted) {
        this.createApplication(queryConf, isExecuted, null);
    }

    public void createApplication(QueryConf queryConf, boolean isExecuted, ByteBuffer campaigns) {
        /* Set execution parameters */
        long timestampReference = System.nanoTime();
        boolean realtime = true;
        int windowSize = 10000;//realtime? 10000 : 10000000;


        /* Create Input Schema */
        ITupleSchema inputSchema = schema;


        // AGGREGATE (count("*") as count, max(event_time) as 'lastUpdate)
        // GROUP BY Campaign_ID with 10 seconds tumbling window
        WindowDefinition windowDefinition = new WindowDefinition(WindowType.ROW_BASED, windowSize, windowSize);
        AggregationType[] aggregationTypes = new AggregationType[2];

        System.out.println("[DBG] aggregation type is COUNT(*)");
        aggregationTypes[0] = AggregationType.CNT;
        System.out.println("[DBG] aggregation type is MAX(0)");
        aggregationTypes[1] = AggregationType.MAX;

        FloatColumnReference[] aggregationAttributes = new FloatColumnReference[2];
        aggregationAttributes[0] = new FloatColumnReference(1);
        aggregationAttributes[1] = new FloatColumnReference(0);

        Expression[] groupByAttributes = null;
        if (isV2)
            groupByAttributes = new Expression[]{new LongColumnReference(3)};
        else
            groupByAttributes = new Expression[]{new LongLongColumnReference(3)};


        int select1Column = 5;
        int select1Constant = 0;
        ComparisonPredicate select1ComparisonPredicate = ComparisonPredicate.EQUAL;
        int select2Column = (this.queryNum == 3) ? 4 : 0;
        int select2Constant = (this.queryNum == 3) ? 2 : 98;
        ComparisonPredicate select2ComparisonPredicate = ComparisonPredicate.GREATER;

        int project1Column1 = 0;
        int project1Column2 = 3;

        //Expression [] expressions2,
        int project2Column1 = 0;
        int project2Column2 = 3;

        /* Generate Campaigns' ByteBuffer and HashTable */
        IPredicate joinPredicate = new LongLongComparisonPredicate
                (LongLongComparisonPredicate.EQUAL_OP, new LongLongColumnReference(1), new LongLongColumnReference(0));
        CampaignGenerator campaignGen = null;
        if (campaigns == null)
            campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate, this.isV2);
        else
            campaignGen = new CampaignGenerator(adsPerCampaign, joinPredicate, campaigns);

        ITupleSchema relationSchema = campaignGen.getCampaignsSchema();
        IQueryBuffer relationBuffer = campaignGen.getRelationBuffer();
        this.ads = campaignGen.getAds();
        HashMap hashMap = campaignGen.getHashMap();

        //IPredicate joinPredicate,
        int joinColumnFromLeftSide = 1;
        int joinColumnFromRightSide = 0;
        ComparisonPredicate joinComparisonPredicate = ComparisonPredicate.EQUAL;


        // Create and initialize the operator for computing the Benchmark's data.
        IOperatorCode cpuCode = new PerformanceEngineeringUDF(
                inputSchema,
                select1Column,
                select1Constant,
                select1ComparisonPredicate,
                select2Column,
                select2Constant,
                select2ComparisonPredicate,
                project1Column1,
                project1Column2,
                project2Column1,
                project2Column2,
                joinColumnFromLeftSide,
                joinColumnFromRightSide,
                joinComparisonPredicate,
                relationSchema,
                relationBuffer,
                isV2, this.queryNum
        );

        IOperatorCode gpuCode = null;

        QueryOperator operator;
        operator = new QueryOperator(cpuCode, gpuCode);

        Set<QueryOperator> operators = new HashSet<QueryOperator>();
        operators.add(operator);

        Query query1 = new Query(0, operators, inputSchema, windowDefinition, null, null, queryConf, timestampReference);

        Set<Query> queries = new HashSet<Query>();
        queries.add(query1);

        Query query2 = null; // we only need this in the case we aggregate -- queryNum == 2
        if (this.queryNum == 2) {
            // Create the aggregate operator
            ITupleSchema joinSchema = ((PerformanceEngineeringUDF) cpuCode).getOutputSchema();
            cpuCode = new Aggregation(windowDefinition, aggregationTypes, aggregationAttributes, groupByAttributes);

            operator = new QueryOperator(cpuCode, gpuCode);
            operators = new HashSet<QueryOperator>();
            operators.add(operator);
            query2 = new Query(1, operators, joinSchema, windowDefinition, null, null, queryConf, timestampReference);

            // Connect the two queries
            queries.add(query2);
            query1.connectTo(query2);
        }

        if (isExecuted) {
            application = new QueryApplication(queries);
            application.setup();

            if (this.queryNum == 2) {
                /* The path is query -> dispatcher -> handler -> aggregator */
                if (SystemConf.CPU)
                    query2.setAggregateOperator((IAggregateOperator) cpuCode);
                else
                    query2.setAggregateOperator((IAggregateOperator) gpuCode);
            }
        }

        return;
    }

    public long[][] getAds() {
        return this.ads;
    }

    public int getAdsPerCampaign() {
        return this.adsPerCampaign;
    }
}
