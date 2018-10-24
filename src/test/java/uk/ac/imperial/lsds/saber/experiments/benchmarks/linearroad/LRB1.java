package uk.ac.imperial.lsds.saber.experiments.benchmarks.linearroad;

import java.util.HashSet;
import java.util.Set;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.QueryConf;
import uk.ac.imperial.lsds.saber.QueryOperator;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.WindowDefinition;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.WindowDefinition.WindowType;
import uk.ac.imperial.lsds.saber.cql.expressions.Expression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatDivision;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatExpression;
import uk.ac.imperial.lsds.saber.cql.expressions.floats.FloatMultiplication;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntConstant;
import uk.ac.imperial.lsds.saber.cql.expressions.ints.IntDivision;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.operators.AggregationType;
import uk.ac.imperial.lsds.saber.cql.operators.IAggregateOperator;
import uk.ac.imperial.lsds.saber.cql.operators.IOperatorCode;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Custom_Aggregation;
import uk.ac.imperial.lsds.saber.cql.operators.cpu.Projection;
import uk.ac.imperial.lsds.saber.cql.operators.cpuCodegen.OperatorKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.AggregationKernel;
import uk.ac.imperial.lsds.saber.cql.operators.gpu.ProjectionKernel;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;

public class LRB1 extends LRB {

	public LRB1 (QueryConf queryConf, boolean jni) {
		createSchema ();
		createApplication (queryConf, jni);
	}

	@Override
	public void createApplication (QueryConf queryConf, boolean jni) {

		WindowDefinition window = new WindowDefinition (WindowType.RANGE_BASED, 300, 10);

		/*Expression [] expressions = new Expression [] {

			new  LongColumnReference(0), *//* timestamp *//*
			new   IntColumnReference(1), *//*   vehicle *//*
			new FloatColumnReference(2), *//*     speed *//*
			new   IntColumnReference(3), *//*   highway *//*
			new   IntColumnReference(4), *//*      lane *//*
			new   IntColumnReference(5), *//* direction *//*
			new   IntDivision (
				new IntColumnReference(6), new IntConstant(5280)
			)
		};

		IOperatorCode cpuCode = new Projection (expressions);
		IOperatorCode gpuCode = new ProjectionKernel (schema, expressions, queryConf.getBatchSize(), 1);*/


		// Code for computing the Distinct function over the window
		AggregationType [] aggregationTypes = new AggregationType [1];
		aggregationTypes[0] = AggregationType.fromString("min");//"cnt");

		FloatColumnReference [] aggregationAttributes = new FloatColumnReference [1];
		aggregationAttributes[0] = new FloatColumnReference(2);//1);

		Expression [] groupByAttributes = new Expression [] {
				new IntColumnReference(1)
		};

		IOperatorCode cpuCode = (jni) ?
                ((SystemConf.GENERATE) ?
                        new OperatorKernel(window, null, null, aggregationTypes, aggregationAttributes, groupByAttributes, null, null, schema) :
                        new Custom_Aggregation (window, aggregationTypes, aggregationAttributes, groupByAttributes) )
                : new Aggregation (window, aggregationTypes, aggregationAttributes, groupByAttributes);

		System.out.println(cpuCode);
		IOperatorCode gpuCode = null; /*new AggregationKernel
                (window, aggregationTypes, aggregationAttributes, groupByAttributes, schema, queryConf.getBatchSize());*/

		QueryOperator operator;
		operator = new QueryOperator (cpuCode, gpuCode);

		Set<QueryOperator> operators = new HashSet<QueryOperator>();
		operators.add(operator);

		long timestampReference = System.nanoTime();

		Query query = new Query (0, operators, schema, window, null, null, queryConf, timestampReference);

		query.setName("SegSpeedStr");
		query.setSQLExpression("SELECT DISTINCT car_id " +
				"FROM SegSpeedStr [RANGE 30 SECONDS];");

		Set<Query> queries = new HashSet<Query>();
		queries.add(query);

		this.application = new QueryApplication(queries);

		application.setup();

		/* The path is query -> dispatcher -> handler -> aggregator */
		if (SystemConf.CPU)
			query.setAggregateOperator((IAggregateOperator) cpuCode);
		else
			query.setAggregateOperator((IAggregateOperator) gpuCode);

		return;
	}
}
