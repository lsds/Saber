package uk.ac.imperial.lsds.saber.experiments.benchmarks.yahoo.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

//import com.google.common.collect.ArrayListMultimap;
//import com.google.common.collect.Multimap;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.SystemConf;
import uk.ac.imperial.lsds.saber.TupleSchema;
import uk.ac.imperial.lsds.saber.TupleSchema.PrimitiveType;
import uk.ac.imperial.lsds.saber.buffers.IQueryBuffer;
import uk.ac.imperial.lsds.saber.buffers.RelationalTableQueryBuffer;
import uk.ac.imperial.lsds.saber.cql.expressions.longlongs.LongLongColumnReference;
import uk.ac.imperial.lsds.saber.cql.expressions.longs.LongColumnReference;
import uk.ac.imperial.lsds.saber.cql.predicates.IPredicate;
import uk.ac.imperial.lsds.saber.processors.HashMap;

public class CampaignGenerator {
	
	public int adsPerCampaign = 0;
	public ITupleSchema campaignsSchema = null; 
	public IQueryBuffer relationBuffer = null;
	public HashMap hashMap = null;
	long [][] adsArray = null;
	
	private boolean isV2 = false;

	public CampaignGenerator (int adsPerCampaign, IPredicate joinPredicate) {
		this(adsPerCampaign, joinPredicate, false);
	}
	
	public CampaignGenerator (int adsPerCampaign, IPredicate joinPredicate, boolean isV2) {
		
		this.adsPerCampaign = adsPerCampaign;
		this.campaignsSchema = isV2? createCampaignsSchemaV2() : createCampaignsSchema();
		this.adsArray = new long [100 * adsPerCampaign][2];
		this.isV2 = isV2;
		
		/* Generate the campaigns and their ads*/
		if (isV2)
			generateBufferV2();
		else
			generateBuffer();
		
		/* Create Hash Table*/
		int column = isV2?((LongColumnReference) joinPredicate.getSecondExpression()).getColumn() : 
							((LongLongColumnReference) joinPredicate.getSecondExpression()).getColumn();
		int offset = campaignsSchema.getAttributeOffset(column);
		createHashMap(relationBuffer, offset);
	}
	
	public CampaignGenerator (int adsPerCampaign, IPredicate joinPredicate, ByteBuffer campaigns) {
		this(adsPerCampaign, joinPredicate, campaigns, false);
	}
	
	public CampaignGenerator (int adsPerCampaign, IPredicate joinPredicate, ByteBuffer campaigns, boolean isV2) {
		
		this.adsPerCampaign = adsPerCampaign;
		this.campaignsSchema = createCampaignsSchema();
		this.adsArray = new long [100 * adsPerCampaign][2];
		
		/* Generate the campaigns and their ads*/
		fillAdsArray(campaigns); 
		//generateBufferIncrementally();
		
		/* Create Hash Table*/
		int column = ((LongColumnReference) joinPredicate.getSecondExpression()).getColumn(); // ((LongLongColumnReference) joinPredicate.getSecondExpression()).getColumn();
		int offset = campaignsSchema.getAttributeOffset(column);
		createHashMap(relationBuffer, offset);
	}

	/* 100 = The number of campaigns to generate events for */
	public static ITupleSchema createCampaignsSchema () {
		
		int [] offsets = new int [2];
		
		offsets[0] =  0; /* Ad Id:   	   uuid */
		offsets[1] = 16; /* Campaign Id:   uuid	*/
				
		ITupleSchema schema = new TupleSchema (offsets, 32);
		
		/* 0:undefined 1:int, 2:float, 3:long, 4:longlong */
		//schema.setAttributeType (0, PrimitiveType.LONG );
		schema.setAttributeType (0, PrimitiveType.LONGLONG  );
		schema.setAttributeType (1, PrimitiveType.LONGLONG  );
		
		//schema.setAttributeName (0, "timestamp");
		schema.setAttributeName (0, "ad_id");
		schema.setAttributeName (1, "campaign_id");
		
		//schema.setName("Campaigns");
		return schema;
	}
	
	public static ITupleSchema createCampaignsSchemaV2 () {	
		int [] offsets = new int [2];	
		offsets[0] =  0; 
		offsets[1] = 8; 				
		ITupleSchema schema = new TupleSchema (offsets, 16);
		schema.setAttributeType (0, PrimitiveType.LONG );
		schema.setAttributeType (1, PrimitiveType.LONG  );		
		schema.setAttributeName (0, "ad_id");
		schema.setAttributeName (1, "campaign_id");		
		return schema;
	}
	
	public void generateBuffer () {
		
		/* Reset tuple size */
		int campaignsTupleSize = campaignsSchema.getTupleSize();
		
		/* set the size of the relational table*/
		SystemConf.RELATIONAL_TABLE_BUFFER_SIZE = campaignsTupleSize * 100 * adsPerCampaign;
		
		byte [] data = new byte [campaignsTupleSize * 100 * adsPerCampaign];		
		ByteBuffer b1 = ByteBuffer.wrap(data);
		
		/* Fill the buffer */
		int i;
		int value = 0;
		int value2 = 0;
		UUID ad_id, campaign_id;
		while (b1.hasRemaining()) {
			
			campaign_id = new UUID(0L, value2%100);
			//campaign_id = UUID.randomUUID();

			for (i = 0; i < adsPerCampaign; i++){   						// every campaign has 10 ads
				
				ad_id = new UUID(0L, value);
				//ad_id = UUID.randomUUID();

				b1.putLong(ad_id.getMostSignificantBits());
				b1.putLong(ad_id.getLeastSignificantBits());				// ad_id	
				
				// fill the array with all the possible ads
				this.adsArray[value][0] = ad_id.getMostSignificantBits();
				this.adsArray[value][1] = ad_id.getLeastSignificantBits();
				
				b1.putLong(campaign_id.getMostSignificantBits());				
				b1.putLong(campaign_id.getLeastSignificantBits());			// campaign_id
				
				// padding
				b1.put(this.campaignsSchema.getPad());
				
				value++;
			}
			value2++;
		}
		
		this.relationBuffer = new RelationalTableQueryBuffer(0, SystemConf.RELATIONAL_TABLE_BUFFER_SIZE, false);
		
		this.relationBuffer.put(data, data.length);		
	}
	
	public void generateBufferV2 () {
		
		/* Reset tuple size */
		int campaignsTupleSize = campaignsSchema.getTupleSize();
		
		/* set the size of the relational table*/
		SystemConf.RELATIONAL_TABLE_BUFFER_SIZE = campaignsTupleSize * 100 * adsPerCampaign;
		
		byte [] data = new byte [campaignsTupleSize * 100 * adsPerCampaign];		
		ByteBuffer b1 = ByteBuffer.wrap(data);
		
		/* Fill the buffer */
		int i;
		int value = 0;
		int value2 = 0;
		long ad_id, campaign_id;
		while (b1.hasRemaining()) {
			
			campaign_id = (value2);
			value2 ++;
			for (i = 0; i < adsPerCampaign; i++){
				ad_id = value;
				b1.putLong(ad_id);			
				this.adsArray[value][0] = 0L;
				this.adsArray[value][1] = ad_id;
				b1.putLong(campaign_id);			
				// padding
				b1.put(this.campaignsSchema.getPad());
				
				value++;
			}
		}
		
		this.relationBuffer = new RelationalTableQueryBuffer(0, SystemConf.RELATIONAL_TABLE_BUFFER_SIZE, false);
		
		this.relationBuffer.put(data, data.length);		
	}
	
	private void fillAdsArray(ByteBuffer campaigns) {
		/* Reset tuple size */
		int campaignsTupleSize = campaignsSchema.getTupleSize();
		
		/* set the size of the relational table*/
		SystemConf.RELATIONAL_TABLE_BUFFER_SIZE = campaignsTupleSize * 100 * adsPerCampaign;
		
		byte [] data = campaigns.array();
		
		/* Fill the array */			
		for (int i = 0 ; i < 100 * adsPerCampaign; i++) {
			// fill the array with all the possible ads
			this.adsArray[i][0] = campaigns.getLong();
			this.adsArray[i][1] = campaigns.getLong();
			
			campaigns.position(campaigns.position() + 16);			
		}
		
		this.relationBuffer = new RelationalTableQueryBuffer(0, SystemConf.RELATIONAL_TABLE_BUFFER_SIZE, false);
		
		this.relationBuffer.put(data, data.length);	
	}
	
	public void createHashMap(IQueryBuffer relationBuffer, int offset) {
		
		this.hashMap = new HashMap();		
		byte[] buffer = relationBuffer.getByteBuffer().array();		
		int tupleSize = campaignsSchema.getTupleSize();
		
		byte[] b = isV2? new byte[8] : new byte[16];
		
		int endIndex = SystemConf.RELATIONAL_TABLE_BUFFER_SIZE; //batch1.getBufferEndPointer();		
		int i = 0;
		int j = 0;
		while ( i < endIndex) {
			
			while (j < b.length) {
				b[j] = buffer[i + offset + j];
				j += 1;
			}
			j = 0;
			
			this.hashMap.register(Arrays.copyOf(b, b.length), i);
			i += tupleSize;			
		}		
	}
	
	public int getAdsPerCampaignNumber () {
		return this.adsPerCampaign;
	}
	
	public ITupleSchema getCampaignsSchema () {
		return this.campaignsSchema;
	}
	
	public IQueryBuffer getRelationBuffer () {
		return this.relationBuffer;
	}
	
	public HashMap getHashMap () {
		return this.hashMap;
	}
	
	public long [][] getAds () {
		return this.adsArray;
	}
}
