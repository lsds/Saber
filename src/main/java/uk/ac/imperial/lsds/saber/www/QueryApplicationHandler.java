package uk.ac.imperial.lsds.saber.www;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jetty.util.MultiMap;

import uk.ac.imperial.lsds.saber.ITupleSchema;
import uk.ac.imperial.lsds.saber.Query;
import uk.ac.imperial.lsds.saber.QueryApplication;
import uk.ac.imperial.lsds.saber.www.helpers.GraphEdge;
import uk.ac.imperial.lsds.saber.www.helpers.GraphQuery;
import uk.ac.imperial.lsds.saber.www.helpers.GraphSource;

public class QueryApplicationHandler implements IHandler {
	
	private QueryApplication application;
	
	private Map<String, Object> info;

	private List<Object> nodes;
	private List<Object> edges;
	
	public QueryApplicationHandler (QueryApplication application) {
		this.application = application;
			
		info = new HashMap<String, Object>();
		
		nodes = new ArrayList<Object>();
		edges = new ArrayList<Object>();
		
		for (Query q: this.application.getQueries())
			parse (q);
		
		info.put("nodes", nodes);
		info.put("edges", edges);
	}
	
	private void parse (Query q) {
		GraphQuery node = 
				new GraphQuery().setId(q.getId()).setName(q.getName()).setQuery(q.getSQLExpression());
		nodes.add(node.getDetails());
		if (q.isMostUpstream()) {
			tryAdd (q.getId(), q.getFirstSchema() );
			tryAdd (q.getId(), q.getSecondSchema());
		} else {
			for (int i = 0; i < q.getNumberOfUpstreamQueries(); ++i) {
				Query p = q.getUpstreamQuery(i);
				GraphEdge edge = 
						new GraphEdge().setSource(p.getId()).setTarget(q.getId());
				edges.add(edge.getDetails());
			}
		}
	}
	
	public void tryAdd (int id, ITupleSchema s) {
		if (s == null)
			return;
		GraphSource src = new GraphSource().setName(s.getName()).setSchema(s.getSchema());
		String key = src.getKey();
		GraphSource element = GraphSource.sources.get(key);
		if (element != null) {
			/* Add an edge */
			GraphEdge edge = 
					new GraphEdge().setSource(element.getId()).setTarget(id);
			edges.add(edge.getDetails());
		} else {
			GraphSource.sources.put(key, src.setId());
			nodes.add(src.getDetails());
			GraphEdge edge = 
					new GraphEdge().setSource(src.getId()).setTarget(id);
			edges.add(edge.getDetails());
		}
	}
	
	public Object getAnswer (MultiMap<String> requestParameters) {
		return info;
	}
}
