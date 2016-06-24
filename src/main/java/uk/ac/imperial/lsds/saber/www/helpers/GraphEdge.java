package uk.ac.imperial.lsds.saber.www.helpers;

import java.util.HashMap;
import java.util.Map;

public class GraphEdge implements GraphElement {
	
	private static String defaultType = "graph_edge_defaults";
	
	private static int next = 1;
	
	private String     id;
	private String source;
	private String target;
	private String   type;
	
	public GraphEdge () {
		id = String.format("e%d", next++);
		source = target = null;
		type = defaultType;
	}
	
	public GraphEdge setSource (String source) {
		this.source = source;
		return this;
	}
	
	public GraphEdge setSource (int number) {
		this.source = String.format("%d", number);
		return this;
	}
	
	public GraphEdge setTarget (String target) {
		this.target = target;
		return this;
	}
	
	public GraphEdge setTarget (int number) {
		this.target = String.format("%d", number);
		return this;
	}
	
	public GraphEdge setType (String type) {
		this.type = type;
		return this;
	}

	public Map<String, Object> getDetails() {
		
		Map<String, Object> details = new HashMap<String, Object>();
		
		details.put("streamid",     id);
		details.put("source",   source);
		details.put("target",   target);
		details.put("type",       type);
		
		Map<String, Object> data = new HashMap<String, Object>();
		
		data.put("data", details);
		
		System.out.println(String.format("[DBG] edge: id \"%s\" source \"%s\" target \"%s\" type \"%s\"", 
				id, source, target, type));
		
		return data;
	}
	
	public boolean equals (GraphEdge edge) {
		return (
			    id.equals (edge.id    ) && 
			source.equals (edge.source) && 
			target.equals (edge.target) && 
			  type.equals (edge.type  ) );
	}
}
