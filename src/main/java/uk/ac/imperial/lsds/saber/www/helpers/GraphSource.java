package uk.ac.imperial.lsds.saber.www.helpers;

import java.util.HashMap;
import java.util.Map;

public class GraphSource implements GraphElement {
	
	public static Map<String, GraphSource> sources = new HashMap<String, GraphSource> ();
	
	private static String defaultType = "graph_type_source";
	
	private static int next = 1;
	
	private String     id;
	private String   type;
	private String   name;
	private String schema;
	
	public GraphSource () {
		id = null;
		type = defaultType;
		name = schema = null;
	}
	
	public GraphSource setId () {
		this.id = String.format("s%d", next++);
		return this;
	}
	
	public String getId() {
		if (id == null)
			throw new NullPointerException ("error: graph source is not initialised");
		return id;
	}
	
	public GraphSource setType (String type) {
		this.type = type;
		return this;
	}
	
	public GraphSource setName (String name) {
		this.name = name;
		return this;
	}
	
	public GraphSource setSchema (String schema) {
		this.schema = schema;
		return this;
	}
	
	public Map<String, Object> getDetails () {
		
		Map<String, Object> details = new HashMap<String, Object>();
		
		details.put ("id",        id);
		details.put ("type",    type);
		details.put ("name",    name);
		details.put ("query", schema);
		
		Map<String, Object> data = new HashMap<String, Object>();
		
		data.put("data", details);
		
		System.out.println(String.format("[DBG] source: id \"%s\" type \"%s\" name \"%s\" schema \"%s\"", 
				id, type, name, schema));
		
		return data;
	}
	
	public String getKey () {
		
		if (name == null || schema == null)
			throw new IllegalStateException ("error: graph element is not initialised");
		
		return String.format("<%s|%s|%s>", type, name, schema);
	}
	
	public boolean equals (GraphSource node) {
		return (
			    id.equals (node.id    ) && 
			  type.equals (node.type  ) && 
			  name.equals (node.name  ) && 
			schema.equals (node.schema) );
	}
}
