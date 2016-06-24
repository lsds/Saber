package uk.ac.imperial.lsds.saber.www.helpers;

import java.util.HashMap;
import java.util.Map;

public class GraphQuery implements GraphElement {
	
	private static String defaultType = "graph_type_query";
	
	private String    id;
	private String  type;
	private String  name;
	private String query;
	
	public GraphQuery () {
		id = null;
		type = defaultType;
		name = query = null;
	}
	
	public GraphQuery setId (int number) {
		this.id = String.format("%d", number);
		return this;
	}
	
	public GraphQuery setType (String type) {
		this.type = type;
		return this;
	}
	
	public GraphQuery setName (String name) {
		this.name = name;
		return this;
	}
	
	public GraphQuery setQuery (String query) {
		this.query = query;
		return this;
	}
	
	public Map<String, Object> getDetails () {
		
		Map<String, Object> details = new HashMap<String, Object>();
		
		details.put ("id",       id);
		details.put ("type",   type);
		details.put ("name",   name);
		details.put ("query", query);
		
		Map<String, Object> data = new HashMap<String, Object>();
		
		data.put("data", details);
		
		System.out.println(String.format("[DBG] query: id \"%s\" type \"%s\" name \"%s\" query \"%s\"", 
				id, type, name, query));
		
		return data;
	}
	
	public boolean equals (GraphQuery node) {
		return (
			   id.equals (node.id   ) && 
			 type.equals (node.type ) && 
			 name.equals (node.name ) && 
			query.equals (node.query) );
	}
}
