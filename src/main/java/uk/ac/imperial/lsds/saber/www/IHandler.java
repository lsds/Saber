package uk.ac.imperial.lsds.saber.www;

import org.eclipse.jetty.util.MultiMap;

public interface IHandler {
	
	public Object getAnswer (MultiMap<String> params);
}
