package uk.ac.imperial.lsds.saber.www;

import java.util.Map;

public interface IHandler {
	
	public Object getAnswer (Map<String, String[]> map);
}
