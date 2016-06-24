package uk.ac.imperial.lsds.saber.www;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import uk.ac.imperial.lsds.saber.QueryApplication;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RESTfulHandler extends AbstractHandler {
	
	private static final ObjectMapper mapper = new ObjectMapper();
	
	private QueryApplicationHandler q;
	private ThroughputHandler [] t;
	
	public RESTfulHandler (QueryApplication application, int limit) {
		q = new QueryApplicationHandler (application);
		int n = application.numberOfQueries();
		t = new ThroughputHandler [n];
		for (int i = 0; i < n; ++i)
			t[i] = new ThroughputHandler (limit);
	}
	
	private int getQueryIndex (String s) throws NumberFormatException {
		int qid = Integer.parseInt(s);
		checkQueryBounds (qid);
		return qid;
	}
	
	private void checkQueryBounds (int idx) {
		if (idx < 0 || idx >= t.length)
			throw new IllegalArgumentException ("error: invalid query index " + idx);
	}
	
	public void addMeasurement (int qid, long timestamp, float cpuValue, float gpuValue) {
		checkQueryBounds(qid);
		t[qid].addMeasurement(timestamp, cpuValue, gpuValue);
	}
	
	public void handle (String target, Request baseRequest, 
			HttpServletRequest request, HttpServletResponse response) 
			throws IOException, ServletException {
		
		response.setContentType("application/json;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		response.setHeader("Access-Control-Allow-Origin", "*");
		response.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
		
		baseRequest.setHandled(true);
		
		String callback = request.getParameter ("callback");
		
		/*
		if (callback == null)
			throw new IllegalStateException ("error: jquery callback is null");
		*/
		
		if (baseRequest.getMethod().equals("GET")) {
			
			String [] s = target.split("/");
		
			assert (s[0].length() == 0);
		
			if (s[1].equals("queries")) {
				if (callback != null)
					response.getWriter().println (
						callback + 
						"(" + 
						mapper.writeValueAsString (q.getAnswer (baseRequest.getParameters())) + 
						")"
					);
				else
					response.getWriter().println (
						mapper.writeValueAsString (q.getAnswer (baseRequest.getParameters()))
					);
				
			} else if (s[1].equals("throughput")) {
				
				/* s[2] is the query id */
				if (s.length != 3)
					throw new IllegalStateException (String.format("error: invalid target (%s)", target));
				
				int idx = getQueryIndex (s[2]);
				
				if (callback != null)
					response.getWriter().println (
						callback + 
						"(" + 
						mapper.writeValueAsString (t[idx].getAnswer (baseRequest.getParameters())) + 
						")"
					);
				else
					response.getWriter().println (
						mapper.writeValueAsString (t[idx].getAnswer (baseRequest.getParameters()))
					);
			} else {
				throw new IllegalStateException (String.format("error: invalid target (%s)", target));
			}
		} else {
			/* POST, etc. */
			throw new IllegalStateException 
				(String.format("error: invalid method request (%s)", baseRequest.getMethod()));
		}
		
		return;
	}

}
