package uk.ac.imperial.lsds.saber.www;

import org.eclipse.jetty.server.Server;

public class RESTfulServer implements Runnable {

	Server server;
	
	public RESTfulServer (int port, RESTfulHandler handler) {
		
		server = new Server (port);
		server.setHandler(handler);
	}
	
	public void run() {
		
		try {
			
			server.start(); server.join();
			
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
