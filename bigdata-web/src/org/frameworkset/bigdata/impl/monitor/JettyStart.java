package org.frameworkset.bigdata.impl.monitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;
import org.frameworkset.bigdata.imp.Imp;
import org.frameworkset.runtime.CommonLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

public class JettyStart {
	private static Logger log = LoggerFactory.getLogger(JettyStart.class);
	public JettyStart() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		String adminNode = System.getProperty("adminNode");
		if(adminNode != null && adminNode.equals("false"))
		{
//			NotifiableFactory.getNotifiable().addListener(new HDFSUploadEventHandler(), HDFSUploadData.hdfsuploadevent);
//			List<EventType> monitorEventTypes = new ArrayList<EventType>();
//			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_request_commond);
//			monitorEventTypes.add(HDFSUploadData.hdfs_upload_monitor_response_commond);
//			NotifiableFactory.getNotifiable().addListener(Imp.getImpStaticManager(), monitorEventTypes);
//			org.frameworkset.remote.EventUtils.init();

			Imp.startDataNode();
		}
		else
		{
			try {

				String port = CommonLauncher.getProperty("port", "8080");
				if (port.equals(""))
					port = "8080";
				String contextPath = CommonLauncher.getProperty("context",
						"bigdata");
				if (contextPath.equals(""))
					contextPath = "bigdata";
	
				int p = Integer.parseInt(port);
				Server server = new Server(p);

				WebAppContext context = new WebAppContext();

				context.setDescriptor("./WebRoot/WEB-INF/web.xml");

				context.setResourceBase("./WebRoot");

				context.setContextPath("/" + contextPath);
				context.setParentLoaderPriority(true);
				ContextHandlerCollection contexts = new ContextHandlerCollection();
				contexts.setHandlers(new Handler[] { context });
	
				server.setHandler(contexts);
	
				// server.setHandler(context);

				server.start();
				server.join();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (SAXException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		

	}
	
	public static void setAppdir(File appdir) {
		Imp.setAppdir(appdir);
	}

}
