import java.io.IOException;
import java.util.Date;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AdminClient implements Watcher
{
	private static final Logger LOG = LoggerFactory.getLogger(AdminClient.class);
	
	ZooKeeper _zk;
	String _hostport;
	
	AdminClient(String hostport)
	{
		this._hostport = hostport;
	}
	
	public void start() throws Exception
	{
		LOG.info("Client start");
		_zk = new ZooKeeper(_hostport, 15000, this);
	}
	
	public void stop() throws IOException
	{
		LOG.info("Client stop");
		try
		{
			_zk.close();
		}
		catch (InterruptedException e)
		{
			LOG.warn("ZooKeeper interrupted while closing");
		}
	}
	
	@Override
	public void process(WatchedEvent event)
	{
		LOG.info(event.toString() + ", " + _hostport);
	}
	
	public void ListState() throws KeeperException, InterruptedException
	{
		try
		{
			Stat stat = new Stat();
			byte masterdata[] = _zk.getData("/master", false, stat);
			Date startdate = new Date(stat.getCtime());
			System.out.println("Master : " + new String(masterdata) + " since "
			                   + startdate);
		}
		catch (NoNodeException e)
		{
			System.out.println("No Master");
		}
		
		System.out.println("Workers:");
		for (String w: _zk.getChildren("/workers", false))
		{
			byte data[] = _zk.getData("/workers/" + w, false, null);
			String state = new String(data);
			System.out.println("\t" + w + ": " + state);		
		}
		
		System.out.println("Tasks:");
		for (String t: _zk.getChildren("/tasks", false))
		{
			System.out.println("\t" + t);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		AdminClient ac = new AdminClient(args[0]);
		
		ac.start();
		
		ac.ListState();
		
		ac.stop();
	}
	
}
