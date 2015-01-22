import java.io.IOException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Client implements Watcher
{
	private static final Logger LOG = LoggerFactory.getLogger(Client.class);
	
	ZooKeeper _zk;
	String _hostport;
	
	Client(String hostport)
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
	
	String QueueCommand(String command) throws Exception
	{
		while (true)
		{
			String name = null;
			try
			{
				name = _zk.create("/tasks/task-", command.getBytes()
				                         , Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				return name;
			}
			catch (NodeExistsException e)
			{
				throw new Exception(name + " already appears to be running");
			}
			catch (ConnectionLossException e)
			{
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Client c = new Client(args[0]);
		
		c.start();
		
		String name = c.QueueCommand(args[1]);
		System.out.println("Creaeted  " + name);
		
		Thread.sleep(20000);
		
		c.stop();
	}
	
}
