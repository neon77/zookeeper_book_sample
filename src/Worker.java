import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Worker implements Watcher
{
	private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
	
	private ZooKeeper _zk;
	private String _hostport;
	private String _serverid = Integer.toHexString((new Random()).nextInt());
	private String _name;
	private String _status;
	
	Worker(String hostport)
	{
		_hostport = hostport;
	}
	
	public void start() throws Exception
	{
		LOG.info("Worker start: " + _serverid);
		_zk = new ZooKeeper(_hostport, 15000, this);
	}
	
	public void stop() throws IOException
	{
		LOG.info("Worker stop: " + _serverid);
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
	
	void Register()
	{
		_name = "worker-" + _serverid;
		_zk.create("/workers/" + _name, "Idle".getBytes()
		           , Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
		           , _createworkercallback, null);
	}
	
	private StringCallback _createworkercallback = new StringCallback()
	{
		@Override
		public void processResult(int rc, String path, Object ctx, String name)
		{
			switch (Code.get(rc))
			{
				case CONNECTIONLOSS:
					Register();
					break;
				
				case OK:
					LOG.info("Registered successfully: " + _serverid);
					break;
					
				case NODEEXISTS:
					LOG.info("Already registered: " + _serverid);
					break;
				
				default:
					LOG.error("Something went wrong: ",
					          KeeperException.create(Code.get(rc), path));
					break;
			}
			
		}
	};
	
	StatCallback _statusupdatecallback = new StatCallback()
	{
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat)
		{
			switch (Code.get(rc))
			{
				case CONNECTIONLOSS:
					UpdateStatus((String)ctx);
					break;
					
				default:
					break;
			}
			
		}	
	};
	
	synchronized private void UpdateStatus(String status)
	{
		if (status == this._status)
		{
			_zk.setData("/wrokers/" + _name, status.getBytes(), -1
			            , _statusupdatecallback, status);
		}
	}
	
	public void set_status(String status)
	{
		this._status = status;
		UpdateStatus(status);
	}
	
	public static void main(String[] args) throws Exception
	{
		Worker w = new Worker(args[0]);
		w.start();
		
		w.Register();
		
		Thread.sleep(20000);
		
		w.stop();
	}
	
}
