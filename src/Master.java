
import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Master implements Watcher
{
	private static final Logger LOG = LoggerFactory.getLogger(Master.class);
	
	private Random random = new Random(this.hashCode());
	private ZooKeeper _zk;
	private String _hostport;
	private String _serverid = Integer.toHexString(random.nextInt());
	private volatile boolean _leader = false;
	
	Master(String hostport)
	{
		_hostport = hostport;
	}
	
	public void start() throws Exception
	{
		LOG.info("Master start: " + _serverid);
		_zk = new ZooKeeper(_hostport, 15000, this);
	}
	
	public void stop() throws IOException
	{
		LOG.info("Master stop: " + _serverid);
		try
		{
			_zk.close();
		}
		catch (InterruptedException e)
		{
			LOG.warn("ZooKeeper interrupted while closing");
		}
	}
	
	public void RunForMasterAsynchronous()
	{
		LOG.info("Running for Master Asynchronous");
		_zk.create("/master", _serverid.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL
		           , _mastercreatecallbackasynronus, null);
	}
	
	protected void RunForMasterSyncronus() throws InterruptedException, KeeperException
	{
		LOG.info("Running for Master Syncronus");
		while (true)
		{
			try
			{
				_zk.create("/master", _serverid.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				set_leader(true);
				break;
			}
			catch (NodeExistsException e)
			{
				set_leader(false);
				break;
			}
			catch (ConnectionLossException e)
			{
				
			}
			if (true == CheckMasterSyncronus())
			{
				break;
			}
		}
	}
	
	StringCallback _mastercreatecallbackasynronus = new StringCallback()
	{
		@Override
		public void processResult(int rc, String path, Object ctx, String name)
		{
			switch (Code.get(rc))
			{
				case CONNECTIONLOSS:
					CheckMasterAsynchronous();
					break;
					
				case OK:
					_leader = true;
					break;
				
				default:
					_leader = false;
					LOG.error("Something went wrong when running for master."
					          , KeeperException.create(Code.get(rc), path));
					break;
			}
			
		}	
	};
	
	StringCallback _mastercreatecallbacksyncronus = new StringCallback()
	{
		@Override
		public void processResult(int rc, String path, Object ctx, String name)
		{
			switch (Code.get(rc))
			{
				case CONNECTIONLOSS:
					try
					{
						CheckMasterSyncronus();
					}
					catch (KeeperException | InterruptedException e)
					{
						e.printStackTrace();
					}
					break;
					
				case OK:
					_leader = true;
					break;
				
				default:
					_leader = false;
					LOG.error("Something went wrong when running for master."
					          , KeeperException.create(Code.get(rc), path));
					break;
			}
			LOG.info("I'm " + _leader != null ? "" : "not " + "the leader " + _serverid);
		}
		
	};
	
	protected boolean CheckMasterSyncronus() throws KeeperException, InterruptedException
	{
		while (true)
		{
			try
			{
				Stat stat = new Stat();
				byte data[] = _zk.getData("/master", false, stat);
				set_leader(new String(data).equals(_serverid));
				return true;
			}
			catch (NoNodeException e)
			{
				// 마스터가 없으므로 생성을 시도한다.
				return false;
			}
			catch (ConnectionLossException e)
			{
				
			}
		}
	}
	
	protected void CheckMasterAsynchronous()
	{
		_zk.getData("/master", false, _mastercheckcallback, null);
	}
	
	DataCallback _mastercheckcallback = new DataCallback()
	{
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat)
		{
			switch (Code.get(rc))
			{
				case CONNECTIONLOSS:
					CheckMasterAsynchronous();
					break;
					
				case NONODE:
					RunForMasterAsynchronous();
					return;
					
				default:
					break;
			}			
		}
		
	};
	
	public void BootStrap()
	{
		CreateParent("/workers", new byte[0]);
		CreateParent("/assign", new byte[0]);
		CreateParent("/tasks", new byte[0]);
		CreateParent("/status", new byte[0]);
	}
	
	private void CreateParent(String path, byte[] data)
	{
		_zk.create(path, data, Ids.OPEN_ACL_UNSAFE
		           , CreateMode.PERSISTENT, _createparentcallback, data);
	}
	
	StringCallback _createparentcallback = new StringCallback()
	{
		@Override
		public void processResult(int rc, String path, Object ctx, String name)
		{
			switch (Code.get(rc))
			{
				case CONNECTIONLOSS:
					CreateParent(path, (byte[])ctx);
					break;
				
				case OK:
					LOG.info("Parent created: " + path);
					break;
					
				case NODEEXISTS:
					LOG.warn("Parent already registered: " + path);
					break;
				
				default:
					LOG.error("Somthing went wrong: "
					          , KeeperException.create(Code.get(rc), path));
					break;
			}
			
		}	
	};
	
	
	@Override
	public void process(WatchedEvent event) {
		System.out.println(event);
	}
	
	public static void TestStartStopMain(String hostport) throws Exception
	{
		Master m = new Master(hostport);
		m.start();
		Thread.sleep(10000);
		m.stop();		
	}
	
	public static void TestMasterMain(String hostport) throws Exception
	{
		Master m = new Master(hostport);
		m.start();
		
		Thread.sleep(500);
		
		m.BootStrap();
		
		Thread.sleep(500);
	
//		m.RunForMasterSyncronus();
		m.RunForMasterAsynchronous();	
		Thread.sleep(1000);
		
		if (m.is_leader())
		{
			System.out.println("I'm the leader!");
			Thread.sleep(20000);
		}
		else
		{
			System.out.println("Someone else is the leader!");
		}
		
		m.stop();
	}
	
	public static void main(String[] args) throws Exception
	{
	//	TestStartStopMain(args[0]);
		TestMasterMain(args[0]);
	}

	public boolean is_leader() {
		return _leader;
	}

	public void set_leader(boolean _leader) {
		this._leader = _leader;
	}
}
