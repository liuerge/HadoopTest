package cn.odydata.bigdata.zk;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Before;
import org.junit.Test;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class simpleZKClient {

	private static final String connectString = "hadoop01:2181,hadoop02:2181,hadoop03:2181";
	private static final int sessionTimeout = 2000;
	ZooKeeper zkClient = null;

	public void junit() {

	}

	@Before
	public void init() throws Exception {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent event) {
				// 收到事件通知后的回调函数(应该是我们自己的事务逻辑处理)
				System.out.println(event.getType() + "---" + event.getPath());
				try {
					zkClient.getChildren("/", true);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * 数据的增删改查
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	//创建数据节点到zk中
	public void testCreate() throws KeeperException, InterruptedException {
		// 参数1:要创建的节点的路径,参数2:节点数据 参数3:节点的权限 参数4:节点的类型
		String nodeCreate = zkClient.create("/eclipse", "hellozk".getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.PERSISTENT);
		//上传的数据可以是任何类型,但都必须转成byte类型
	}
	
	//判断znode是否存在
	@Test
	public void testExist() throws Exception{
		Stat stat = zkClient.exists("/eclipse", false);
		System.out.println(stat ==null?"not exit":"exit");
	}
	
	
	//获取子节点
	@Test
	public void getChildren() throws KeeperException, InterruptedException {
		List<String> children = zkClient.getChildren("/", true);
		for (String child : children) {
			System.out.println(child);
		}
		Thread.sleep(Long.MAX_VALUE);
	}
	
	//获取znode的数据
	@Test
	public void getData() throws KeeperException, InterruptedException {
//		byte[] data = zkClient.getData("/eclipse", false, null);
		byte[] data = zkClient.getData("/test", false, null);
		System.out.println(new String(data));
	}
	
	//删除znode的数据
		@Test
		public void deleteZnode() throws KeeperException, InterruptedException {
			//参数2:指定要删除的版本 -1表示删除所有版本
			zkClient.delete("/eclipse", -1);
		}
		
		//删除znode的数据
		@Test
		public void setData() throws KeeperException, InterruptedException {
			//参数2:指定要删除的版本 -1表示删除所有版本
			zkClient.setData("/appe", "imissyoumx".getBytes(), -1);
			byte[] data = zkClient.getData("/appe", false, null);
			System.out.println(new String(data));
		}

}
