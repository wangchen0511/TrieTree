package chen.trie;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.Assert;
import org.testng.annotations.Test;
import chen.async.write.CreateConnection;

public class TestTrieDisk {

	@Test
	public void basicTest() throws SQLException, InterruptedException {
		Connection conn = new CreateConnection().createConnection();
		PreparedStatement stat = conn
				.prepareStatement("truncate table trietree");
		stat.executeUpdate();
		stat.close();
		final TreeDBWrapper tree = new TreeDBWrapper(new TrieTree());
		for (int i = 0; i < 100; i++) {
			tree.putStr("Hel");
			tree.putStr("He");
			tree.putStr("Ho");
			tree.putStr("World");
		}
		Thread.sleep(1000);
		Statement stmt = conn.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT * FROM trietree";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			long id = rs.getLong("id");
			long count = rs.getLong("count");
			long parentId = rs.getLong("parentId");
			boolean isValid = rs.getBoolean("isValid");
			String ch = rs.getString("ch");
			System.out.println("Id: " + id + " parentId: " + parentId
					+ " count: " + count + "char: " + ch + "isValid: "
					+ isValid);

			switch ((int) id) {
			case 1:
				Assert.assertEquals(count, 0);
				Assert.assertEquals(parentId, 0);
				Assert.assertEquals(isValid, false);
				Assert.assertEquals(ch, "H");
				break;
			case 2:
				Assert.assertEquals(count, 100);
				Assert.assertEquals(parentId, 1);
				Assert.assertEquals(isValid, true);
				Assert.assertEquals(ch, "e");
				break;
			case 3:
				Assert.assertEquals(count, 100);
				Assert.assertEquals(parentId, 2);
				Assert.assertEquals(isValid, true);
				Assert.assertEquals(ch, "l");
				break;
			case 4:
				Assert.assertEquals(count, 100);
				Assert.assertEquals(parentId, 1);
				Assert.assertEquals(isValid, true);
				Assert.assertEquals(ch, "o");
				break;
			default:
				break;
			}
		}
		stmt.close();
		rs.close();
		conn.close();
	}

	@Test
	public void testMultiThreadDisk() throws IOException, InterruptedException,
			SQLException {
		Connection conn = new CreateConnection().createConnection();
		PreparedStatement stat = conn
				.prepareStatement("truncate table trietree");
		stat.executeUpdate();
		stat.close();
		
		final TreeDBWrapper tree = new TreeDBWrapper(new TrieTree());
		final CountDownLatch start = new CountDownLatch(6);
		final CountDownLatch end = new CountDownLatch(6);
		ExecutorService exec = Executors.newCachedThreadPool();
		exec.submit(new Runnable() {

			@Override
			public void run() {
				start.countDown();
				try {
					start.await();
					for (int i = 0; i < 100; i++) {
						tree.putStr("Hel");
					}
					end.countDown();
					end.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});

		exec.submit(new Runnable() {

			@Override
			public void run() {
				start.countDown();
				try {
					start.await();
					for (int i = 0; i < 100; i++) {
						tree.putStr("He");
					}
					end.countDown();
					end.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});

		exec.submit(new Runnable() {

			@Override
			public void run() {
				start.countDown();
				try {
					start.await();
					for (int i = 0; i < 100; i++) {
						tree.putStr("Ho");
					}
					tree.putStr("World");
					end.countDown();
					end.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});

		exec.submit(new Runnable() {

			@Override
			public void run() {
				start.countDown();
				try {
					start.await();
					for (int i = 0; i < 1; i++) {
						List<ResultEntry> strs = tree.matchedStr("H");
						System.out.println("Match H");
						for (ResultEntry entry : strs) {
							System.out.println("String is " + entry.getStr()
									+ " number is " + entry.getCounter());
						}
					}
					end.countDown();
					end.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});

		exec.submit(new Runnable() {

			@Override
			public void run() {
				start.countDown();
				try {
					start.await();
					for (int i = 0; i < 1; i++) {
						List<ResultEntry> strs = tree.matchedStr("He");
						System.out.println("Match He");
						for (ResultEntry entry : strs) {
							System.out.println("String is " + entry.getStr()
									+ " number is " + entry.getCounter());
						}
					}
					end.countDown();
					end.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		});
		start.countDown();
		start.await();
		end.countDown();
		end.await();
		List<ResultEntry> strs = tree.matchedStr("H");
		System.out.println("Fianl Match H");
		for (ResultEntry entry : strs) {
			System.out.println("String is " + entry.getStr() + " number is "
					+ entry.getCounter());
		}
		Thread.sleep(1000);
		tree.close();
		

		Statement stmt = conn.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		String sql = "SELECT * FROM trietree";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			long id = rs.getLong("id");
			long count = rs.getLong("count");
			long parentId = rs.getLong("parentId");
			boolean isValid = rs.getBoolean("isValid");
			String ch = rs.getString("ch");
			System.out.println("Id: " + id + " parentId: " + parentId
					+ " count: " + count + "char: " + ch + "isValid: "
					+ isValid);

			switch ((int) id) {
			case 1:
				Assert.assertEquals(count, 0);
				Assert.assertEquals(parentId, 0);
				Assert.assertEquals(isValid, false);
				Assert.assertEquals(ch, "H");
				break;
			case 2:
				Assert.assertEquals(count, 100);
				Assert.assertEquals(parentId, 1);
				Assert.assertEquals(isValid, true);
				Assert.assertEquals(ch, "e");
				break;
			case 3:
				Assert.assertEquals(count, 100);
				Assert.assertEquals(parentId, 2);
				Assert.assertEquals(isValid, true);
				Assert.assertEquals(ch, "l");
				break;
			case 4:
				Assert.assertEquals(count, 100);
				Assert.assertEquals(parentId, 1);
				Assert.assertEquals(isValid, true);
				Assert.assertEquals(ch, "o");
				break;
			default:
				break;
			}
		}
		stmt.close();
		rs.close();
		conn.close();
	}
	
	@Test
	public void testLoadDB() throws SQLException{
		TrieTree tree = TreeDBWrapper.loadTrieFromDB("test", "jdbc:mysql://localhost", "trietree");
		List<ResultEntry> strs = tree.matchedStr("H");
		System.out.println("Fianl Match H");
		for (ResultEntry entry : strs) {
			System.out.println("String is " + entry.getStr() + " number is "
					+ entry.getCounter());
		}
		strs = tree.matchedStr("W");
		System.out.println("Fianl Match W");
		for (ResultEntry entry : strs) {
			System.out.println("String is " + entry.getStr() + " number is "
					+ entry.getCounter());
		}
	}

}
