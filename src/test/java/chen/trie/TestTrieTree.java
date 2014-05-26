package chen.trie;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestTrieTree {

	@Test
	public void testTrieTree() throws InterruptedException {
		final TrieTree tree = new TrieTree();
		final CountDownLatch start = new CountDownLatch(6);
		final CountDownLatch end = new CountDownLatch(6);
		ExecutorService exec = Executors.newCachedThreadPool();
		exec.submit(new Runnable() {

			@Override
			public void run() {
				start.countDown();
				try {
					start.await();
					for (int i = 0; i < 10000; i++) {
						tree.putStr("Hello");
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
					for (int i = 0; i < 10000; i++) {
						tree.putStr("Hello wolrd");
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
					for (int i = 0; i < 10000; i++) {
						tree.putStr("Hallo olrd");
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
					for (int i = 0; i < 10000; i++) {
						List<ResultEntry> strs = tree.matchedStr("H");
						System.out.println("Match H");
						for(ResultEntry entry : strs){
							System.out.println("String is " + entry.getStr() + " number is " + entry.getCounter());
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
					for (int i = 0; i < 10000; i++) {
						List<ResultEntry> strs = tree.matchedStr("He");
						System.out.println("Match He");
						for(ResultEntry entry : strs){
							System.out.println("String is " + entry.getStr() + " number is " + entry.getCounter());
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
		for(ResultEntry entry : strs){
			System.out.println("String is " + entry.getStr() + " number is " + entry.getCounter());
		}
	}

}
