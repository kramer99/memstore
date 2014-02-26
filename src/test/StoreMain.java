import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.kramer.Memstore;

public class StoreMain {
	
//	private static String STORE_LOCATION = "h:/workspace/memstore/store.dat";
	private static String STORE_LOCATION = "c:/opt/store";
	
	private Memstore store;
	
	public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
		StoreMain sm = new StoreMain();
		sm.testWrite();
		sm.testRead();
		sm.testBigWrite();
	}

	public StoreMain() throws IOException {
		store = new Memstore(STORE_LOCATION);
	}

	public void testWrite() throws FileNotFoundException, IOException {
		long start = System.currentTimeMillis();
		store.put("string", "fdsgdfsg");
		store.serialize();
		long end = System.currentTimeMillis();
		System.out.println("testWrite: " + (end-start) + "ms");
	}

	public void testRead() throws FileNotFoundException, IOException, ClassNotFoundException {
		long start = System.currentTimeMillis();
		String s = (String)store.get("string");
		long end = System.currentTimeMillis();
		System.out.println("testRead: " + (end-start) + "ms");
	}

	public void testBigWrite() throws FileNotFoundException, IOException, ClassNotFoundException {
		long start = System.currentTimeMillis();
		List<String> bigData = new ArrayList<String>();
		for (int i=0; i < 100000; i++) {
			bigData.add(new String("String #: " + i));
		}
		long mid = System.currentTimeMillis();
		store.put("bigData", bigData);
		store.serialize();
		long end = System.currentTimeMillis();
		System.out.println("testBigWrite: " + (end-start) + "ms (data created: " + (mid-start) + "ms)");
	}

}
