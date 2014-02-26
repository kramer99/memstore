import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.kramer.Memstore;

public class StoreTest {
	
//	private static String STORE_LOCATION = "h:/workspace/memstore/store.dat";
	private static String STORE_LOCATION = "c:/opt/store";
	private List<String> bigData;

	public StoreTest() {
		bigData = new ArrayList<String>();
		for (int i=0; i < 100000; i++) {
			bigData.add(new String("String #: " + i));
		}
	}

	@Test
	public void testWrite() throws FileNotFoundException, IOException {
		Memstore store = new Memstore(STORE_LOCATION);
		store.put("string", "fdsgdfsg");
		store.serialize();
	}

	@Test
	public void testWriteSameObjectTwice() throws FileNotFoundException, IOException {
		Memstore store = new Memstore(STORE_LOCATION);
		store.put("key", "first");
		store.put("key", "second");
		assertEquals("second", store.get("key"));
		store.serialize();
		assertEquals("second", store.get("key"));
		store = new Memstore(STORE_LOCATION);
		assertEquals("second", store.get("key"));
	}

	@Test
	public void testWriteSerializeThenWriteAgain() throws FileNotFoundException, IOException {
		Memstore store = new Memstore(STORE_LOCATION);
		store.put("junk", "zzz");
		store.serialize();
		store.put("morejunk", "zzz");
		store.serialize();
	}

	@Test
	public void testBigWrite() throws FileNotFoundException, IOException, ClassNotFoundException {
		Memstore store = new Memstore(STORE_LOCATION);
		store.put("bigData", bigData);
		store.serialize();
		assertEquals(5, store.size());	// [string, key, junk, morejunk, bigData]
	}

}
