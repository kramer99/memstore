package org.kramer;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Memstore 
{
	private Log log = LogFactory.getLog(getClass());

	private ConcurrentHashMap<String,Object> data = new ConcurrentHashMap<String,Object>();
	private ConcurrentHashMap<String,Object> journal = new ConcurrentHashMap<String,Object>();
	private File journalFile; 
	private File dataFile;
	private ObjectOutputStream journalStream;
//	private ObjectOutputStream dataStream;
	private String fileBase;
	
	// true when: after serialize() has been called but before put() has been called
	private boolean serialized;
	
	// TODO: prevent attempts to instantiate Store with the same file as an already instantiated instance
	public Memstore(String fileBase) throws IOException {
		this.fileBase = fileBase;
		dataFile = new File(fileBase + ".data");
		if (dataFile.exists()) {
			log.info("loading existing data into memory store.");
			loadData();
		} else {
			log.info("created new memory store: " + fileBase + ".data");
		}
	}

	private void openJournal() throws IOException {
		log.info("opening journal for output.");
		journalFile = new File(fileBase + ".journal");
		if (journalFile.exists()) {
			log.warn("store was not serialized before last termination. syncing the journal file now.");
			ObjectInputStream in = new ObjectInputStream(new FileInputStream(journalFile));
			try {
				journal = (ConcurrentHashMap<String,Object>)in.readObject();
				journalStream = new ObjectOutputStream(new FileOutputStream(journalFile));
				serialize();
			} catch (EOFException e) {
				log.debug("the journal is empty, so do nothing.");
				serialized = true;
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);	// will never happen
			}
		} else {
			journalStream = new ObjectOutputStream(new FileOutputStream(journalFile));
		}
	}
	
	// TODO: should this be synchronized?
	public void put(String key, Object value) throws IOException {
		log.debug("inserting key: " + key);
		if (serialized) {
			log.debug("inserting more entries after previous serialization.");
			journalStream = new ObjectOutputStream(new FileOutputStream(journalFile));
			serialized = false;
		} else if (journalStream == null) {
			// journal doesn't need to be opened until the first write
			openJournal();
		}
		journal.put(key, value);
		journalStream.writeObject(journal);
	}
	
	public Object get(String key) {
		Object fromJournal = journal.get(key);
		if (fromJournal != null) {
			log.debug("getting entry: '" + key + "' from journal.");
			return fromJournal;
		} else {
			log.debug("getting entry: '" + key + "' from main data.");
			return data.get(key);
		}
	}
	
	public int size() {
		return data.size();
	}
		
	/**
	 * Append the contents of the journal to the existing data, which is
	 * then serialized to disk.  Journal is then deleted.  This should be
	 * called infrequently due to it's blocking nature, ie: store all the
	 * data you need to store, then call this once.
	 * 
	 * @throws IOException
	 */
	public synchronized void serialize() throws IOException {
		log.debug("serializing.");
		data.putAll(journal);
		ObjectOutputStream dataStream = new ObjectOutputStream(new FileOutputStream(dataFile));
		dataStream.writeObject(data);
		dataStream.close();
		journalStream.close();
		boolean deleted = journalFile.delete();
		if (!deleted)
			log.warn("couldn't delete the journal file after serialization.");
		serialized = true;
	}
	
	@SuppressWarnings("unchecked")
	private void loadData() throws IOException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(dataFile));
		try {
			data = (ConcurrentHashMap<String,Object>)in.readObject();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);	// will never happen
		}
	}
	
}
