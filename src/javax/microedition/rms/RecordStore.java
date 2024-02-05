package javax.microedition.rms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Vector;

import com.nokia.mj.impl.fileutils.FileUtility;
import com.nokia.mj.impl.rt.support.ApplicationInfo;
import com.nokia.mj.impl.storage.StorageAttribute;
import com.nokia.mj.impl.storage.StorageEntry;
import com.nokia.mj.impl.storage.StorageException;
import com.nokia.mj.impl.storage.StorageFactory;
import com.nokia.mj.impl.storage.StorageSession;
import com.nokia.mj.impl.utils.Base64;

public class RecordStore
{
	private static String separator = System.getProperty("file.separator");
	private static Vector openRecordStores = new Vector();

	static {
		try {
			FileUtility file = new FileUtility(getRootPath());
			if(!file.exists()) file.mkdir();
		} catch (Exception e) {}
	}

	public static final int AUTHMODE_PRIVATE = 0;
	public static final int AUTHMODE_ANY = 1;
	
	private String name;
	Vector records;
	Vector recordListeners;
	int count;
	private boolean closed;
	private long lastModified;
	private int version;
	private String rootPath;
	private Object sync = new Object();
	private boolean homeSuite;
	private int authmode;
	private boolean writable;

	RecordStore(String aName, String aRootPath, boolean aHomeSuite, boolean existing) throws RecordStoreException, RecordStoreNotFoundException, RecordStoreFullException {
		records = new Vector();
		recordListeners = new Vector(3);
		name = aName;
		rootPath = aRootPath;
		homeSuite = aHomeSuite;
		try {
			if (!existing) {
				count = 1;
				writeIndex();
				openRecordStores.addElement(this);
				return;
			}
			FileUtility file = new FileUtility(rootPath + "idx");
			if(!file.exists()) throw new RecordStoreNotFoundException(name);
			if(file.fileSize() == 0) {
				count = 1;
				writeIndex();
				openRecordStores.addElement(this);
				return;
			}
			DataInputStream dataInputStream = file.openDataInputStream();
			count = dataInputStream.readInt();
			for (int len = dataInputStream.readInt(), i = 0; i < len; ++i) {
				records.addElement(new Integer(dataInputStream.readInt()));
			}
			try {
				if(dataInputStream.available() > 0) {
					lastModified = dataInputStream.readLong();
					version = dataInputStream.readInt();
					authmode = dataInputStream.readInt();
					writable = dataInputStream.readBoolean();
				}
			} catch (IOException e) {
			}
			dataInputStream.close();
			if(!homeSuite && authmode == AUTHMODE_PRIVATE) {
				throw new SecurityException();
			}
			openRecordStores.addElement(this);
		} catch (IOException e) {
			throw new RecordStoreNotFoundException(name);
		}
	}

	public void setMode(int aAuthmode, boolean aWritable) throws RecordStoreException {
		if(closed) throw new RecordStoreNotOpenException();
		if(!homeSuite) throw new SecurityException("Only read operations allowed");
		if(aAuthmode < 0 || aAuthmode > 1) throw new IllegalArgumentException("Access mode is invalid");
		authmode = aAuthmode;
		writable = aWritable;
		writeIndex();
	}

	public long getLastModified() throws RecordStoreNotOpenException {
//		try {
//			FileUtility file = new FileUtility(rootPath + "idx");
//			if(file.exists()) return file.lastModified();
//		} catch (Exception e) {
//		}
		return lastModified;
	}

	public int getVersion() throws RecordStoreNotOpenException {
		return version;
	}

	public String getName() throws RecordStoreNotOpenException {
		return name;
	}

	public int getNextRecordID() throws RecordStoreNotOpenException, RecordStoreException {
		return count;
	}

	public int getRecordSize(int recordId) throws RecordStoreNotOpenException, InvalidRecordIDException, RecordStoreException {
		if (!recordIdExists(recordId)) {
			throw new InvalidRecordIDException();
		}
		try {
			return (int)new FileUtility(rootPath + recordId + ".rms").fileSize();
		} catch (Exception e) {
			throw new RecordStoreException(e.toString());
		}
	}

	public int getSize() throws RecordStoreNotOpenException {
		int n = 0;
		try {
			for (int i = 0; i < records.size(); ++i) {
				n += getRecordSize(((Integer)records.elementAt(i)).intValue());
			}
		} catch(RecordStoreNotOpenException e) {
			throw e;
		} catch (Exception e) {
			throw new RecordStoreNotOpenException(e.toString());
		}
		return n;
	}

	public int getSizeAvailable() throws RecordStoreNotOpenException {
		try {
			return (int) new FileUtility(rootPath).availableSize();
		} catch (Exception e) {
			return 32000000;
		}
	}

	public RecordEnumeration enumerateRecords(RecordFilter recordFilter, RecordComparator recordComparator, boolean keepUpdated) {
		return new RecordEnumerationImpl(this, sync, recordFilter, recordComparator, keepUpdated);
	}

	public static RecordStore openRecordStore(String aName, boolean createIfNecessary) throws RecordStoreException, RecordStoreFullException, RecordStoreNotFoundException {
		return openRecordStore(aName, createIfNecessary, 0, true);
	}

	public static RecordStore openRecordStore(String aName, boolean createIfNecessary, int authmode, boolean writable) throws RecordStoreException, RecordStoreFullException, RecordStoreNotFoundException {
		if(aName.length() > 32 || aName.length() < 1) throw new IllegalArgumentException("Record store name is invalid");
		String rootPath = getRootPath() + encodeStoreName(aName) + separator;
		RecordStore rs = findRecordStore(rootPath);
		if(rs != null) {
			rs.setMode(authmode, writable);
			return rs;
		}
		FileUtility file = new FileUtility(rootPath);
		boolean exists = file.exists() && file.isDirectory() && new FileUtility(rootPath + "idx").exists();
		if (exists || createIfNecessary) {
			rs = new RecordStore(aName, rootPath, true, exists);
			rs.setMode(authmode, writable);
			return rs;
		}
		throw new RecordStoreNotFoundException(aName);
	}

	public static RecordStore openRecordStore(String aName, String vendorName, String suiteName) throws RecordStoreException, RecordStoreFullException, RecordStoreNotFoundException {
		if(aName.length() > 32 || aName.length() < 1) throw new IllegalArgumentException("Record store name is invalid");
		String rootPath = getRootPath(aName, vendorName, suiteName) + encodeStoreName(aName) + separator;
		RecordStore rs = findRecordStore(rootPath);
		if(rs != null) return rs;
		FileUtility file = new FileUtility(rootPath);
		if (!file.exists() || !file.isDirectory() || !new FileUtility(rootPath + "idx").exists()) {
			throw new RecordStoreNotFoundException(aName);
		}
		return new RecordStore(aName, rootPath, vendorName.equals(ApplicationInfo.getInstance().getVendor()) && suiteName.equals(ApplicationInfo.getInstance().getSuiteName()), true);
	}

	public static void deleteRecordStore(String aName) throws RecordStoreException, RecordStoreNotFoundException {
		String rootPath = getRootPath() + encodeStoreName(aName) + separator;
		if(findRecordStore(rootPath) != null) {
			throw new RecordStoreException("Cannot delete currently opened record store: " + aName);
		}
		FileUtility file = new FileUtility(rootPath);
		if(!file.exists()) {
			throw new RecordStoreNotFoundException(aName);
		}
		try {
			FileUtility[] files = file.listFiles();
			for (int i = 0; i < files.length; i++) {
				files[i].delete();
			}
			file.delete();
		} catch (Exception e) {
			throw new RecordStoreException(e.toString());
		}
	}

	public static String[] listRecordStores() {
		// TODO: check if idx file exists
		String[] list = null;
		try {
			FileUtility file = new FileUtility(getRootPath());
			list = file.listFileArray(false);
			if(list != null) {
				for(int i = 0; i < list.length; i++) {
					list[i] = decodeStoreName(list[i].substring(0, list[i].length()-1));
				}
			}
		} catch (Exception e) {
		}
		return list;
	}

	public void deleteRecord(int recordId) throws RecordStoreNotOpenException, InvalidRecordIDException, RecordStoreException {
		if(closed) throw new RecordStoreNotOpenException();
		if(!homeSuite && !writable) throw new SecurityException("Only read operations allowed");
		synchronized(sync) {
			int i = records.indexOf(new Integer(recordId));
			if(i != -1) {
				records.removeElementAt(i);
				modify();
				try {
					FileUtility file = new FileUtility(rootPath + recordId + ".rms");
					if (file.exists()) {
						file.delete();
					}
				} catch (Exception e) { 
					throw new RecordStoreException(e.toString());
				}
				writeIndex();
				recordDeleted(recordId);
				return;
			}
			throw new InvalidRecordIDException();
		}
	}

	public int getRecord(int recordId, byte[] b, int offset) throws RecordStoreNotOpenException, InvalidRecordIDException, RecordStoreException {
		if(closed) throw new RecordStoreNotOpenException();
		if (!recordIdExists(recordId)) {
			throw new InvalidRecordIDException("recordId=" + recordId);
		}
		try {
			FileUtility file = new FileUtility(rootPath + recordId + ".rms");
			DataInputStream dataInputStream = file.openDataInputStream();
			int length = (int)file.fileSize();
			dataInputStream.readFully(b, offset, length);
			dataInputStream.close();
			return length;
		} catch (Exception e) {
			throw new RecordStoreException("recordId=" + recordId);
		}
	}

	public byte[] getRecord(int recordId) throws RecordStoreNotOpenException, InvalidRecordIDException, RecordStoreException {
		if(closed) throw new RecordStoreNotOpenException();
		if (!recordIdExists(recordId)) {
			throw new InvalidRecordIDException("recordId=" + recordId);
		}
		try {
			FileUtility file = new FileUtility(rootPath + recordId + ".rms");
			if (file.fileSize() > 0) {
				byte[] b = new byte[(int)file.fileSize()];
				DataInputStream dis = file.openDataInputStream();
				dis.readFully(b);
				dis.close();
				return b;
			}
			return null;
		} catch (Exception e) {
			throw new RecordStoreException("recordId=" + recordId);
		}
	}

	public int getNumRecords() throws RecordStoreNotOpenException {
		if(closed) throw new RecordStoreNotOpenException();
		return records.size();
	}

	private boolean recordIdExists(int recordId) {
		boolean b = false;
		for (int i = 0; i < records.size(); ++i) {
			if (((Integer)records.elementAt(i)).intValue() == recordId) {
				b = true;
			}
		}
		return b;
	}

	public int addRecord(byte[] data, int offset, int length) throws RecordStoreNotOpenException, RecordStoreException, RecordStoreFullException {
		if(closed) throw new RecordStoreNotOpenException();
		if(!homeSuite && !writable) throw new SecurityException("Only read operations allowed");
		synchronized(sync) {
			records.addElement(new Integer(count));
			modify();
			try {
				FileUtility file = new FileUtility(rootPath);
				if (!file.exists() && !file.isDirectory()) {
					file.mkdir();
				}
				file = new FileUtility(rootPath + count + ".rms");
				if(!file.exists()) file.createNewFile();
				OutputStream fileOutputStream = file.openOutputStream();
				if (data != null) {
					fileOutputStream.write(data, offset, length);
				}
				fileOutputStream.close();
				writeIndex();
			} catch (Exception e) {
				throw new RecordStoreException(e.toString());
			}
			recordAdded(count);
			return count++;
		}
	}

	public void setRecord(int recordId, byte[] data, int offset, int length) throws RecordStoreNotOpenException, RecordStoreException, RecordStoreFullException {
		if(closed) throw new RecordStoreNotOpenException();
		if(!homeSuite && !writable) throw new SecurityException("Only read operations allowed");
		if(recordId == count) {
			addRecord(data, offset, length);
			return;
		}
		if (!recordIdExists(recordId)) {
			throw new InvalidRecordIDException("recordId=" + recordId);
		}
		synchronized(sync) {
			modify();
			try {
				FileUtility file = new FileUtility(rootPath);
				if (!file.exists() && !file.isDirectory()) {
					file.mkdir();
				}
				file = new FileUtility(rootPath + recordId + ".rms");
				if(!file.exists()) file.createNewFile();
				OutputStream fileOutputStream = file.openOutputStream();
				if (data != null) {
					fileOutputStream.write(data, offset, length);
				}
				fileOutputStream.close();
				writeIndex();
			} catch (Exception e) {
				throw new RecordStoreException("recordId=" + recordId);
			}
			recordChanged(recordId);
		}
	}

	public void closeRecordStore() throws RecordStoreNotOpenException, RecordStoreException {
		if(closed) throw new RecordStoreNotOpenException();
		closed = true;
		openRecordStores.removeElement(this);
		if (!recordListeners.isEmpty()) {
			recordListeners.removeAllElements();
		}
		if(homeSuite || writable) writeIndex();
	}

	private void modify() {
		lastModified = System.currentTimeMillis();
		version++;
	}

	private void writeIndex() throws RecordStoreException {
		try {
			FileUtility file = new FileUtility(rootPath);
			if (!file.exists()) {
				file.mkdir();
			}
			file = new FileUtility(rootPath + "idx");
			if(!file.exists()) file.createNewFile();
			DataOutputStream dataOutputStream = file.openDataOutputStream();
			dataOutputStream.writeInt(count);
			dataOutputStream.writeInt(records.size());
			for (int i = 0; i < records.size(); ++i) {
				dataOutputStream.writeInt(((Integer)records.elementAt(i)).intValue());
			}
			dataOutputStream.writeLong(lastModified);
			dataOutputStream.writeInt(version);
			dataOutputStream.writeInt(authmode);
			dataOutputStream.writeBoolean(writable);
			dataOutputStream.close();
		} catch (Exception e) {
			throw new RecordStoreException(name);
		}
	}

	public void addRecordListener(RecordListener listener) {
		if (!recordListeners.contains(listener)) {
			recordListeners.addElement(listener);
		}
	}

	public void removeRecordListener(RecordListener listener) {
		recordListeners.removeElement(listener);
	}

	private void recordChanged(int n) {
		for (int i = 0; i < recordListeners.size(); ++i) {
			((RecordListener)recordListeners.elementAt(i)).recordChanged(this, n);
		}
	}

	private void recordAdded(int n) {
		for (int i = 0; i < recordListeners.size(); ++i) {
			((RecordListener)recordListeners.elementAt(i)).recordAdded(this, n);
		}
	}

	private void recordDeleted(int n) {
		for (int i = 0; i < recordListeners.size(); ++i) {
			((RecordListener)recordListeners.elementAt(i)).recordDeleted(this, n);
		}
	}

	private static String getRootPath() {
		return ApplicationInfo.getInstance().getRootPath() + "rms2" + separator;
	}
	
	private static String getRootPath(String aName, String vendorName, String suiteName) throws RecordStoreNotFoundException {
		StorageSession session = null;
		try {
			StorageAttribute attr = new StorageAttribute("PACKAGE_NAME", suiteName);
			StorageEntry query = new StorageEntry();
			query.addAttribute(attr);

			attr = new StorageAttribute("VENDOR", vendorName);
			query.addAttribute(attr);

			attr = new StorageAttribute("ROOT_PATH", "");
			query.addAttribute(attr);

			session = StorageFactory.createSession();
			session.open();
			StorageEntry[] receivedEntries = session.search("APPLICATION_PACKAGE", query);
			if ((receivedEntries == null) || (receivedEntries.length == 0)) {
				throw new RecordStoreNotFoundException("Record store does not exist: " + aName);
			}
			return receivedEntries[0].getAttribute("ROOT_PATH").getValue() + "rms2" + separator;
		} catch (StorageException se) {
			throw new RecordStoreNotFoundException("Record store does not exist: " + aName);
		} finally {
			if (session != null) {
				session.close();
				session.destroySession();
			}
		}
	}

	private static String encodeStoreName(String filename) {
		try {
			filename = Base64.encode(filename.getBytes("UTF-8"));
			byte[] utf8 = filename.getBytes("UTF-8");
			for (int i = 0; i < utf8.length; i++) {
				if (utf8[i] == (byte) '/') {
					utf8[i] = (byte) '-';
					continue;
				}
//				if (utf8[i] == (byte) '=') {
//					utf8[i] = (byte) '_';
//					continue;
//				}
			}
			filename = new String(utf8, "UTF-8");
		} catch (UnsupportedEncodingException e) {
		}
		return filename;
	}

	private static String decodeStoreName(String filename) {
		try {
			byte[] utf8 = filename.getBytes("UTF-8");
			for (int i = 0; i < utf8.length; i++) {
				if (utf8[i] == (byte) '-') {
					utf8[i] = (byte) '/'; 
					continue;
				}
//				if (utf8[i] == (byte) '_') {
//					utf8[i] = (byte) '=';
//					continue;
//				}
			}
			filename = new String(utf8, "UTF-8");
			byte[] bytes = Base64.decode(filename);
			filename = new String(bytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
		}
		return filename;
	}
	
	private static RecordStore findRecordStore(String rootPath) {
		int num = openRecordStores.size();
		for (int i = 0; i < num; i++) {
			RecordStore rs = (RecordStore) openRecordStores.elementAt(i);
			if (rs.rootPath.equals(rootPath)) {
				return rs;
			}
		}
		return null;
	}

	int[] getRecordIds() throws RecordStoreException {
		if(closed) throw new RecordStoreNotOpenException();
		int[] recordIds = new int[records.size()];
		for(int i = 0; i < recordIds.length; i++) {
			recordIds[i] = ((Integer)records.elementAt(i)).intValue();
		}
		return recordIds;
	}
}
