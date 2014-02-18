package com.stratio.deep;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.log4j.Logger;

public class DeepSSTablesReader {
    private static Logger logger = Logger.getLogger(DeepSSTablesReader.class);

    /*
     * We match the structure of the regexp that matches the data file. Each
     * data file has the following structure
     * <keyspace_name>-<cf_name>-<sstable_protocol_name>-<filenum>-Data.db
     */
    private static final String DATA_FILE_REGEXP = "^(.*)-(.*)-([\\w]{2})-([\\d]+)-Data\\.db";
    private static final Pattern DATA_FILE_PATTERN = Pattern.compile(DATA_FILE_REGEXP);

    private static DeepSSTablesReader instance;

    private String dataFileLocation;

    private DeepSSTablesReader() {
	DatabaseDescriptor.loadSchemas();

	// we assume there's only 1 data file location
	dataFileLocation = DatabaseDescriptor.getAllDataFileLocations()[0];

	logger.info("Data file location: " + dataFileLocation);
    }

    public static void readData(String keyspace) throws IOException {
	/*
	 * 0. Make a list of all SSTable files in
	 * <i>dataFileLocation/keyspace/columnFamily/*-Data.db</i>
	 */
	String separator = FileSystems.getDefault().getSeparator();
	Path pDataDir = FileSystems.getDefault().getPath(
		instance().dataFileLocation + separator + "crawler_keyspace" + separator + "Page");
	logger.info("Data dir " + pDataDir.toString() + " opened successfully");

	/*
	 * 1. Open the keyspace using the keyspace name.
	 */

	/*
	 * Keyspace ks = Keyspace.open(keyspace);
	 * 
	 * List<SSTableReader> ssTableReaders = ks.getAllSSTables();
	 * SSTableReader reader = ssTableReaders.get(0); SSTableScanner scanner
	 * = reader.getScanner();
	 * 
	 * for ( ; scanner.hasNext() ;) { OnDiskAtomIterator iterator =
	 * scanner.next();
	 * 
	 * DecoratedKey key = iterator.getKey();
	 * logger.info("DecoratedKey: "+key);
	 * 
	 * 
	 * 
	 * for (;iterator.hasNext();) { OnDiskAtom atom = iterator.next(); atom.
	 * } }
	 */

	/*
	 * 2. for each file SSTable file I have to:
	 * a. Create a
	 * org.apache.cassandra.io.sstable.Descriptor for the SSTable file 
	 * b. Open an org.apache.cassandra.io.sstable.SSTableReader using the
	 * descriptor 
	 * c. get an org.apache.cassandra.io.sstable.SSTableScanner
	 * from the SSTableReader 
	 * d. Create an iterator over the SSTable rows
	 * (the same way as
	 * org.apache.cassandra.tools.SSTableExport#serializeRow) 
	 * e. When the
	 * keys are over inside this file skip to the next SSTable file and goto a.
	 */

	try (DirectoryStream<Path> dStream = Files.newDirectoryStream(pDataDir, new DirectoryStream.Filter<Path>() {

	    @Override
	    public boolean accept(Path entry) throws IOException {
		boolean result = DATA_FILE_PATTERN.matcher(entry.getFileName().toString()).matches();
		logger.info("Processed path entry " + entry.getFileName().toString() + ", result: " + result);
		return result;
	    }
	    
	})) {
	    
	    
	    for (Path entry: dStream) {
		Matcher matcher = DATA_FILE_PATTERN.matcher(entry.getFileName().toString());

		if (!matcher.find()) {
		    throw new IOException("filename structure not recognized");
		}

		String keyspaceName = matcher.group(1);
		String cfName = matcher.group(2);
		String protocol = matcher.group(3);
		String sstableIdx = matcher.group(4);

		logger.info("Iterating over: " + entry.getFileName().toString());
		
		logger.info("keyspace: " + keyspaceName + "; cfName: " + cfName + "; protocol: " + protocol
			+ "; sstableIdx: " + sstableIdx);
		
		Descriptor descriptor = Descriptor.fromFilename(entry.toFile().getCanonicalPath());
		
		logger.info(descriptor.baseFilename());
		
		Keyspace ks = Keyspace.open("crawler_keyspace");
		ColumnFamilyStore cfs = ks.getColumnFamilyStore(cfName);
	
	    }
	    
	}
    }

    public static DeepSSTablesReader instance() {
	if (instance == null) {
	    instance = new DeepSSTablesReader();
	}

	return instance;
    }
}
