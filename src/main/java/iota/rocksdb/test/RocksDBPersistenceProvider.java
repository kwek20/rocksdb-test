package iota.rocksdb.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.Range;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SizeApproximationFlag;
import org.rocksdb.Slice;
import org.rocksdb.SstFileManager;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import iota.rocksdb.test.persist.Indexable;
import iota.rocksdb.test.persist.Persistable;
import iota.rocksdb.test.util.IotaIOUtils;

public class RocksDBPersistenceProvider {
    
    Logger log = LoggerFactory.getLogger(RocksDBPersistenceProvider.class);

    private static final int BLOOM_FILTER_BITS_PER_KEY = 10;


    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private final SecureRandom seed = new SecureRandom();

    private final String dbPath;
    private final String logPath;
    private String configPath;
    
    private final int cacheSize;
    private final Map<String, Class<? extends Persistable>> columnFamilies;
    private final Map.Entry<String, Class<? extends Persistable>> metadataColumnFamily;

    private Map<Class<?>, ColumnFamilyHandle> classTreeMap;
    private Map<Class<?>, ColumnFamilyHandle> metadataReference = Collections.emptyMap();

    private RocksDB db;
    // DBOptions is only used in initDB(). However, it is closeable - so we keep a reference for shutdown.
    private DBOptions options;
    private BloomFilter bloomFilter;
    private boolean available;
    
    private SstFileManager sstFileManager;
    private Cache cache, compressedCache;
    private ColumnFamilyOptions columnFamilyOptions;
    private Statistics statistics;
    
    /**
     * Creates a new RocksDB provider without reading from a configuration file
     * 
     * @param dbPath The location where the database will be stored
     * @param logPath The location where the log files will be stored
     * @param cacheSize the size of the cache used by the database implementation
     * @param columnFamilies A map of the names related to their Persistable class
     * @param metadataColumnFamily Map of metadata used by the Persistable class, can be <code>null</code>
     */
    public RocksDBPersistenceProvider(String dbPath, String logPath, int cacheSize,
            Map<String, Class<? extends Persistable>> columnFamilies,
            Map.Entry<String, Class<? extends Persistable>> metadataColumnFamily) {
        this(dbPath, logPath, null, cacheSize, columnFamilies, metadataColumnFamily);
    }
    
    /**
     * Creates a new RocksDB provider by reading the configuration to be used in this instance from a file
     * 
     * @param dbPath The location where the database will be stored
     * @param logPath The location where the log files will be stored
     * @param configPath The location where the RocksDB config is read from
     * @param cacheSize the size of the cache used by the database implementation
     * @param columnFamilies A map of the names related to their Persistable class
     * @param metadataColumnFamily Map of metadata used by the Persistable class, can be <code>null</code>
     */
    public RocksDBPersistenceProvider(String dbPath, String logPath, String configPath, int cacheSize,
                                      Map<String, Class<? extends Persistable>> columnFamilies,
                                      Map.Entry<String, Class<? extends Persistable>> metadataColumnFamily) {
        this.dbPath = dbPath;
        this.logPath = logPath;
        this.cacheSize = cacheSize;
        this.columnFamilies = columnFamilies;
        this.metadataColumnFamily = metadataColumnFamily;
        this.configPath = configPath;

    }

    public void init() throws Exception {
        log.info("Initializing Database on " + dbPath);
        initDB(dbPath, logPath, configPath, columnFamilies);
        available = true;
        log.info("RocksDB persistence provider initialized.");
    }

    public boolean isAvailable() {
        return this.available;
    }


    public void shutdown() {
        if (db != null) {
            try (FlushOptions option = new FlushOptions().setAllowWriteStall(true).setWaitForFlush(true)){
                
                db.flush(option);
                db.syncWal();
                db.flushWal(true);
                db.closeE();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            IotaIOUtils.closeQuietly(columnFamilyHandle);
        }
        IotaIOUtils.closeQuietly(db, options, bloomFilter, cache, compressedCache, columnFamilyOptions, statistics);
    }

    public boolean save(Persistable thing, Indexable index) throws Exception {
        System.out.println("Persisting: " + thing);
        System.out.println("Index: " + index);
        
        ColumnFamilyHandle handle = classTreeMap.get(thing.getClass());
        db.put(handle, index.bytes(), thing.bytes());

        ColumnFamilyHandle referenceHandle = metadataReference.get(thing.getClass());
        if (referenceHandle != null) {
            db.put(referenceHandle, index.bytes(), thing.metadata());
        }
        return true;
    }

    public Persistable get(Class<?> model, Indexable index) throws Exception {
        Persistable object = (Persistable) model.newInstance();
        object.read(db.get(classTreeMap.get(model), index == null ? new byte[0] : index.bytes()));

        ColumnFamilyHandle referenceHandle = metadataReference.get(model);
        if (referenceHandle != null) {
            object.readMetadata(db.get(referenceHandle, index == null ? new byte[0] : index.bytes()));
        }
        return object;
    }

    public boolean mayExist(Class<?> model, Indexable index) {
        ColumnFamilyHandle handle = classTreeMap.get(model);
        return db.keyMayExist(handle, index.bytes(), new StringBuilder());
    }

    public long count(Class<?> model) throws Exception {
        return getCountEstimate(model);
    }

    private long getCountEstimate(Class<?> model) throws RocksDBException {
        ColumnFamilyHandle handle = classTreeMap.get(model);
        return db.getLongProperty(handle, "rocksdb.estimate-num-keys");
    }
    
    public void clear(Class<?> column) throws Exception {
        log.info("Deleting: {} entries", column.getSimpleName());
        flushHandle(classTreeMap.get(column));
    }

    public void clearMetadata(Class<?> column) throws Exception {
        log.info("Deleting: {} metadata", column.getSimpleName());
        flushHandle(metadataReference.get(column));
    }

    private void flushHandle(ColumnFamilyHandle handle) throws RocksDBException {
        List<byte[]> itemsToDelete = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(handle)) {

            for (iterator.seekToLast(); iterator.isValid(); iterator.prev()) {
                itemsToDelete.add(iterator.key());
            }
        }
        if (!itemsToDelete.isEmpty()) {
            log.info("Amount to delete: " + itemsToDelete.size());
        }
        int counter = 0;
        for (byte[] itemToDelete : itemsToDelete) {
            if (++counter % 10000 == 0) {
                log.info("Deleted: {}", counter);
            }
            db.delete(handle, itemToDelete);
        }
    }

    // options is closed in shutdown
    @SuppressWarnings("resource")
    private void initDB(String path, String logPath, String configFile, Map<String, Class<? extends Persistable>> columnFamilies) throws Exception {
        try {
            try {
                RocksDB.loadLibrary();
            } catch (Exception e) {
                throw e;
            }

            
            sstFileManager = new SstFileManager(Env.getDefault());

            List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
            // Pass columnFamilyDescriptors so that they are loaded from options file, we check modifications later
            options = createOptions(logPath, configFile, columnFamilyDescriptors);

            bloomFilter = new BloomFilter(BLOOM_FILTER_BITS_PER_KEY);
            
            columnFamilyOptions = new ColumnFamilyOptions();
            
            loadColumnFamilyDescriptors(columnFamilyDescriptors);
            
            db = RocksDB.open(options, path, columnFamilyDescriptors, columnFamilyHandles);
            db.enableFileDeletions(true);

            initClassTreeMap(columnFamilyDescriptors);

        } catch (Exception e) {
            IotaIOUtils.closeQuietly(db, options, bloomFilter, columnFamilyOptions, cache, compressedCache);
            throw e;
        }
    }
    
    private void loadColumnFamilyDescriptors(List<ColumnFamilyDescriptor> columnFamilyDescriptors) {
        boolean needsUpdate = checkUpdate(columnFamilyDescriptors, columnFamilies.keySet());
        if (!columnFamilyDescriptors.isEmpty() && needsUpdate) {
            // We updated the database
            log.info("IRI Database scheme has been updated.. Loading new ColumnFamilyDescriptors");
            
        }
        
        columnFamilyDescriptors.clear();
        if (columnFamilyDescriptors.isEmpty()) {
            //Add default column family. Main motivation is to not change legacy code
            columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
            for (String name : columnFamilies.keySet()) {
                columnFamilyDescriptors.add(new ColumnFamilyDescriptor(name.getBytes(), columnFamilyOptions));
            }
            // metadata descriptor is always last
            if (metadataColumnFamily != null) {
                columnFamilyDescriptors.add(
                        new ColumnFamilyDescriptor(metadataColumnFamily.getKey().getBytes(), columnFamilyOptions));
                metadataReference = new HashMap<>();
            }
        }
    }

    private boolean checkUpdate(List<ColumnFamilyDescriptor> columnFamilyDescriptors, Set<String> names) {
        int totalDescriptors = columnFamilies.size() + (metadataColumnFamily != null ? 2 : 1); // +1 for default
        if (totalDescriptors != columnFamilyDescriptors.size()) {
            return true;
        }
        
        // Does not prevent name changes, as this must be migrated. RocksDB will throw an error if attempted
        // Should be fixed in Issue #1473
        long count = names.stream().map(name -> name.getBytes()).filter(name -> {
            for (ColumnFamilyDescriptor descriptor : columnFamilyDescriptors) {
                if (Objects.deepEquals(name, descriptor.getName())) {
                    return false;
                }
            }
            return true;
        }).count();
        return count != 0;
    }

    private void initClassTreeMap(List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws Exception {
        Map<Class<?>, ColumnFamilyHandle> classMap = new LinkedHashMap<>();
        String mcfName = metadataColumnFamily == null ? "" : metadataColumnFamily.getKey();
        //skip default column
        int i = 1;
        for (; i < columnFamilyDescriptors.size(); i++) {

            String name = new String(columnFamilyDescriptors.get(i).getName());
            if (name.equals(mcfName)) {
                Map<Class<?>, ColumnFamilyHandle> metadataRef = new HashMap<>();
                metadataRef.put(metadataColumnFamily.getValue(), columnFamilyHandles.get(i));
                metadataReference = MapUtils.unmodifiableMap(metadataRef);
            }
            else {
                classMap.put(columnFamilies.get(name), columnFamilyHandles.get(i));
            }
        }
        for (; ++i < columnFamilyHandles.size(); ) {
            db.dropColumnFamily(columnFamilyHandles.get(i));
        }

        classTreeMap = MapUtils.unmodifiableMap(classMap);
    }

    private DBOptions createOptions(String logPath, String configFile, List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws IOException {
        DBOptions options = null;
        File pathToLogDir = Paths.get(logPath).toFile();
        if (!pathToLogDir.exists() || !pathToLogDir.isDirectory()) {
            boolean success = pathToLogDir.mkdir();
            if (!success) {
                log.warn("Unable to make directory: {}", pathToLogDir);
            }
        }

        if (configFile != null) {
            File config = Paths.get(configFile).toFile();
            if (config.exists() && config.isFile() && config.canRead()) {
                Properties configProperties = new Properties();
                try (InputStream stream = new FileInputStream(config)){
                    configProperties.load(stream);
                    options = DBOptions.getDBOptionsFromProps(configProperties);
                } catch (IllegalArgumentException e) {
                    log.warn("RocksDB configuration file is empty, falling back to default values");
                }
            }
        } 

        if (options == null){
            options = new DBOptions();
        }
        
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        
        //Defaults we always need to set
        options.setSstFileManager(sstFileManager);
        options.setStatistics(statistics = new Statistics());

        return options;
    }
    
    public long getPersistanceSize() throws RocksDBException {
        System.out.println(statistics.getTickerCount(TickerType.WAL_FILE_BYTES));
        System.out.println(statistics.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN));
        
        System.out.println(sstFileManager.getTotalSize());
        
        List<Range> ranges = new LinkedList<Range>();
        ranges.add(new Range(
                new Slice("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), 
                new Slice("999999999999999999999999999999999999999999999999999999999999999999999999999999999")));
        
        long[] size = new long[ranges.size()];
        size = db.getApproximateSizes(columnFamilyHandles.get(0), ranges, SizeApproximationFlag.INCLUDE_FILES, SizeApproximationFlag.INCLUDE_MEMTABLES);
        
        System.out.println(Arrays.toString(size));
        return size[0];
    }
}
