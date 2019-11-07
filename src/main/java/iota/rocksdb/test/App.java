package iota.rocksdb.test;

import java.util.LinkedHashMap;
import java.util.Map;

import iota.rocksdb.test.persist.HashFactory;
import iota.rocksdb.test.persist.Indexable;
import iota.rocksdb.test.persist.Persistable;
import iota.rocksdb.test.util.TransactionTestUtils;

/**
 * Hello world!
 *
 */
public class App {
    public static final Map<String, Class<? extends Persistable>> COLUMN_FAMILIES =
        new LinkedHashMap<String, Class<? extends Persistable>>() {{
            put("transaction", Transaction.class);
        }};
        
    RocksDBPersistenceProvider provider;
            
    public App() throws Exception {
        provider = new RocksDBPersistenceProvider(
                "/home/brord/Desktop/Work/IOTA/rocksdbtest/db", 
                "/home/brord/Desktop/Work/IOTA/rocksdbtest/log", 
                "/home/brord/Desktop/Work/IOTA/rocksdbtest/config.properties", 
                1, COLUMN_FAMILIES, null);
        provider.init();
        
        Indexable index = getIndexable();
        Transaction tx = getTransaction();
        provider.save(tx, index);
        System.out.println(provider.getPersistanceSize());
        provider.shutdown();
    }
    
    private Transaction getTransaction() {
        return TransactionTestUtils.getTransaction();
    }
    
    private Indexable getIndexable() {
        Indexable index = HashFactory.TRANSACTION.create("GQQOWD9RWTC9VAH9KIJLDSTEWH9YGOBMVYDQJYSOVVETXNDFRAZWMKPGAHPHZYLJZGNIUNDQ");
        return index;
    }
    
    public static void main( String[] args ) {
        try {
            new App();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
