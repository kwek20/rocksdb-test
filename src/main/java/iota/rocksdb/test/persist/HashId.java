package iota.rocksdb.test.persist;

/**
 * Represents an ID reference for a transaction, address or bundle. Stores the bytes
 * of the object reference.
 */
public interface HashId {

    /**@return the bytes of the current Hash ID*/
    byte[] bytes();
}
