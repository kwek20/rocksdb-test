package iota.rocksdb.test.persist.save;

/**
 * Creates a null safe byte array for storing trit values
 */
public class TritSafe extends SafeObject {
	public TritSafe(byte[] bytes) {
		super(bytes, "TritSafe is attempted to be initialized with a null byte array");
	}
}
