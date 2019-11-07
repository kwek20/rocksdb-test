package iota.rocksdb.test.util;


import java.util.Random;

import org.apache.commons.lang3.StringUtils;

import iota.rocksdb.test.Transaction;
import iota.rocksdb.test.persist.Hash;
import iota.rocksdb.test.persist.HashFactory;

public class TransactionTestUtils {

    /** Length of a transaction object in trytes */
    public static final int SIZE = 1604;
    private static final int TAG_SIZE_IN_BYTES = 17; // = ceil(81 TRITS / 5 TRITS_PER_BYTE)

    /** Total supply of IOTA available in the network. Used for ensuring a balanced ledger state and bundle balances */
    public static final long SUPPLY = 2779530283277761L; // = (3^33 - 1) / 2

    /** The predefined offset position and size (in trits) for the varying components of a transaction object */
    public static final int SIGNATURE_MESSAGE_FRAGMENT_TRINARY_OFFSET = 0,
            SIGNATURE_MESSAGE_FRAGMENT_TRINARY_SIZE = 6561;
    public static final int ADDRESS_TRINARY_OFFSET = SIGNATURE_MESSAGE_FRAGMENT_TRINARY_OFFSET
            + SIGNATURE_MESSAGE_FRAGMENT_TRINARY_SIZE, ADDRESS_TRINARY_SIZE = 243;
    public static final int VALUE_TRINARY_OFFSET = ADDRESS_TRINARY_OFFSET + ADDRESS_TRINARY_SIZE,
            VALUE_TRINARY_SIZE = 81, VALUE_USABLE_TRINARY_SIZE = 33;
    public static final int OBSOLETE_TAG_TRINARY_OFFSET = VALUE_TRINARY_OFFSET + VALUE_TRINARY_SIZE,
            OBSOLETE_TAG_TRINARY_SIZE = 81;
    public static final int TIMESTAMP_TRINARY_OFFSET = OBSOLETE_TAG_TRINARY_OFFSET + OBSOLETE_TAG_TRINARY_SIZE,
            TIMESTAMP_TRINARY_SIZE = 27;
    public static final int CURRENT_INDEX_TRINARY_OFFSET = TIMESTAMP_TRINARY_OFFSET + TIMESTAMP_TRINARY_SIZE,
            CURRENT_INDEX_TRINARY_SIZE = 27;
    public static final int LAST_INDEX_TRINARY_OFFSET = CURRENT_INDEX_TRINARY_OFFSET + CURRENT_INDEX_TRINARY_SIZE,
            LAST_INDEX_TRINARY_SIZE = 27;
    public static final int BUNDLE_TRINARY_OFFSET = LAST_INDEX_TRINARY_OFFSET + LAST_INDEX_TRINARY_SIZE,
            BUNDLE_TRINARY_SIZE = 243;
    public static final int TRUNK_TRANSACTION_TRINARY_OFFSET = BUNDLE_TRINARY_OFFSET + BUNDLE_TRINARY_SIZE,
            TRUNK_TRANSACTION_TRINARY_SIZE = 243;
    public static final int BRANCH_TRANSACTION_TRINARY_OFFSET = TRUNK_TRANSACTION_TRINARY_OFFSET
            + TRUNK_TRANSACTION_TRINARY_SIZE, BRANCH_TRANSACTION_TRINARY_SIZE = 243;

    public static final int TAG_TRINARY_OFFSET = BRANCH_TRANSACTION_TRINARY_OFFSET + BRANCH_TRANSACTION_TRINARY_SIZE,
            TAG_TRINARY_SIZE = 81;
    public static final int ATTACHMENT_TIMESTAMP_TRINARY_OFFSET = TAG_TRINARY_OFFSET + TAG_TRINARY_SIZE,
            ATTACHMENT_TIMESTAMP_TRINARY_SIZE = 27;
    public static final int ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_OFFSET = ATTACHMENT_TIMESTAMP_TRINARY_OFFSET
            + ATTACHMENT_TIMESTAMP_TRINARY_SIZE, ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_SIZE = 27;
    public static final int ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_OFFSET = ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_OFFSET
            + ATTACHMENT_TIMESTAMP_LOWER_BOUND_TRINARY_SIZE, ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_SIZE = 27;
    private static final int NONCE_TRINARY_OFFSET = ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_OFFSET
            + ATTACHMENT_TIMESTAMP_UPPER_BOUND_TRINARY_SIZE, NONCE_TRINARY_SIZE = 81;

    public static final int TRINARY_SIZE = NONCE_TRINARY_OFFSET + NONCE_TRINARY_SIZE;
    public static final int TRYTES_SIZE = TRINARY_SIZE / 3;

    public static final int ESSENCE_TRINARY_OFFSET = ADDRESS_TRINARY_OFFSET,
            ESSENCE_TRINARY_SIZE = ADDRESS_TRINARY_SIZE + VALUE_TRINARY_SIZE + OBSOLETE_TAG_TRINARY_SIZE
                    + TIMESTAMP_TRINARY_SIZE + CURRENT_INDEX_TRINARY_SIZE + LAST_INDEX_TRINARY_SIZE;
    
    private static Random seed = new Random(1);

    public final static int NON_EMPTY_SIG_PART_BYTES_COUNT = 1000;
    public final static int TRUNCATION_BYTES_COUNT = TransactionTruncator.SIG_DATA_MAX_BYTES_LENGTH - NON_EMPTY_SIG_PART_BYTES_COUNT;
    public final static int SIG_FILL = 3, REST_FILL = 4, EMPTY_FILL = 0;

    /**
     * Constructs transaction bytes where the signature message fragment is filled with 1000
     * {@link TransactionTestUtils#SIG_FILL} bytes, 312 {@link TransactionTestUtils#EMPTY_FILL} bytes and
     * the non signature message fragment part with 292 {@link TransactionTestUtils#REST_FILL} bytes.
     * @return byte data of the transaction
     */
    public static byte[] constructTransactionBytes() {
        byte[] originTxData = new byte[Transaction.SIZE];
        for (int i = 0; i < NON_EMPTY_SIG_PART_BYTES_COUNT; i++) {
            originTxData[i] = SIG_FILL;
        }
        for (int i = NON_EMPTY_SIG_PART_BYTES_COUNT; i < TransactionTruncator.SIG_DATA_MAX_BYTES_LENGTH; i++) {
            originTxData[i] = EMPTY_FILL;
        }
        for (int i = TransactionTruncator.SIG_DATA_MAX_BYTES_LENGTH; i < Transaction.SIZE; i++) {
            originTxData[i] = REST_FILL;
        }
        return originTxData;
    }
    
    /**
     * Generates transaction trits with the provided trytes, trunk and hash.
     * If the trytes are not enough to make a full transaction, 9s are appended.
     * 
     * @param trytes The transaction trytes to use
     * @param trunk The trunk transaction hash
     * @param branch The branch transaction hash
     * @return The transaction trits
     */
    public static byte[] createTransactionWithTrunkAndBranchTrits(String trytes, Hash trunk, Hash branch) {
        String expandedTrytes = expandTrytes(trytes);
        byte[] trits =  Converter.allocatingTritsFromTrytes(expandedTrytes);
        return getTransactionTritsWithTrunkAndBranchTrits(trits, trunk, branch);
    }
    
    /**
     * Generates transaction trits with the provided trytes, trunk and hash.
     * No validation is done on the resulting trits, so fields are not valid except trunk and branch. 
     * 
     * @param trunk The trunk transaction hash
     * @param branch The branch transaction hash
     * @return The transaction trits
     */
    public static byte[] getTransactionTritsWithTrunkAndBranch(Hash trunk, Hash branch) {
        byte[] trits = getTransactionTrits();
        return getTransactionTritsWithTrunkAndBranchTrits(trits, trunk, branch);
    }
    
    /**
     * Generates transaction trits with the provided trits, trunk and hash.
     * 
     * @param trunk The trunk transaction hash
     * @param branch The branch transaction hash
     * @return trits The transaction trits
     */
    public static byte[] getTransactionTritsWithTrunkAndBranchTrits(byte[] trits, Hash trunk, Hash branch) {
        System.arraycopy(trunk.trits(), 0, trits, TRUNK_TRANSACTION_TRINARY_OFFSET,
                TRUNK_TRANSACTION_TRINARY_SIZE);
        System.arraycopy(branch.trits(), 0, trits, BRANCH_TRANSACTION_TRINARY_OFFSET,
                BRANCH_TRANSACTION_TRINARY_SIZE);
        return trits;
    }

    /**
     * Increases a char with the next char in the alphabet, until the char is Z.
     * When the char is Z, adds a new char starting at A.
     * 9 turns To A.
     * 
     * @param trytes The Trytes to change.
     * @return The changed trytes
     */
    public static String nextWord(String trytes) {
        if ("".equals(trytes)) {
            return "A";
        }
        char[] chars = trytes.toUpperCase().toCharArray();
        for (int i = chars.length -1; i>=0; --i) {
            if (chars[i] == '9') {
                chars[i] = 'A';
            }
            else if (chars[i] != 'Z') {
                ++chars[i];
                return new String(chars);
            }
        }
        return trytes + 'A';
    }
    
    /**
     * Generates a transaction.
     * 
     * @return The transaction
     */
    public static Transaction getTransaction() {
        byte[] trits = getTransactionTrits();
        return buildTransaction(trits);
    }
    
    /**
     * Generates trits for a transaction.
     * 
     * @return The transaction trits
     */
    public static byte[] getTransactionTrits() {
        return getTrits(TRINARY_SIZE);
    }
    
    /**
     * Generates a transaction with only 9s.
     * 
     * @return The transaction
     */
    public static Transaction get9Transaction() {
        byte[] trits = new byte[TRINARY_SIZE];
        for (int i = 0; i < trits.length; i++) {
            trits[i] = 0;
        }

        return buildTransaction(trits);
    }
    
    
    /**
     * Generates a transaction with trunk and hash.
     * 
     * @param trunk The trunk transaction hash
     * @param branch The branch transaction hash
     * @return The transaction
     */
    public static Transaction createTransactionWithTrunkAndBranch(Hash trunk, Hash branch) {
        byte[] trits = getTrits(TRINARY_SIZE);
        getTransactionTritsWithTrunkAndBranchTrits(trits, trunk, branch);
        return buildTransaction(trits);
    }

    /**
     * Generates trits for a hash.
     * 
     * @return The transaction hash
     */
    public static Hash getTransactionHash() {
        byte[] out = getTrits(Hash.SIZE_IN_TRITS);
        return HashFactory.TRANSACTION.create(out);
    }
    
    /**
     * Builds a transaction by transforming trits to bytes.
     * Make sure the trits are in the correct order (TVM.trits())
     * 
     * @param trits The trits to build the transaction
     * @return The created transaction
     */
    public static Transaction buildTransaction(byte[] trits) {  
        
        byte[] bytes = new byte[Transaction.SIZE];
        Converter.bytes(trits, bytes);
        
        Transaction t = new Transaction();
        t.read(bytes);
        t.readMetadata(bytes);
        
        return t;
    }

    /**
     * Appends 9s to the supplied trytes until the trytes are of size {@link TransactionViewModel.TRYTES_SIZE}.
     * 
     * @param trytes the trytes to append to.
     * @return The expanded trytes string
     */
    private static String expandTrytes(String trytes) {
        return trytes + StringUtils.repeat('9', TRYTES_SIZE - trytes.length());
    }
    
    /**
     * Generates 'random' trits of specified size.
     * Not truly random as we always use the same seed.
     * 
     * @param size the amount of trits to generate
     * @return The trits
     */
    private static byte[] getTrits(int size) {
        byte[] out = new byte[size];

        for(int i = 0; i < out.length; i++) {
            out[i] = (byte) (seed.nextInt(3) - 1);
        }
        return out;
    }
}
