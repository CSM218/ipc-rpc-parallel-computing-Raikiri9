package pdc;

public class Message {
    // REQUIRED BY AUTOGRADE: NUST protocol schema fields
    private static final int MAGIC = 0x43534D32;  // "CSM2" in ASCII hex
    private static final int STUDENT_ID = 22183334;  // ← REPLACE WITH YOUR ACTUAL STUDENT NUMBER (digits from N00011100X)

    public enum Type {
        TASK(0x01),
        HEARTBEAT(0x02),
        RESULT(0x03),
        ACK(0x04);
        
        private final byte code;
        Type(int code) { this.code = (byte) code; }
        public byte getCode() { return code; }
        public static Type fromCode(byte code) {
            for (Type t : values()) {
                if (t.code == code) return t;
            }
            throw new IllegalArgumentException("Invalid code: " + code);
        }
    }

    private final Type type;
    private final int taskId;
    private final byte[] payload;
    private final long timestamp;

    public Message(Type type, int taskId, byte[] payload) {
        this.type = type;
        this.taskId = taskId;
        this.payload = payload != null ? payload.clone() : new byte[0];
        this.timestamp = System.nanoTime();
    }

    public byte[] pack() {
        final byte VERSION = 0x02;
        // Schema: [4:magic][4:studentId][1:version][1:type][4:taskId][4:payloadLen][payload][1:checksum]
        int totalSize = 4 + 4 + 1 + 1 + 4 + 4 + payload.length + 1;
        byte[] buffer = new byte[totalSize];
        int offset = 0;
        
        // Write magic number (4 bytes)
        buffer[offset++] = (byte) ((MAGIC >> 24) & 0xFF);
        buffer[offset++] = (byte) ((MAGIC >> 16) & 0xFF);
        buffer[offset++] = (byte) ((MAGIC >> 8) & 0xFF);
        buffer[offset++] = (byte) (MAGIC & 0xFF);
        
        // Write student ID (4 bytes)
        buffer[offset++] = (byte) ((STUDENT_ID >> 24) & 0xFF);
        buffer[offset++] = (byte) ((STUDENT_ID >> 16) & 0xFF);
        buffer[offset++] = (byte) ((STUDENT_ID >> 8) & 0xFF);
        buffer[offset++] = (byte) (STUDENT_ID & 0xFF);
        
        // Write version
        buffer[offset++] = VERSION;
        
        // Write message type
        buffer[offset++] = type.getCode();
        
        // Write taskId (4 bytes)
        buffer[offset++] = (byte) ((taskId >> 24) & 0xFF);
        buffer[offset++] = (byte) ((taskId >> 16) & 0xFF);
        buffer[offset++] = (byte) ((taskId >> 8) & 0xFF);
        buffer[offset++] = (byte) (taskId & 0xFF);
        
        // Write payload length (4 bytes)
        int len = payload.length;
        buffer[offset++] = (byte) ((len >> 24) & 0xFF);
        buffer[offset++] = (byte) ((len >> 16) & 0xFF);
        buffer[offset++] = (byte) ((len >> 8) & 0xFF);
        buffer[offset++] = (byte) (len & 0xFF);
        
        // Copy payload
        System.arraycopy(payload, 0, buffer, offset, payload.length);
        offset += payload.length;
        
        // Write checksum (XOR of magic bytes)
        byte checksum = (byte) (buffer[0] ^ buffer[1] ^ buffer[2] ^ buffer[3]);
        buffer[offset] = checksum;
        
        return buffer;
    }

    public static Message unpack(byte[] data) {
        // Minimum size: magic(4) + studentId(4) + version(1) + type(1) + taskId(4) + len(4) + checksum(1) = 19
        if (data == null || data.length < 19) {
            return null; // Fragmented message - caller must buffer
        }
        
        // Validate magic number (bytes 0-3)
        int magic = ((data[0] & 0xFF) << 24) |
                    ((data[1] & 0xFF) << 16) |
                    ((data[2] & 0xFF) << 8) |
                    (data[3] & 0xFF);
        if (magic != MAGIC) {
            throw new IllegalArgumentException("Invalid magic number: 0x" + Integer.toHexString(magic));
        }
        
        // Skip student ID field (bytes 4-7) - required by NUST protocol schema but unused in logic
        
        // Validate version (byte 8)
        if (data[8] != 0x02) {
            throw new IllegalArgumentException("Unknown protocol version: " + data[8]);
        }
        
        // Read type (byte 9)
        Type type = Type.fromCode(data[9]);
        
        // Read taskId (bytes 10-13)
        int taskId = ((data[10] & 0xFF) << 24) |
                     ((data[11] & 0xFF) << 16) |
                     ((data[12] & 0xFF) << 8) |
                     (data[13] & 0xFF);
        
        // Read payload length (bytes 14-17)
        int payloadLength = ((data[14] & 0xFF) << 24) |
                            ((data[15] & 0xFF) << 16) |
                            ((data[16] & 0xFF) << 8) |
                            (data[17] & 0xFF);
        
        // Check for complete message (including checksum)
        int expectedLength = 18 + payloadLength + 1; // 18 header bytes + payload + 1 checksum
        if (data.length < expectedLength) {
            return null; // Still fragmented
        }
        
        // Validate checksum (byte 18+payloadLength)
        byte expectedChecksum = (byte) (data[0] ^ data[1] ^ data[2] ^ data[3]);
        byte actualChecksum = data[18 + payloadLength];
        if (expectedChecksum != actualChecksum) {
            throw new IllegalArgumentException("Checksum mismatch");
        }
        
        // Extract payload (bytes 18 to 18+payloadLength-1)
        byte[] payload = new byte[payloadLength];
        System.arraycopy(data, 18, payload, 0, payloadLength);
        
        return new Message(type, taskId, payload);
    }

    public Type getType() { return type; }
    public int getTaskId() { return taskId; }
    public byte[] getPayload() { return payload.clone(); }
    public long getTimestamp() { return timestamp; }
}