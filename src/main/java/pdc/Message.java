package pdc;

public class Message{
    // Lets do it
    public enum Type{
    TASK(0x01),
    HEARTBEAT(0x02),
    RESULT(0x03),
    ACK(0x04);

    private final byte code;
    Type(int code){this.code = (byte) code;}
        public byte getCode(){return code;}
        public static Type fromCode(byte code){
            for(Type t : Type.values()){
                if(t.code == code) return t;
            }
            throw new IllegalArgumentException("Invalid code: " + code);
        }
}


        private final Type type;
        private final int taskId;
        private final byte[] payload;
        private final long timestamp;

        public Message(Type type, int taskId, byte[] payload){
            this.type = type;
            this.taskId = taskId;
            this.payload = payload != null ? payload : new byte[0];
            this.timestamp = System.nanoTime();
        }
        public byte [] pack() {
            //Add one byte protocol version (future proofing
            final byte VERSION = 0x02;

            // Calculate total size: version (1) + type (1) + taskId (4) + length (4) + payload + checksum
            int totalSize = 1 + 1 + 4 + 4 + payload.length + 1;
            byte[] buffer = new byte[totalSize];
            int offset = 0;

            // Write version protocol
            buffer[offset++] = VERSION;

            // Write message type
            buffer[offset++] = type.getCode();

            // Write taskId as 4 bytes
            buffer[offset++] = (byte) ((taskId >> 24) & 0xFF);
            buffer[offset++] = (byte) ((taskId >> 16) & 0xFF);
            buffer[offset++] = (byte) ((taskId >> 8) & 0xFF);
            buffer[offset++] = (byte) (taskId & 0xFF);

            // Write payload length as 4 bytes
            int len = payload.length;
            buffer[offset++] = (byte) ((len >> 24) & 0xFF);
            buffer[offset++] = (byte) ((len >> 16) & 0xFF);
            buffer[offset++] = (byte) ((len >> 8) & 0xFF);
            buffer[offset++] = (byte) (len & 0xFF);

            // Copy payload 
            System.arraycopy(payload, 0, buffer, offset, payload.length);
            offset += payload.length;

            // Calculate checksum (simple XOR of all bytes)
            byte checksum = (byte) (VERSION ^ type.getCode() ^ buffer[2] ^ buffer[3] );
            buffer[offset] = checksum;
            return buffer;
        }
public static Message unpack(byte[] data) {
    // if data is too short, return null
    if (data ==null || data.length < 11) {
        return null;
}
// Check protocol version
if (data[0] != 0x02) {
    throw new IllegalArgumentException("Unsupported protocol version: " + data[0]);
}
Type type = Type.fromCode(data[1]);
int taskId = ((data[2] & 0xFF) << 24) | 
             ((data[3] & 0xFF) << 16) |
             ((data[4] & 0xFF) << 8) | 
             (data[5] & 0xFF);
            
int payloadLength = ((data[6] & 0xFF) << 24) | 
                    ((data[7] & 0xFF) << 16) |
                    ((data[8] & 0xFF) << 8) | 
                    (data[9] & 0xFF);

                    //Check if we have a complete message
                    int expectedLength = 10 + payloadLength + 1; // header + payload + checksum
                    if (data.length < expectedLength) {
                        return null; // Incomplete message
                    }

                    //Verify checksum
                    byte expectedChecksum = (byte) (data[0] ^ data[1] ^ data[2] ^ data[3] );
                byte actualChecksum = data[10 + payloadLength];
                if (expectedChecksum != actualChecksum) {
                    throw new IllegalArgumentException("Checksum mismatch");
                }
                
                // Extract payload
                byte[] payload = new byte[payloadLength];
                System.arraycopy(data, 10, payload, 0, payloadLength);

                return new Message(type, taskId, payload);
}
public Type getType(){return type;}
public int getTaskId(){return taskId;}
public byte[] getPayload(){return payload.clone();}
public long getTimestamp(){return timestamp;}

    }
