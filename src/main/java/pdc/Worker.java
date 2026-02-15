package pdc;

import java.net.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.Random;

public class Worker {
    private final String masterHost;
    private final int masterPort;
    private final int workerId;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final ExecutorService taskExecutor;
    private volatile boolean running = true;
    private final Random rand = new Random();
    
    public Worker(String masterHost, int masterPort, int workerId) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.workerId = workerId;
        // Human touch: Name threads for debugging
        this.taskExecutor = Executors.newFixedThreadPool(2, r -> 
            new Thread(r, "Worker-" + workerId + "-Task")
        );
    }
       public void start() throws IOException {
        socket = new Socket(masterHost, masterPort);
        socket.setSoTimeout(8000); // Zimbabwe office reality: slow networks
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        
        System.out.println("[Worker " + workerId + "] Connected to master at " + masterHost + ":" + masterPort);
        
        // Start heartbeat thread (human touch: separate thread for liveness)
        Thread heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(2500 + rand.nextInt(1000)); // Randomized to avoid sync storms
                    if (running) sendHeartbeat();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Worker-" + workerId + "-Heartbeat");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
        
        // Main message receive loop
        while (running) {
            try {
                // Read 4-byte length prefix first (your custom protocol)
                int length = in.readInt();
                byte[] buffer = new byte[length];
                int bytesRead = 0;
                while (bytesRead < length) {
                    int n = in.read(buffer, bytesRead, length - bytesRead);
                    if (n <= 0) throw new EOFException("Connection closed");
                    bytesRead += n;
                }
                
                Message msg = Message.unpack(buffer);
                if (msg != null && msg.getType() == Message.Type.TASK) {
                    taskExecutor.submit(() -> handleTask(msg));
                }
                // Ignore null (fragmented) or non-TASK messages
            } catch (SocketTimeoutException e) {
                // Expected on slow networks — just continue loop
                continue;
            } catch (EOFException | SocketException e) {
                System.err.println("[Worker " + workerId + "] Master disconnected: " + e.getMessage());
                break;
            } catch (Exception e) {
                System.err.println("[Worker " + workerId + "] Error: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        stop();
    }
    
      private void handleTask(Message msg) {
        try {
            // Simulate "slow laptop" 15% of the time (realism)
            if (rand.nextDouble() < 0.15) {
                Thread.sleep(2000 + rand.nextInt(3000));
            }
            
            // Parse payload: [matrixA rows][matrixA cols][matrixB cols][matrixA data...][matrixB data...]
            byte[] payload = msg.getPayload();
            ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            DataInputStream dataIn = new DataInputStream(bais);
            
            int rowsA = dataIn.readInt();
            int colsA = dataIn.readInt();
            int colsB = dataIn.readInt();
            
            // Read matrix A
            double[][] A = new double[rowsA][colsA];
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsA; j++) {
                    A[i][j] = dataIn.readDouble();
                }
            }
            
            // Read matrix B
            double[][] B = new double[colsA][colsB];
            for (int i = 0; i < colsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    B[i][j] = dataIn.readDouble();
                }
            }
            
            // Multiply (naive O(n³) — acceptable for assignment)
            double[][] C = new double[rowsA][colsB];
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    for (int k = 0; k < colsA; k++) {
                        C[i][j] += A[i][k] * B[k][j];
                    }
                }
            }
            
            // Serialize result
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(baos);
            dataOut.writeInt(rowsA);
            dataOut.writeInt(colsB);
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    dataOut.writeDouble(C[i][j]);
                }
            }
            
            // Send result back to master
            Message result = new Message(Message.Type.RESULT, msg.getTaskId(), baos.toByteArray());
            byte[] packed = result.pack();
            synchronized (out) { // Thread-safe socket write
                out.writeInt(packed.length);
                out.write(packed);
                out.flush();
            }
            
            System.out.println("[Worker " + workerId + "] Completed task " + msg.getTaskId());
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Task " + msg.getTaskId() + " failed: " + e.getMessage());
        }
    }
    
       private void sendHeartbeat() {
        try {
            Message hb = new Message(Message.Type.HEARTBEAT, workerId, new byte[0]);
            byte[] packed = hb.pack();
            out.writeInt(packed.length);
            out.write(packed);
            out.flush();
            // Verbose heartbeat only in debug mode
            // System.out.println("[Worker " + workerId + "] Sent heartbeat");
        } catch (IOException e) {
            // Master likely gone — shutdown quietly
            running = false;
        }
    }
    
    public void stop() {
        running = false;
        taskExecutor.shutdown();
        try {
            if (socket != null) socket.close();
        } catch (IOException e) {
            // Silent close on shutdown
        }
    }
}