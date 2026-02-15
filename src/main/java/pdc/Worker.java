package pdc;

import java.net.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.Random;

public class Worker {
    private String masterHost;
    private int masterPort;
    private int workerId;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final ExecutorService taskExecutor;
    private volatile boolean running = true;
    private final Random rand = new Random();
    
    // REQUIRED BY TESTS: No-arg constructor
    public Worker() {
        // REQUIRED: Read config from environment variables
        String host = System.getenv("MASTER_HOST");
        String portStr = System.getenv("MASTER_PORT");
        int port = (portStr != null) ? Integer.parseInt(portStr) : 9999;
        
        this.masterHost = (host != null) ? host : "localhost";
        this.masterPort = port;
        this.workerId = -1;
        this.taskExecutor = Executors.newFixedThreadPool(2, r -> 
            new Thread(r, "Worker-unregistered-Task")
        );
    }
    
    public Worker(String masterHost, int masterPort, int workerId) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.workerId = workerId;
        this.taskExecutor = Executors.newFixedThreadPool(2, r -> 
            new Thread(r, "Worker-" + workerId + "-Task")
        );
    }
    
    // REQUIRED BY TESTS: Connect to cluster
    public void joinCluster(String host, int port) {
        // Allow environment variables to override parameters
        String envHost = System.getenv("MASTER_HOST");
        String envPort = System.getenv("MASTER_PORT");
        this.masterHost = (envHost != null) ? envHost : host;
        this.masterPort = (envPort != null) ? Integer.parseInt(envPort) : port;
        
        if (workerId == -1) {
            workerId = Math.abs(masterHost.hashCode() % 1000);
        }
    }
    
    // REQUIRED BY TESTS: Start execution loop
    public void execute() {
        try {
            start();
        } catch (IOException e) {
            System.err.println("[Worker " + workerId + "] Execute failed: " + e.getMessage());
        }
    }
    
    public void start() throws IOException {
        socket = new Socket(masterHost, masterPort);
        socket.setSoTimeout(8000); // Zimbabwe office reality
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        
        System.out.println("[Worker " + workerId + "] Connected to master at " + masterHost + ":" + masterPort);
        
        // Start heartbeat thread
        Thread heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(2500 + rand.nextInt(1000));
                    if (running) sendHeartbeat();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }, "Worker-" + workerId + "-Heartbeat");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
        
        // Main message loop
        while (running) {
            try {
                int length = in.readInt();
                byte[] buffer = new byte[length];
                int bytesRead = 0;
                while (bytesRead < length) {
                    int n = in.read(buffer, bytesRead, length - bytesRead);
                    if (n <= 0) throw new EOFException("Connection closed");
                    bytesRead += n;
                }
                
                Message msg = Message.unpack(buffer);
                if (msg != null && msg.getType() == Message.Type.TASK) { // CORRECTED: Using getType() getter
                    taskExecutor.submit(() -> handleTask(msg));
                }
            } catch (SocketTimeoutException e) {
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
    
    private void sendHeartbeat() {
        try {
            Message hb = new Message(Message.Type.HEARTBEAT, workerId, new byte[0]);
            byte[] packed = hb.pack();
            out.writeInt(packed.length);
            out.write(packed);
            out.flush();
        } catch (IOException e) {
            running = false;
        }
    }
    
    private void handleTask(Message msg) {
        try {
            // Simulate slow laptop 15% of the time
            if (rand.nextDouble() < 0.15) {
                Thread.sleep(2000 + rand.nextInt(3000));
            }
            
            byte[] payload = msg.getPayload(); // CORRECTED: Using getPayload() getter
            ByteArrayInputStream bais = new ByteArrayInputStream(payload);
            DataInputStream dataIn = new DataInputStream(bais);
            
            int rowsA = dataIn.readInt();
            int colsA = dataIn.readInt();
            int colsB = dataIn.readInt();
            
            double[][] A = new double[rowsA][colsA];
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsA; j++) {
                    A[i][j] = dataIn.readDouble();
                }
            }
            
            double[][] B = new double[colsA][colsB];
            for (int i = 0; i < colsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    B[i][j] = dataIn.readDouble();
                }
            }
            
            double[][] C = new double[rowsA][colsB];
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    for (int k = 0; k < colsA; k++) {
                        C[i][j] += A[i][k] * B[k][j];
                    }
                }
            }
            
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(baos);
            dataOut.writeInt(rowsA);
            dataOut.writeInt(colsB);
            for (int i = 0; i < rowsA; i++) {
                for (int j = 0; j < colsB; j++) {
                    dataOut.writeDouble(C[i][j]);
                }
            }
            
            Message result = new Message(Message.Type.RESULT, msg.getTaskId(), baos.toByteArray()); // CORRECTED: Using getTaskId() getter
            byte[] packed = result.pack();
            synchronized (out) {
                out.writeInt(packed.length);
                out.write(packed);
                out.flush();
            }
            
            System.out.println("[Worker " + workerId + "] Completed task " + msg.getTaskId()); // CORRECTED: Using getTaskId() getter
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Task " + msg.getTaskId() + " failed: " + e.getMessage());
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