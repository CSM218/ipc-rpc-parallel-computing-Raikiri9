package pdc;

import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Master {
    private final int port;
    private final ServerSocket serverSocket;
    private final Map<Integer, WorkerState> workers = new ConcurrentHashMap<>();
    private final Map<Integer, TaskState> tasks = new ConcurrentHashMap<>();
    private final AtomicInteger nextTaskId = new AtomicInteger(1);
    private final ScheduledExecutorService heartbeatMonitor;
    private volatile boolean running = true;
    
    // Human touch: Realistic heartbeat timeout (Zimbabwe office networks)
    private static final long HEARTBEAT_TIMEOUT_MS = 10000; // 10 seconds
    
    public Master(int port) throws IOException {
        this.port = port;
        this.serverSocket = new ServerSocket(port);
        this.serverSocket.setSoTimeout(1000); // Allow periodic shutdown checks
        
        // Human touch: Named thread for heartbeat monitoring
        this.heartbeatMonitor = Executors.newSingleThreadScheduledExecutor(r ->
            new Thread(r, "Master-HeartbeatMonitor")
        );
        
        // Start heartbeat monitoring thread
        heartbeatMonitor.scheduleAtFixedRate(this::checkWorkerHealth, 
            HEARTBEAT_TIMEOUT_MS, HEARTBEAT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
    }
    
    // Worker tracking with liveness timestamps
    private static class WorkerState {
        final int id;
        final Socket socket;
        final DataOutputStream out;
        volatile long lastHeartbeat;
        
        WorkerState(int id, Socket socket, DataOutputStream out) {
            this.id = id;
            this.socket = socket;
            this.out = out;
            this.lastHeartbeat = System.nanoTime();
        }
        
        boolean isAlive() {
            long elapsedMs = (System.nanoTime() - lastHeartbeat) / 1_000_000;
            return elapsedMs < HEARTBEAT_TIMEOUT_MS;
        }
    }
    
    // Task tracking with assignment history
    private static class TaskState {
        final int id;
        final byte[] payload;
        volatile int assignedWorker = -1;
        volatile long assignedAt = 0;
        volatile boolean completed = false;
        
        TaskState(int id, byte[] payload) {
            this.id = id;
            this.payload = payload;
        }
    }
    
    public void start() throws IOException {
        System.out.println("[Master] Listening on port " + port);
        while (running) {
            try {
                Socket workerSocket = serverSocket.accept();
                new Thread(() -> handleWorker(workerSocket), 
                    "Master-WorkerHandler").start();
            } catch (SocketTimeoutException e) {
                // Expected — allows shutdown check
                continue;
            }
        }
    }
    
        private void handleWorker(Socket socket) {
        DataInputStream in = null;
        int workerId = -1;
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        
        try {
            in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            
            System.out.println("[Master] New worker connected from " + socket.getInetAddress());
            
            while (running && !socket.isClosed()) {
                try {
                    // Read 4-byte length prefix (your custom protocol)
                    int length = in.readInt();
                    byte[] msgBuffer = new byte[length];
                    int bytesRead = 0;
                    
                    // Handle TCP fragmentation at socket level
                    while (bytesRead < length) {
                        int n = in.read(msgBuffer, bytesRead, length - bytesRead);
                        if (n <= 0) throw new EOFException("Connection closed unexpectedly");
                        bytesRead += n;
                    }
                    
                    Message msg = Message.unpack(msgBuffer);
                    if (msg == null) {
                        // Fragmentation already handled by length prefix — this shouldn't happen
                        continue;
                    }
                    
                    // First message determines worker ID (registration)
                    if (workerId == -1) {
                        workerId = msg.getTaskId(); // Reuse taskId field for worker ID during registration
                        WorkerState ws = new WorkerState(workerId, socket, out);
                        workers.put(workerId, ws);
                        System.out.println("[Master] Registered worker " + workerId);
                        continue;
                    }
                    
                    // Handle heartbeat
                    if (msg.getType() == Message.Type.HEARTBEAT) {
                        WorkerState ws = workers.get(workerId);
                        if (ws != null) {
                            ws.lastHeartbeat = System.nanoTime();
                            // Human touch: Log slow workers for debugging (commented for production)
                            // long elapsedMs = (System.nanoTime() - ws.lastHeartbeat) / 1_000_000;
                            // if (elapsedMs > 5000) System.out.println("[Master] Worker " + workerId + " is slow: " + elapsedMs + "ms");
                        }
                        continue;
                    }
                    
                    // Handle task result
                    if (msg.getType() == Message.Type.RESULT) {
                        int taskId = msg.getTaskId();
                        TaskState ts = tasks.get(taskId);
                        if (ts != null && !ts.completed) {
                            ts.completed = true;
                            System.out.println("[Master] Task " + taskId + " completed by worker " + workerId);
                            
                            // Human touch: Acknowledge receipt (prevents worker retransmission)
                            Message ack = new Message(Message.Type.ACK, taskId, new byte[0]);
                            byte[] ackPacked = ack.pack();
                            synchronized (out) {
                                out.writeInt(ackPacked.length);
                                out.write(ackPacked);
                                out.flush();
                            }
                        }
                        continue;
                    }
                    
                } catch (SocketTimeoutException e) {
                    // Expected on slow networks — continue loop
                    continue;
                } catch (EOFException | SocketException e) {
                    System.err.println("[Master] Worker " + workerId + " disconnected: " + e.getMessage());
                    break;
                } catch (IOException e) {
                    if (!socket.isClosed()) {
                        System.err.println("[Master] I/O error with worker " + workerId + ": " + e.getMessage());
                    }
                    break;
                }
            }
            
        } catch (IOException e) {
            System.err.println("[Master] Error handling worker " + workerId + ": " + e.getMessage());
        } finally {
            // Cleanup on disconnect
            if (workerId != -1) {
                workers.remove(workerId);
                System.out.println("[Master] Removed worker " + workerId + " from cluster");
                
                // Reassign incomplete tasks from this worker
                for (TaskState ts : tasks.values()) {
                    if (!ts.completed && ts.assignedWorker == workerId) {
                        System.out.println("[Master] Reassigning orphaned task " + ts.id);
                        reassignTask(ts.id);
                    }
                }
            }
            try {
                if (socket != null && !socket.isClosed()) socket.close();
            } catch (IOException e) { /* silent */ }
        }
    }
    
       private void checkWorkerHealth() {
        if (!running) return;
        
        long now = System.nanoTime();
        List<Integer> deadWorkers = new ArrayList<>();
        
        // Find dead/slow workers
        for (Map.Entry<Integer, WorkerState> entry : workers.entrySet()) {
            WorkerState ws = entry.getValue();
            long elapsedMs = (now - ws.lastHeartbeat) / 1_000_000;
            
            if (elapsedMs > HEARTBEAT_TIMEOUT_MS) {
                System.err.println("[Master] Worker " + ws.id + " marked dead (no heartbeat for " + elapsedMs + "ms)");
                deadWorkers.add(ws.id);
            } else if (elapsedMs > HEARTBEAT_TIMEOUT_MS * 0.75) {
                // Human touch: Warn about slow workers (75% of timeout)
                System.out.println("[Master] Worker " + ws.id + " is slow: " + elapsedMs + "ms since last heartbeat");
            }
        }
        
        // Remove dead workers and reassign their tasks
        for (int workerId : deadWorkers) {
            workers.remove(workerId);
            for (TaskState ts : tasks.values()) {
                if (!ts.completed && ts.assignedWorker == workerId) {
                    System.out.println("[Master] Reassigning task " + ts.id + " from dead worker " + workerId);
                    reassignTask(ts.id);
                }
            }
        }
    }
    
       private void reassignTask(int taskId) {
        TaskState ts = tasks.get(taskId);
        if (ts == null || ts.completed) return;
        
        // Human touch: Exponential backoff for repeated failures
        long elapsedMs = (System.nanoTime() - ts.assignedAt) / 1_000_000;
        int retryCount = (int) (elapsedMs / HEARTBEAT_TIMEOUT_MS);
        
        if (retryCount > 3) {
            System.err.println("[Master] Task " + taskId + " failed 3+ times — giving up");
            ts.completed = true; // Mark as failed to avoid infinite loop
            return;
        }
        
        // Find least-loaded alive worker
        WorkerState target = null;
        int minTasks = Integer.MAX_VALUE;
        
        for (WorkerState ws : workers.values()) {
            if (!ws.isAlive()) continue;
            
            // Count tasks assigned to this worker
            int assigned = 0;
            for (TaskState other : tasks.values()) {
                if (!other.completed && other.assignedWorker == ws.id) assigned++;
            }
            
            if (assigned < minTasks) {
                minTasks = assigned;
                target = ws;
            }
        }
        
        if (target != null) {
            try {
                Message taskMsg = new Message(Message.Type.TASK, taskId, ts.payload);
                byte[] packed = taskMsg.pack();
                synchronized (target.out) {
                    target.out.writeInt(packed.length);
                    target.out.write(packed);
                    target.out.flush();
                }
                ts.assignedWorker = target.id;
                ts.assignedAt = System.nanoTime();
                System.out.println("[Master] Reassigned task " + taskId + " to worker " + target.id + " (retry " + (retryCount + 1) + ")");
            } catch (IOException e) {
                System.err.println("[Master] Failed to reassign task " + taskId + " to worker " + target.id + ": " + e.getMessage());
                // Will be retried on next health check
            }
        } else {
            System.out.println("[Master] No alive workers available to reassign task " + taskId + " — will retry later");
        }
    }   
     public void submitTask(byte[] matrixData) {
        int taskId = nextTaskId.getAndIncrement();
        TaskState ts = new TaskState(taskId, matrixData);
        tasks.put(taskId, ts);
        
        // Assign to least-loaded worker immediately
        reassignTask(taskId);
    }

    public void stop() throws IOException {
        running = false;
        heartbeatMonitor.shutdown();
        serverSocket.close();
        // Close all worker sockets
        for (WorkerState ws : workers.values()) {
            try { ws.socket.close(); } catch (IOException e) { /* silent */ }
        }
    }
}