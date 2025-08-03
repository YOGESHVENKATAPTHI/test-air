const express = require("express");
const { Pool } = require("pg");
const multer = require("multer");
const { v4: uuidv4 } = require("uuid");
const crypto = require("crypto");
const http = require("http");
const { Readable, Transform, PassThrough } = require("stream");
const { pipeline } = require("stream/promises");
require("dotenv").config();
const fetch = require("node-fetch");
const Pusher = require("pusher");
const { Server: SocketIOServer } = require("socket.io");
const Redis = require("ioredis");
const Bull = require("bull");
const cluster = require("cluster");
const os = require("os");
const { LRUCache } = require("lru-cache");
const fs = require("fs");
const path = require("path");

// Enhanced configuration with rotation thresholds
const MAX_DB_CAPACITY_MB = 450;
const DB_ROTATION_THRESHOLD = 400; // Rotate when DB reaches 400MB
const MAX_CHUNK_SIZE = 200 * 1024 * 1024; // 200MB max chunk size
const MIN_CHUNK_SIZE = 20 * 1024 * 1024;  // 20MB min chunk size
const MESSAGE_CHUNK_SIZE = 8 * 1024; // 8KB for messages
const DROPBOX_ACCOUNT_MAX_CAPACITY = 1.8 * 1024 * 1024 * 1024; // 1.8GB per account
const DROPBOX_ROTATION_THRESHOLD = 1.5 * 1024 * 1024 * 1024; // Rotate at 1.5GB
const PROGRESS_UPDATE_INTERVAL = 1024; // 1KB intervals for progress
const MAX_CONCURRENT_UPLOADS = Math.max(2, os.cpus().length);
const MAX_PARALLEL_CHUNKS = Math.max(3, os.cpus().length);
const UPLOAD_TIMEOUT = 300000; // 5 minutes per chunk
const RETRY_ATTEMPTS = 3;
const CIRCUIT_BREAKER_THRESHOLD = 3;
const TEMP_UPLOAD_DIR = "./temp-uploads";
const DROPBOX_WAIT_TIMEOUT = 30000;
const MAX_WAIT_QUEUE_SIZE = 50;
const CHUNK_DOWNLOAD_SIZE = 5 * 1024 * 1024;
const OFFLINE_DETECTION_TIMEOUT = 30000;
const PRIORITY_BOOST_INTERVAL = 60000;
const FAIR_SHARE_TIME_SLICE = 5000; 
const MAX_OFFLINE_RETENTION = 3600000;
const HEARTBEAT_INTERVAL = 10000;
const QUEUE_REBALANCE_INTERVAL = 30000; 
const CONNECTION_REFRESH_INTERVAL = 60000; // 1 minute connection refresh
const QUEUE_PRIORITIES = {
  CRITICAL: 100, 
  HIGH: 75,  
  NORMAL: 50, 
  LOW: 25, 
  OFFLINE: 1 
};

if (!fs.existsSync(TEMP_UPLOAD_DIR)) {
  fs.mkdirSync(TEMP_UPLOAD_DIR, { recursive: true });
}

const CONNECTION_CACHE_TTL = 5 * 60 * 1000;
const DROPBOX_CACHE_TTL = 15 * 1000;
const FILE_METADATA_CACHE_TTL = 30 * 60 * 1000;
const numCPUs = Math.min(os.cpus().length, 4);

if (cluster.isMaster && process.env.NODE_ENV === 'production') {
  console.log(`[Cluster] Master ${process.pid} starting ${numCPUs} workers`);
  
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`[Cluster] Worker ${worker.process.pid} died`);
    cluster.fork();
  });
} else {
  startServer();
}

function startServer() {
  const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3,
    lazyConnect: true,
    keepAlive: 30000,
    connectTimeout: 10000,
    commandTimeout: 5000,
    maxMemoryPolicy: 'allkeys-lru'
  });

  const redisPub = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
  const redisSub = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
  [redis, redisPub, redisSub].forEach((client, index) => {
    const names = ['main', 'pub', 'sub'];
    client.on('error', (err) => console.error(`[Redis:${names[index]}] Error:`, err.message));
    client.on('connect', () => console.log(`[Redis:${names[index]}] Connected`));
    client.on('ready', () => console.log(`[Redis:${names[index]}] Ready`));
  });
  
  const connectionCache = new LRUCache({ max: 50, ttl: CONNECTION_CACHE_TTL });
  const dropboxCache = new LRUCache({ max: 100, ttl: DROPBOX_CACHE_TTL });
  const fileMetadataCache = new LRUCache({ max: 500, ttl: FILE_METADATA_CACHE_TTL });
  
  const createQueueConfig = (name, concurrency = 1) => ({
    name,
    redis: process.env.REDIS_URL || 'redis://localhost:6379',
    defaultJobOptions: {
      removeOnComplete: 100,
      removeOnFail: 50,
      attempts: RETRY_ATTEMPTS,
      backoff: {
        type: 'exponential',
        delay: 2000,
      },
      ttl: 3600000, // 1 hour TTL
    },
    settings: {
      stalledInterval: 30000,
      maxStalledCount: 3,
    }
  });

  const fileProcessingQueue = new Bull('file-processing-v4', createQueueConfig('file-processing', 1));
  const chunkUploadQueue = new Bull('chunk-upload-v4', createQueueConfig('chunk-upload', MAX_CONCURRENT_UPLOADS));
  const dropboxWaitQueue = new Bull('dropbox-wait-v4', createQueueConfig('dropbox-wait', 3));
  const priorityManagementQueue = new Bull('priority-management-v4', createQueueConfig('priority-management', 1));
  const offlineDetectionQueue = new Bull('offline-detection-v4', createQueueConfig('offline-detection', 1));

  const allQueues = [fileProcessingQueue, chunkUploadQueue, dropboxWaitQueue, priorityManagementQueue, offlineDetectionQueue];
  allQueues.forEach((queue, index) => {
    const queueNames = ['fileProcessing', 'chunkUpload', 'dropboxWait', 'priorityManagement', 'offlineDetection'];
    const queueName = queueNames[index];
    
    queue.on('error', (error) => console.error(`[Queue:${queueName}] Error:`, error.message));
    queue.on('waiting', (jobId) => console.log(`[Queue:${queueName}] Job ${jobId} waiting`));
    queue.on('active', (job) => console.log(`[Queue:${queueName}] Job ${job.id} started (Priority: ${job.opts.priority})`));
    queue.on('completed', (job, result) => console.log(`[Queue:${queueName}] Job ${job.id} completed`));
    queue.on('failed', (job, err) => console.error(`[Queue:${queueName}] Job ${job.id} failed:`, err.message));
    queue.on('stalled', (job) => console.warn(`[Queue:${queueName}] Job ${job.id} stalled`));
  });
  
  class EnhancedSessionManager {
    constructor() {
      this.roomSessions = new Map(); // roomName -> Set of socketIds
      this.socketToRoom = new Map();  // socketId -> roomName
      this.socketHeartbeats = new Map(); // socketId -> lastHeartbeat
      this.offlineFiles = new Map(); // fileId -> offline data
      this.userFiles = new Map(); // socketId -> Set of fileIds
      
      // Start heartbeat monitoring
      this.startHeartbeatMonitoring();
      
      // Start periodic cleanup
      this.startPeriodicCleanup();
    }

    addUserToRoom(socketId, roomName) {
      if (!this.roomSessions.has(roomName)) {
        this.roomSessions.set(roomName, new Set());
      }
      
      this.roomSessions.get(roomName).add(socketId);
      this.socketToRoom.set(socketId, roomName);
      this.updateHeartbeat(socketId);
      
      console.log(`[Session] User ${socketId} joined room ${roomName}`);
    }

    removeUserFromRoom(socketId) {
      const roomName = this.socketToRoom.get(socketId);
      if (roomName && this.roomSessions.has(roomName)) {
        this.roomSessions.get(roomName).delete(socketId);
        if (this.roomSessions.get(roomName).size === 0) {
          this.roomSessions.delete(roomName);
        }
      }
      
      this.socketToRoom.delete(socketId);
      this.socketHeartbeats.delete(socketId);
      
      // Handle user's files going offline
      this.handleUserOffline(socketId);
      
      console.log(`[Session] User ${socketId} left room ${roomName}`);
    }

    updateHeartbeat(socketId) {
      this.socketHeartbeats.set(socketId, Date.now());
    }

    isUserOnline(socketId) {
      const lastHeartbeat = this.socketHeartbeats.get(socketId);
      if (!lastHeartbeat) return false;
      return (Date.now() - lastHeartbeat) < OFFLINE_DETECTION_TIMEOUT;
    }

    getRoomUsers(roomName) {
      return this.roomSessions.get(roomName) || new Set();
    }

    isRoomOnline(roomName) {
      const users = this.getRoomUsers(roomName);
      return Array.from(users).some(socketId => this.isUserOnline(socketId));
    }

    addFileToUser(socketId, fileId) {
      if (!this.userFiles.has(socketId)) {
        this.userFiles.set(socketId, new Set());
      }
      this.userFiles.get(socketId).add(fileId);
    }

    async handleUserOffline(socketId) {
      const userFiles = this.userFiles.get(socketId);
      if (!userFiles) return;

      console.log(`[Session] Handling offline files for user ${socketId}`);
      
      for (const fileId of userFiles) {
        await this.markFileOffline(fileId, socketId);
      }
      
      this.userFiles.delete(socketId);
    }

    async markFileOffline(fileId, socketId) {
      try {
        const progressData = await redis.get(`progress:${fileId}`);
        if (!progressData) return;

        const parsed = JSON.parse(progressData);
        const roomName = this.socketToRoom.get(socketId);
        
        this.offlineFiles.set(fileId, {
          fileId,
          socketId,
          roomName,
          offlineAt: Date.now(),
          progressData: parsed,
          lastChunkIndex: parsed.uploadedChunks || 0
        });

        // Move all pending chunks of this file to offline priority
        await this.demoteFileChunks(fileId);
        
        console.log(`[Session] File ${fileId} marked as offline`);
        
        // Store in Redis for persistence
        await redis.setex(`offline_file:${fileId}`, MAX_OFFLINE_RETENTION / 1000, JSON.stringify({
          fileId,
          socketId,
          roomName,
          offlineAt: Date.now(),
          progressData: parsed
        }));

      } catch (error) {
        console.error(`[Session] Error marking file offline:`, error.message);
      }
    }

    async demoteFileChunks(fileId) {
      try {
        const jobs = await chunkUploadQueue.getJobs(['waiting', 'delayed', 'active']);
        
        for (const job of jobs) {
          if (job.data.fileId === fileId) {
            // Update job priority to offline level
            await job.update({
              ...job.data,
              isOffline: true,
              offlineAt: Date.now()
            });
            
            // Change priority if job is waiting
            if (job.opts.delay || job.processedOn === null) {
              await job.changePriority(QUEUE_PRIORITIES.OFFLINE);
              console.log(`[Queue] Demoted chunk ${job.data.chunkIndex} of file ${fileId} to offline priority`);
            }
          }
        }
      } catch (error) {
        console.error(`[Session] Error demoting file chunks:`, error.message);
      }
    }

    async handleUserReconnected(socketId, roomName) {
      console.log(`[Session] User ${socketId} reconnected to room ${roomName}`);
      
      // Check for offline files from this user/room
      const offlineFiles = await this.getOfflineFiles(roomName);
      
      if (offlineFiles.length > 0) {
        // Send popup notification to user
        io.to(socketId).emit('offline-files-detected', {
          files: offlineFiles.map(file => ({
            fileId: file.fileId,
            fileName: file.progressData.fileName,
            progress: file.progressData.progress,
            offlineTime: Date.now() - file.offlineAt
          }))
        });
      }
    }

    async getOfflineFiles(roomName) {
      const offlineFiles = [];
      for (const [fileId, data] of this.offlineFiles) {
        if (data.roomName === roomName) {
          offlineFiles.push(data);
        }
      }
      return offlineFiles;
    }

    async resumeOfflineFile(fileId, socketId) {
      const offlineData = this.offlineFiles.get(fileId);
      if (!offlineData) {
        throw new Error('Offline file not found');
      }

      console.log(`[Session] Resuming offline file ${fileId}`);
      
      // Update socket association
      offlineData.socketId = socketId;
      this.addFileToUser(socketId, fileId);
      
      // Promote file chunks back to normal priority
      await this.promoteFileChunks(fileId);
      
      // Remove from offline tracking
      this.offlineFiles.delete(fileId);
      await redis.del(`offline_file:${fileId}`);
      
      return offlineData;
    }

    async promoteFileChunks(fileId) {
      try {
        const jobs = await chunkUploadQueue.getJobs(['waiting', 'delayed']);
        
        for (const job of jobs) {
          if (job.data.fileId === fileId && job.data.isOffline) {
            // Update job data
            await job.update({
              ...job.data,
              isOffline: false,
              resumedAt: Date.now()
            });
            
            // Promote priority
            await job.changePriority(QUEUE_PRIORITIES.HIGH);
            console.log(`[Queue] Promoted chunk ${job.data.chunkIndex} of file ${fileId} to high priority`);
          }
        }
      } catch (error) {
        console.error(`[Session] Error promoting file chunks:`, error.message);
      }
    }

    async deleteOfflineFile(fileId) {
      const offlineData = this.offlineFiles.get(fileId);
      if (!offlineData) return;

      console.log(`[Session] Deleting offline file ${fileId}`);
      
      try {
        // Remove all related jobs from queues
        const allJobs = await Promise.all([
          fileProcessingQueue.getJobs(['waiting', 'delayed', 'active']),
          chunkUploadQueue.getJobs(['waiting', 'delayed', 'active']),
          dropboxWaitQueue.getJobs(['waiting', 'delayed', 'active'])
        ]);

        for (const jobs of allJobs) {
          for (const job of jobs) {
            if (job.data.fileId === fileId) {
              await job.remove();
              console.log(`[Cleanup] Removed job ${job.id} for deleted file ${fileId}`);
            }
          }
        }

        // Clean up temp file if exists
        if (offlineData.progressData.filePath && fs.existsSync(offlineData.progressData.filePath)) {
          fs.unlinkSync(offlineData.progressData.filePath);
          console.log(`[Cleanup] Deleted temp file for ${fileId}`);
        }

        // Clean up Redis data
        await Promise.all([
          redis.del(`progress:${fileId}`),
          redis.del(`file_lock:${fileId}`),
          redis.del(`offline_file:${fileId}`)
        ]);

        // Remove from tracking
        this.offlineFiles.delete(fileId);
        
      } catch (error) {
        console.error(`[Session] Error deleting offline file:`, error.message);
      }
    }

    startHeartbeatMonitoring() {
      setInterval(() => {
        const now = Date.now();
        const offlineUsers = [];
        
        for (const [socketId, lastHeartbeat] of this.socketHeartbeats) {
          if (now - lastHeartbeat > OFFLINE_DETECTION_TIMEOUT) {
            offlineUsers.push(socketId);
          }
        }
        
        offlineUsers.forEach(socketId => {
          console.log(`[Session] Detected offline user: ${socketId}`);
          this.handleUserOffline(socketId);
        });
        
      }, HEARTBEAT_INTERVAL);
    }

    startPeriodicCleanup() {
      setInterval(async () => {
        const now = Date.now();
        const expiredFiles = [];
        
        // Clean up old offline files
        for (const [fileId, data] of this.offlineFiles) {
          if (now - data.offlineAt > MAX_OFFLINE_RETENTION) {
            expiredFiles.push(fileId);
          }
        }
        
        for (const fileId of expiredFiles) {
          await this.deleteOfflineFile(fileId);
          console.log(`[Cleanup] Expired offline file: ${fileId}`);
        }
        
      }, 300000); // Every 5 minutes
    }
  }
  
  class AdvancedPriorityManager {
    constructor() {
      this.fileUploadSchedule = new Map();
      this.roundRobinIndex = 0;
      this.lastProcessingTime = new Map();
      
      this.startPriorityBoostScheduler();
      this.startFairShareScheduler();
      this.startQueueRebalancer();
    }

    async calculateJobPriority(jobData) {
      const { fileId, chunkIndex, totalChunks, isOffline, fileSize, socketId } = jobData;
      
      let basePriority = QUEUE_PRIORITIES.NORMAL;
      
      // Offline files get lowest priority
      if (isOffline) {
        return QUEUE_PRIORITIES.OFFLINE;
      }
      
      // Check if user is currently online
      if (socketId && sessionManager.isUserOnline(socketId)) {
        basePriority = QUEUE_PRIORITIES.HIGH;
      }
      
      // Boost priority for smaller files
      if (fileSize && fileSize < 50 * 1024 * 1024) { // Files smaller than 50MB
        basePriority += 10;
      }

      if (totalChunks > 10) {
        const completionBoost = Math.floor((chunkIndex / totalChunks) * 20);
        basePriority += completionBoost;
      }

      const waitTime = await this.getFileWaitTime(fileId);
      const timeBoost = Math.min(Math.floor(waitTime / 60000) * 5, 25); // Max 25 points boost
      basePriority += timeBoost;
      
      // Fair share adjustment
      const fairSharePenalty = await this.getFairSharePenalty(fileId);
      basePriority -= fairSharePenalty;
      
      return Math.max(Math.min(basePriority, QUEUE_PRIORITIES.CRITICAL), QUEUE_PRIORITIES.OFFLINE);
    }

    async getFileWaitTime(fileId) {
      try {
        const waitTimeKey = `file_wait_time:${fileId}`;
        let startTime = await redis.get(waitTimeKey);
        
        if (!startTime) {
          startTime = Date.now();
          await redis.setex(waitTimeKey, 3600, startTime.toString());
        } else {
          startTime = parseInt(startTime);
        }
        
        return Date.now() - startTime;
      } catch (error) {
        return 0;
      }
    }

    async getFairSharePenalty(fileId) {
      try {
        const lastProcessing = this.lastProcessingTime.get(fileId) || 0;
        const timeSinceLastProcessing = Date.now() - lastProcessing;
        
        // If file was processed recently, apply penalty
        if (timeSinceLastProcessing < FAIR_SHARE_TIME_SLICE) {
          return 15; // Penalty points
        }
        
        return 0;
      } catch (error) {
        return 0;
      }
    }

    async updateProcessingTime(fileId) {
      this.lastProcessingTime.set(fileId, Date.now());
    }

    async rebalanceQueue() {
      try {
        console.log('[PriorityManager] Starting queue rebalance');
        
        const waitingJobs = await chunkUploadQueue.getJobs(['waiting', 'delayed']);
        const updates = [];
        
        for (const job of waitingJobs) {
          const newPriority = await this.calculateJobPriority(job.data);
          
          if (job.opts.priority !== newPriority) {
            updates.push({
              job,
              newPriority,
              oldPriority: job.opts.priority
            });
          }
        }
        
        // Apply priority updates
        for (const update of updates) {
          await update.job.changePriority(update.newPriority);
          console.log(`[PriorityManager] Updated job ${update.job.id} priority: ${update.oldPriority} -> ${update.newPriority}`);
        }
        
        console.log(`[PriorityManager] Rebalanced ${updates.length} jobs`);
        
      } catch (error) {
        console.error('[PriorityManager] Error rebalancing queue:', error.message);
      }
    }

    startPriorityBoostScheduler() {
      setInterval(async () => {
        try {
          const waitingJobs = await chunkUploadQueue.getJobs(['waiting']);
          const boostCandidates = waitingJobs.filter(job => {
            const waitTime = Date.now() - job.timestamp;
            return waitTime > PRIORITY_BOOST_INTERVAL && !job.data.isOffline;
          });
          
          for (const job of boostCandidates.slice(0, 10)) { // Boost up to 10 jobs at a time
            const currentPriority = job.opts.priority || QUEUE_PRIORITIES.NORMAL;
            const newPriority = Math.min(currentPriority + 10, QUEUE_PRIORITIES.HIGH);
            
            await job.changePriority(newPriority);
            console.log(`[PriorityManager] Boosted job ${job.id} priority to ${newPriority}`);
          }
          
        } catch (error) {
          console.error('[PriorityManager] Error in priority boost scheduler:', error.message);
        }
      }, PRIORITY_BOOST_INTERVAL);
    }

    startFairShareScheduler() {
      setInterval(async () => {
        try {
          // Implement round-robin fair scheduling
          const activeFiles = await this.getActiveFiles();
          
          if (activeFiles.length <= 1) return;
          
          // Rotate the priority of files to ensure fair processing
          this.roundRobinIndex = (this.roundRobinIndex + 1) % activeFiles.length;
          const currentFavoriteFile = activeFiles[this.roundRobinIndex];
          
          // Boost chunks of the current favorite file
          const waitingJobs = await chunkUploadQueue.getJobs(['waiting']);
          const favoriteFileJobs = waitingJobs.filter(job => 
            job.data.fileId === currentFavoriteFile && !job.data.isOffline
          );
          
          for (const job of favoriteFileJobs.slice(0, 3)) { // Boost up to 3 chunks
            await job.changePriority(QUEUE_PRIORITIES.HIGH + 10);
          }
          
          console.log(`[FairShare] Boosted file ${currentFavoriteFile} in round-robin`);
          
        } catch (error) {
          console.error('[PriorityManager] Error in fair share scheduler:', error.message);
        }
      }, FAIR_SHARE_TIME_SLICE);
    }

    startQueueRebalancer() {
      setInterval(() => {
        this.rebalanceQueue();
      }, QUEUE_REBALANCE_INTERVAL);
    }

    async getActiveFiles() {
      try {
        const jobs = await chunkUploadQueue.getJobs(['waiting', 'active']);
        const fileIds = new Set();
        
        jobs.forEach(job => {
          if (!job.data.isOffline) {
            fileIds.add(job.data.fileId);
          }
        });
        
        return Array.from(fileIds);
      } catch (error) {
        console.error('[PriorityManager] Error getting active files:', error.message);
        return [];
      }
    }
  }

  // Initialize managers
  const sessionManager = new EnhancedSessionManager();
  const priorityManager = new AdvancedPriorityManager();

  // =============================================================================
  // EXPRESS & SOCKET.IO SETUP WITH ENHANCED FEATURES
  // =============================================================================
  const app = express();
  const server = http.createServer(app);
  const io = new SocketIOServer(server, {
    cors: { origin: "*" },
    pingTimeout: 60000,
    pingInterval: 25000,
    transports: ['websocket', 'polling'],
    maxHttpBufferSize: 1e8
  });

  app.use(require("cors")());
  app.use(express.json({ limit: '10mb' }));

  // Enhanced multer configuration
  const storage = multer.diskStorage({
    destination: (req, file, cb) => cb(null, TEMP_UPLOAD_DIR),
    filename: (req, file, cb) => {
      const sanitizedName = file.originalname.replace(/[^a-zA-Z0-9.-]/g, '_');
      const uniqueName = `${Date.now()}-${uuidv4()}-${sanitizedName}`;
      cb(null, uniqueName);
    }
  });

  const upload = multer({
    storage: storage,
    limits: {
      fileSize: 100 * 1024 * 1024 * 1024, // 100GB max file size
      fieldSize: 10 * 1024 * 1024,
      files: 1
    },
    fileFilter: (req, file, cb) => {
      console.log(`[Upload] Receiving file: ${file.originalname}`);
      if (!file.originalname || file.originalname.trim() === '') {
        return cb(new Error('Invalid filename'));
      }
      cb(null, true);
    }
  });

  const pusher = new Pusher({
    appId: "1984691",
    key: "d187547f8eaf3e9449f4",
    secret: "5928eab1ff0bf7f51820",
    cluster: "ap2",
    useTLS: true
  });

  // =============================================================================
  // ENHANCED SOCKET.IO HANDLERS WITH SESSION MANAGEMENT
  // =============================================================================
  io.on("connection", (socket) => {
    console.log(`[Socket.IO] Client connected: ${socket.id}`);

    socket.on("join-room", (roomName) => {
      socket.join(roomName);
      sessionManager.addUserToRoom(socket.id, roomName);
      
      console.log(`[Socket.IO] Socket ${socket.id} joined room "${roomName}"`);
      
      // Check for offline files and notify user
      sessionManager.handleUserReconnected(socket.id, roomName);
      
      socket.emit("room-joined", {
        roomName,
        message: "Successfully joined room",
        timestamp: new Date().toISOString()
      });
    });

    // Enhanced heartbeat system
    socket.on("heartbeat", () => {
      sessionManager.updateHeartbeat(socket.id);
      socket.emit("heartbeat-ack", { timestamp: Date.now() });
    });

    // Handle offline file decisions
    socket.on("offline-file-decision", async (data) => {
      const { fileId, action } = data; // action: 'resume' or 'delete'
      
      try {
        if (action === 'resume') {
          const offlineData = await sessionManager.resumeOfflineFile(fileId, socket.id);
          socket.emit("file-resumed", {
            fileId,
            fileName: offlineData.progressData.fileName,
            message: "File upload resumed successfully",
            progress: offlineData.progressData.progress
          });
          console.log(`[Socket.IO] File ${fileId} resumed by user ${socket.id}`);
        } else if (action === 'delete') {
          await sessionManager.deleteOfflineFile(fileId);
          socket.emit("file-deleted", {
            fileId,
            message: "Offline file deleted successfully"
          });
          console.log(`[Socket.IO] File ${fileId} deleted by user ${socket.id}`);
        }
      } catch (error) {
        console.error(`[Socket.IO] Error handling offline file decision:`, error.message);
        socket.emit("offline-file-error", {
          fileId,
          error: error.message
        });
      }
    });

    socket.on("request-upload-status", async (fileId) => {
      try {
        console.log(`[Socket.IO] Upload status requested for file ${fileId}`);
        const progressData = await redis.get(`progress:${fileId}`);
        if (progressData) {
          const parsed = JSON.parse(progressData);
          socket.emit("upload-progress", {
            ...parsed,
            message: "Upload status retrieved"
          });
        } else {
          socket.emit("upload-error", { 
            fileId, 
            error: "Upload status not found",
            message: "File not currently being processed"
          });
        }
      } catch (error) {
        console.error(`[Socket.IO] Error getting upload status:`, error.message);
        socket.emit("upload-error", { fileId, error: error.message });
      }
    });

    socket.on("cancel-upload", async (fileId) => {
      try {
        console.log(`[Socket.IO] Upload cancellation requested for file ${fileId}`);
        
        // Remove from offline tracking first
        sessionManager.offlineFiles.delete(fileId);
        
        // Get active jobs and remove them
        const allJobs = await Promise.all([
          fileProcessingQueue.getJobs(['waiting', 'active', 'delayed']),
          chunkUploadQueue.getJobs(['waiting', 'active', 'delayed']),
          dropboxWaitQueue.getJobs(['waiting', 'active', 'delayed'])
        ]);
        
        let cancelledJobs = 0;
        for (const jobs of allJobs) {
          for (const job of jobs) {
            if (job.data.fileId === fileId) {
              await job.remove();
              cancelledJobs++;
            }
          }
        }
        
        // Clean up progress and locks
        await Promise.all([
          redis.del(`progress:${fileId}`),
          redis.del(`file_lock:${fileId}`),
          redis.del(`offline_file:${fileId}`),
          redis.del(`file_wait_time:${fileId}`)
        ]);
        
        socket.emit("upload-cancelled", { 
          fileId, 
          cancelledJobs,
          message: "Upload cancelled successfully"
        });
        console.log(`[Socket.IO] Upload cancelled for file ${fileId} (${cancelledJobs} jobs removed)`);
      } catch (error) {
        console.error(`[Socket.IO] Error cancelling upload:`, error.message);
        socket.emit("upload-error", { fileId, error: error.message });
      }
    });

    socket.on("get-queue-stats", async () => {
      try {
        const [fileQueueStats, chunkQueueStats, waitQueueStats, priorityQueueStats, offlineQueueStats] = await Promise.all([
          fileProcessingQueue.getJobCounts(),
          chunkUploadQueue.getJobCounts(),
          dropboxWaitQueue.getJobCounts(),
          priorityManagementQueue.getJobCounts(),
          offlineDetectionQueue.getJobCounts()
        ]);

        const activeJobs = await Promise.all([
          chunkUploadQueue.getJobs(['active']),
          dropboxWaitQueue.getJobs(['waiting', 'delayed']),
          chunkUploadQueue.getJobs(['waiting'])
        ]);

        // Get priority distribution
        const waitingJobs = activeJobs[2];
        const priorityDistribution = {
          critical: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.CRITICAL).length,
          high: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.HIGH && job.opts.priority < QUEUE_PRIORITIES.CRITICAL).length,
          normal: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.NORMAL && job.opts.priority < QUEUE_PRIORITIES.HIGH).length,
          low: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.LOW && job.opts.priority < QUEUE_PRIORITIES.NORMAL).length,
          offline: waitingJobs.filter(job => job.opts.priority < QUEUE_PRIORITIES.LOW).length
        };

        socket.emit("queue-stats", {
          fileProcessing: fileQueueStats,
          chunkUpload: chunkQueueStats,
          dropboxWait: waitQueueStats,
          priorityManagement: priorityQueueStats,
          offlineDetection: offlineQueueStats,
          activeUploads: activeJobs[0].length,
          waitingForDropbox: activeJobs[1].length,
          priorityDistribution,
          offlineFiles: sessionManager.offlineFiles.size,
          onlineUsers: sessionManager.socketHeartbeats.size,
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        console.error(`[Socket.IO] Error getting queue stats:`, error.message);
        socket.emit("queue-stats-error", { error: error.message });
      }
    });

    socket.on("get-offline-files", async () => {
      try {
        const roomName = sessionManager.socketToRoom.get(socket.id);
        if (roomName) {
          const offlineFiles = await sessionManager.getOfflineFiles(roomName);
          socket.emit("offline-files-list", {
            files: offlineFiles.map(file => ({
              fileId: file.fileId,
              fileName: file.progressData.fileName,
              progress: file.progressData.progress,
              offlineTime: Date.now() - file.offlineAt,
              fileSize: file.progressData.totalSize
            }))
          });
        }
      } catch (error) {
        console.error(`[Socket.IO] Error getting offline files:`, error.message);
        socket.emit("offline-files-error", { error: error.message });
      }
    });

    socket.on("ping", () => {
      sessionManager.updateHeartbeat(socket.id);
      socket.emit("pong", { timestamp: Date.now() });
    });

    socket.on("disconnect", (reason) => {
      console.log(`[Socket.IO] Socket ${socket.id} disconnected: ${reason}`);
      sessionManager.removeUserFromRoom(socket.id);
    });

    socket.on("error", (error) => {
      console.error(`[Socket.IO] Socket ${socket.id} error:`, error.message);
    });
  });

  // =============================================================================
  // ENHANCED DATABASE CONNECTION MANAGEMENT WITH ROTATION
  // =============================================================================
  const connectionPools = new Map();
  const poolConfigs = {
    max: 10,
    min: 2,
    acquireTimeoutMillis: 8000,
    createTimeoutMillis: 8000,
    destroyTimeoutMillis: 3000,
    idleTimeoutMillis: 20000,
    reapIntervalMillis: 1000,
    createRetryIntervalMillis: 200,
    propagateCreateError: false
  };

  async function getOptimizedConnections() {
    const cacheKey = 'neon_connections';
    let cached = connectionCache.get(cacheKey);
    
    if (cached) {
      console.log("[Cache] Using cached Neon connections");
      return cached;
    }

    try {
      console.log("[Neon] Fetching fresh connections from Airtable");
      const connections = await fetchAirtableConnections();
      connectionCache.set(cacheKey, connections);
      console.log(`[Neon] Cached ${connections.records.length} connections`);
      return connections;
    } catch (error) {
      console.error("[Neon] Circuit breaker open or fetch failed:", error.message);
      cached = connectionCache.get(cacheKey);
      if (cached) {
        console.log("[Neon] Using stale cached connections as fallback");
        return cached;
      }
      throw error;
    }
  }

  async function fetchAirtableConnections() {
    console.log("[Airtable] Fetching base information");
    const response = await fetch("https://api.airtable.com/v0/meta/bases", {
      headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
      timeout: 10000
    });

    if (!response.ok) {
      throw new Error(`Airtable fetch failed: ${response.status} ${response.statusText}`);
    }

    const basesData = await response.json();
    const targetBases = basesData.bases.filter(b => b.name === "Base1");
    console.log(`[Airtable] Found ${targetBases.length} target bases`);
    
    let allRecords = [];
    for (const base of targetBases) {
      const url = `https://api.airtable.com/v0/${base.id}/Table%201`;
      try {
        const connectionsResponse = await fetch(url, {
          headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
          timeout: 10000
        });
        
        if (connectionsResponse.ok) {
          const connectionsData = await connectionsResponse.json();
          allRecords = allRecords.concat(
            connectionsData.records.map(record => ({ ...record, baseId: base.id }))
          );
          console.log(`[Airtable] Loaded ${connectionsData.records.length} records from base ${base.id}`);
        } else {
          console.warn(`[Airtable] Failed to fetch from base ${base.id}: ${connectionsResponse.status}`);
        }
      } catch (error) {
        console.warn(`[Airtable] Error fetching base ${base.id}:`, error.message);
      }
    }

    console.log(`[Airtable] Total records fetched: ${allRecords.length}`);
    return { records: allRecords };
  }

  function getConnectionPool(record) {
    const poolKey = record.id;
    
    if (!connectionPools.has(poolKey)) {
      console.log(`[Pool] Creating new connection pool for ${poolKey}`);
      const pool = new Pool({
        connectionString: record.fields.connectionstring,
        ssl: { rejectUnauthorized: false },
        ...poolConfigs
      });

      pool.on('error', (err) => console.error(`[Pool ${poolKey}] Error:`, err.message));
      pool.on('connect', () => console.log(`[Pool ${poolKey}] New client connected`));
      pool.on('remove', () => console.log(`[Pool ${poolKey}] Client removed`));

      connectionPools.set(poolKey, pool);
    }

    return connectionPools.get(poolKey);
  }

  async function getBestNeonConnection() {
    const connections = await getOptimizedConnections();
    let bestConnection = null;
    let maxSpace = 0;

    for (const record of connections.records) {
      try {
        const pool = getConnectionPool(record);
        const client = await pool.connect();
        const dbSize = await getDatabaseSizeMB(client);
        const freeSpace = MAX_DB_CAPACITY_MB - dbSize;
        
        // Only consider connections with significant free space
        if (freeSpace > maxSpace && freeSpace > (MAX_DB_CAPACITY_MB - DB_ROTATION_THRESHOLD)) {
          maxSpace = freeSpace;
          bestConnection = { record, pool, freeSpace };
        }
        client.release();
      } catch (error) {
        console.warn(`[Connection] Error checking DB ${record.id}:`, error.message);
      }
    }

    if (!bestConnection) throw new Error('No eligible Neon connections available');
    return bestConnection;
  }

  // Start periodic connection refresh
  setInterval(async () => {
    try {
      console.log('[Connection] Refreshing Neon connections cache');
      const connections = await fetchAirtableConnections();
      connectionCache.set('neon_connections', connections);
      console.log(`[Connection] Updated cache with ${connections.records.length} connections`);
    } catch (error) {
      console.error('[Connection] Refresh error:', error.message);
    }
  }, CONNECTION_REFRESH_INTERVAL);

  // =============================================================================
  // ENHANCED DROPBOX MANAGER WITH ACCOUNT ROTATION AND LOCKING
  // =============================================================================
  class EnhancedDropboxManager {
    constructor() {
      this.accountLocks = new Map();
      this.uploadSemaphore = new Map();
      this.waitingChunks = new Map();
      this.accountUsageCache = new Map();
    }

    async getEligibleAccounts(requiredSpace = MAX_CHUNK_SIZE) {
      const cacheKey = `dropbox_accounts_${requiredSpace}`;
      let cached = dropboxCache.get(cacheKey);
      
      if (cached) {
        console.log("[Cache] Using cached Dropbox accounts");
        return cached;
      }

      try {
        console.log("[Dropbox] Fetching fresh account information");
        const accounts = await this.fetchDropboxAccounts();
        const eligible = [];

        console.log(`[Dropbox] Evaluating ${accounts.length} accounts for eligibility`);
        for (const acc of accounts) {
          const used = parseFloat(acc.fields.storage || "0");
          const free = DROPBOX_ACCOUNT_MAX_CAPACITY - used;
          const isLocked = await this.isAccountLocked(acc.id);
          
          console.log(`[Dropbox] Account ${acc.id}: ${(free/(1024*1024*1024)).toFixed(2)}GB free, locked: ${isLocked}`);
          
          // Check if account is near capacity
          const nearCapacity = used > DROPBOX_ROTATION_THRESHOLD;
          if (free >= requiredSpace && !isLocked && !nearCapacity) {
            eligible.push({
              ...acc,
              freeSpace: free,
              priority: free
            });
          }
        }

        eligible.sort((a, b) => b.freeSpace - a.freeSpace);
        console.log(`[Dropbox] Found ${eligible.length} eligible accounts`);
        
        dropboxCache.set(cacheKey, eligible);
        return eligible;
      } catch (error) {
        console.error("[Dropbox] Error fetching accounts:", error.message);
        return [];
      }
    }

    async fetchDropboxAccounts() {
      console.log("[Dropbox] Fetching Dropbox bases from Airtable");
      const basesResponse = await fetch("https://api.airtable.com/v0/meta/bases", {
        headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
        timeout: 10000
      });

      if (!basesResponse.ok) {
        throw new Error(`Dropbox bases fetch failed: ${basesResponse.status} ${basesResponse.statusText}`);
      }

      const basesData = await basesResponse.json();
      const dropBases = basesData.bases.filter(b => b.name.toLowerCase() === "drop");
      console.log(`[Dropbox] Found ${dropBases.length} Dropbox bases`);
      
      let accounts = [];
      for (const base of dropBases) {
        const url = `https://api.airtable.com/v0/${base.id}/Table%201`;
        try {
          const response = await fetch(url, {
            headers: { Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}` },
            timeout: 10000
          });
          
          if (response.ok) {
            const data = await response.json();
            accounts = accounts.concat(
              data.records.map(record => ({ ...record, baseId: base.id }))
            );
            console.log(`[Dropbox] Loaded ${data.records.length} accounts from base ${base.id}`);
          } else {
            console.warn(`[Dropbox] Failed to fetch accounts from base ${base.id}: ${response.status}`);
          }
        } catch (error) {
          console.warn(`[Dropbox] Error fetching base ${base.id}:`, error.message);
        }
      }

      console.log(`[Dropbox] Total accounts fetched: ${accounts.length}`);
      return accounts;
    }

    async isAccountLocked(accountId) {
      try {
        const key = `dropbox_lock_${accountId}`;
        const lockExists = await redis.exists(key);
        return lockExists === 1;
      } catch (error) {
        console.error(`[Dropbox] Error checking lock for account ${accountId}:`, error.message);
        return false;
      }
    }

    async acquireLock(account, lockId, lockOwner) {
      try {
        const key = `dropbox_lock_${account.id}`;
        const acquired = await redis.set(key, lockId, 'PX', 180000, 'NX');
        
        if (acquired) {
          this.accountLocks.set(account.id, lockId);
          console.log(`[Dropbox] Lock acquired for account ${account.id} (${lockId})`);
          
          // Update Airtable with lock details
          await this.updateAccountLockStatus(account, {
            upload_lock: lockId,
            lock_timestamp: new Date().toISOString(),
            lock_owner: lockOwner,
            lock_expiry: new Date(Date.now() + 3600000).toISOString(), // 1 hour expiry
            availability: 0 // Mark as unavailable
          });
          
          return true;
        }
        
        console.warn(`[Dropbox] Failed to acquire lock for account ${account.id}`);
        return false;
      } catch (error) {
        console.error(`[Dropbox] Error acquiring lock for account ${account.id}:`, error.message);
        return false;
      }
    }

    async releaseLock(account, lockId) {
      try {
        const key = `dropbox_lock_${account.id}`;
        const currentLock = await redis.get(key);
        
        if (currentLock === lockId) {
          await redis.del(key);
          this.accountLocks.delete(account.id);
          console.log(`[Dropbox] Lock released for account ${account.id} (${lockId})`);
          
          // Clear lock details in Airtable
          await this.updateAccountLockStatus(account, {
            upload_lock: null,
            lock_timestamp: null,
            lock_owner: null,
            lock_expiry: null,
            availability: 1 // Mark as available
          });
          
          await this.notifyWaitingChunks();
          return true;
        }
        
        console.warn(`[Dropbox] Lock mismatch for account ${account.id}. Expected: ${lockId}, Got: ${currentLock}`);
        return false;
      } catch (error) {
        console.error(`[Dropbox] Error releasing lock for account ${account.id}:`, error.message);
        return false;
      }
    }

    async updateAccountLockStatus(account, fields) {
      try {
        const url = `https://api.airtable.com/v0/${account.baseId}/Table%201/${account.id}`;
        const response = await fetch(url, {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({ fields }),
          timeout: 15000
        });

        if (!response.ok) {
          const errorBody = await response.text();
          throw new Error(`Failed to update lock status: ${response.status} - ${errorBody}`);
        }

        console.log(`[Dropbox] Updated lock status for account ${account.id}`);
      } catch (error) {
        console.error(`[Dropbox] Error updating lock status for account ${account.id}:`, error.message);
      }
    }

    async notifyWaitingChunks() {
      try {
        await dropboxWaitQueue.add('process-waiting-chunks', {
          timestamp: Date.now()
        }, {
          priority: 10,
          delay: 100
        });
      } catch (error) {
        console.error('[Dropbox] Error notifying waiting chunks:', error.message);
      }
    }

    async uploadChunkStream(account, chunkData, onProgress) {
      const { filePath, chunkSize, startByte, fileName, chunkIndex, fileSize, fileId } = chunkData;
      const lockId = `${fileId}_${chunkIndex}_${Date.now()}`;
      
      console.log(`[Dropbox] Starting upload for chunk ${chunkIndex} to account ${account.id}`);
      
      if (!fs.existsSync(filePath)) {
        throw new Error(`Temp file no longer exists: ${filePath}`);
      }

      const fileStats = fs.statSync(filePath);
      if (startByte >= fileStats.size) {
        throw new Error(`Invalid byte range: ${startByte} >= ${fileStats.size}`);
      }

      const lockAcquired = await this.acquireLock(account, lockId, fileId);
      if (!lockAcquired) {
        throw new Error(`Failed to acquire lock for account ${account.id}`);
      }

      try {
        let accessToken = account.fields.accesstoken;
        if (!accessToken || this.isTokenExpired(accessToken)) {
          console.log(`[Dropbox] Refreshing access token for account ${account.id}`);
          accessToken = await this.refreshAccessToken(account);
        }

        const dropboxPath = `/infinityshare/${fileName}_chunk_${chunkIndex}_${uuidv4()}`;
        
        console.log(`[Dropbox] Creating read stream for chunk ${chunkIndex} (${startByte} to ${startByte + chunkSize - 1})`);
        const readStream = fs.createReadStream(filePath, {
          start: startByte,
          end: startByte + chunkSize - 1,
          highWaterMark: 1024 * 1024 // 1MB chunks for better progress granularity
        });

        readStream.on('error', (error) => {
          console.error(`[Stream] Error reading chunk ${chunkIndex}:`, error);
          throw error;
        });
        
        let uploadedBytes = 0;
        let lastUpdateTime = Date.now();
        let lastUploadedBytes = 0;
        const startTime = Date.now();
        
        // Enhanced progress tracking with speed calculation
        const progressStream = new Transform({
          transform(chunk, encoding, callback) {
            uploadedBytes += chunk.length;
            const now = Date.now();
            
            // Calculate speed
            const timeDelta = (now - lastUpdateTime) / 1000;
            const bytesDelta = uploadedBytes - lastUploadedBytes;
            const speed = timeDelta > 0 ? bytesDelta / timeDelta : 0;
            
            // Emit progress at 1KB intervals or at least once per second
            if (uploadedBytes - lastUploadedBytes >= PROGRESS_UPDATE_INTERVAL || 
                now - lastUpdateTime >= 1000) {
              
              if (onProgress) {
                onProgress({
                  uploadedBytes: uploadedBytes,
                  chunkProgress: (uploadedBytes / chunkSize) * 100,
                  absolutePosition: startByte + uploadedBytes,
                  totalProgress: ((startByte + uploadedBytes) / fileSize) * 100,
                  speed: Math.round(speed / 1024) // KB/s
                });
              }
              
              lastUploadedBytes = uploadedBytes;
              lastUpdateTime = now;
            }
            
            this.push(chunk);
            callback();
          }
        });
        
        console.log(`[Dropbox] Uploading to ${dropboxPath}`);
        const uploadPromise = fetch("https://content.dropboxapi.com/2/files/upload", {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/octet-stream",
            "Dropbox-API-Arg": JSON.stringify({
              path: dropboxPath,
              mode: "add",
              autorename: true,
              mute: false
            }),
            "Content-Length": chunkSize.toString()
          },
          body: readStream.pipe(progressStream),
          timeout: UPLOAD_TIMEOUT
        });

        const response = await Promise.race([
          uploadPromise,
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Upload timeout')), UPLOAD_TIMEOUT)
          )
        ]);

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(`Dropbox upload failed (${response.status}): ${errorText}`);
        }

        console.log(`[Dropbox] Upload successful for chunk ${chunkIndex}, creating permanent link`);
        const permanentUrl = await this.createPermanentLink(accessToken, dropboxPath);
        await this.updateStorageUsage(account, chunkSize);
        
        // Final progress update
        const totalTime = (Date.now() - startTime) / 1000;
        const avgSpeed = totalTime > 0 ? (chunkSize / totalTime) / 1024 : 0;
        
        if (onProgress) {
          onProgress({
            uploadedBytes: chunkSize,
            chunkProgress: 100,
            absolutePosition: startByte + chunkSize,
            totalProgress: ((startByte + chunkSize) / fileSize) * 100,
            speed: Math.round(avgSpeed) // KB/s
          });
        }
        
        console.log(`[Dropbox] Chunk ${chunkIndex} upload completed successfully`);
        return {
          url: permanentUrl,
          path: dropboxPath,
          size: chunkSize
        };

      } finally {
        await this.releaseLock(account, lockId);
      }
    }

    async createPermanentLink(accessToken, dropboxPath) {
      try {
        console.log(`[Dropbox] Creating permanent link for ${dropboxPath}`);
        const response = await fetch("https://api.dropboxapi.com/2/sharing/create_shared_link_with_settings", {
          method: "POST",
          headers: {
            "Authorization": `Bearer ${accessToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            path: dropboxPath,
            settings: { requested_visibility: "public" }
          }),
          timeout: 30000
        });

        let result;
        if (!response.ok) {
          const errorData = await response.json();
          if (errorData.error_summary?.includes("shared_link_already_exists")) {
            console.log(`[Dropbox] Link already exists, retrieving existing link`);
            const listResponse = await fetch("https://api.dropboxapi.com/2/sharing/list_shared_links", {
              method: "POST",
              headers: {
                "Authorization": `Bearer ${accessToken}`,
                "Content-Type": "application/json"
              },
              body: JSON.stringify({ path: dropboxPath, direct_only: true }),
              timeout: 30000
            });
            
            const listData = await listResponse.json();
            result = listData.links[0];
          } else {
            throw new Error(`Failed to create shared link: ${errorData.error_summary}`);
          }
        } else {
          result = await response.json();
        }

        let directUrl = result.url;
        if (directUrl.includes("dl=0")) {
          directUrl = directUrl.replace("dl=0", "dl=1");
        } else if (!directUrl.includes("dl=1")) {
          const separator = directUrl.includes("?") ? "&" : "?";
          directUrl += `${separator}dl=1`;
        }

        console.log(`[Dropbox] Permanent link created: ${directUrl}`);
        return directUrl;
      } catch (error) {
        console.error("[Dropbox] Error creating permanent link:", error);
        throw error;
      }
    }

    async refreshAccessToken(account) {
      try {
        console.log(`[Dropbox] Refreshing token for account ${account.id}`);
        const params = new URLSearchParams({
          grant_type: 'refresh_token',
          refresh_token: account.fields.refreshtoken,
          client_id: account.fields.appkey,
          client_secret: account.fields.appsecret
        });

        const response = await fetch('https://api.dropboxapi.com/oauth2/token', {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body: params,
          timeout: 30000
        });

        if (!response.ok) {
          const error = await response.json();
          throw new Error(`Token refresh failed: ${error.error_description || error.error}`);
        }

        const tokenData = await response.json();
        await this.updateAccountToken(account, tokenData.access_token);
        console.log(`[Dropbox] Token refreshed successfully for account ${account.id}`);
        return tokenData.access_token;
      } catch (error) {
        console.error(`[Dropbox] Token refresh failed for account ${account.id}:`, error.message);
        throw error;
      }
    }

    async updateAccountToken(account, newToken) {
      try {
        const url = `https://api.airtable.com/v0/${account.baseId}/Table%201/${account.id}`;
        const response = await fetch(url, {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            fields: { accesstoken: newToken }
          }),
          timeout: 15000
        });

        if (!response.ok) {
          const errorBody = await response.text();
          console.error("[Dropbox] Airtable Error Response:", errorBody);
          throw new Error(`Failed to update token: ${response.status}`);
        }

        account.fields.accesstoken = newToken;
        console.log(`[Dropbox] Updated access token for account ${account.id}`);
      } catch (error) {
        console.error(`[Dropbox] Error updating token for account ${account.id}:`, error.message);
        throw error;
      }
    }

    async updateStorageUsage(account, additionalBytes) {
      try {
        const currentStorage = parseFloat(account.fields.storage || "0");
        const newStorage = currentStorage + additionalBytes;
        
        console.log(`[Dropbox] Updating storage for account ${account.id}: ${(currentStorage/(1024*1024)).toFixed(2)}MB -> ${(newStorage/(1024*1024)).toFixed(2)}MB`);
        
        const url = `https://api.airtable.com/v0/${account.baseId}/Table%201/${account.id}`;
        const response = await fetch(url, {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${process.env.AIRTABLE_API_KEY}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify({
            fields: { storage: newStorage.toString() }
          }),
          timeout: 15000
        });

        if (!response.ok) {
          throw new Error(`Failed to update storage: ${response.status}`);
        }

        account.fields.storage = newStorage.toString();
        dropboxCache.delete(`dropbox_accounts_${MAX_CHUNK_SIZE}`);
        console.log(`[Dropbox] Storage updated successfully for account ${account.id}`);
      } catch (error) {
        console.error(`[Dropbox] Error updating storage for account ${account.id}:`, error.message);
      }
    }

    isTokenExpired(token) {
      return false;
    }
  }

  const dropboxManager = new EnhancedDropboxManager();

  // =============================================================================
  // ENHANCED FILE PROCESSOR WITH PRIORITY INTEGRATION
  // =============================================================================
  function getOptimalChunkSize(fileSize) {
    if (fileSize > 10 * 1024 * 1024 * 1024) return 100 * 1024 * 1024; // 100MB for >10GB
    if (fileSize > 1 * 1024 * 1024 * 1024) return 50 * 1024 * 1024;   // 50MB for >1GB
    if (fileSize > 100 * 1024 * 1024) return 25 * 1024 * 1024;        // 25MB for >100MB
    if (fileSize > 10 * 1024 * 1024) return 10 * 1024 * 1024;         // 10MB for >10MB
    return Math.max(fileSize, 1024 * 1024); // Minimum 1MB or file size
  }

  class OptimizedFileProcessor {
    constructor() {
      this.activeUploads = new Map();
      this.fileLocks = new Map();
    }

    async processFile(filePath, fileName, roomName, userId, socketId) {
      const fileId = uuidv4();
      
      try {
        if (!fs.existsSync(filePath)) {
          throw new Error(`File not found: ${filePath}`);
        }

        const stats = fs.statSync(filePath);
        const fileSize = stats.size;
        
        if (fileSize === 0) {
          throw new Error('Cannot upload empty file');
        }

        const chunkSize = getOptimalChunkSize(fileSize);
        const totalChunks = Math.ceil(fileSize / chunkSize);
        
        console.log(`[FileProcessor] Processing ${fileName} (${totalChunks} chunks, ${(fileSize / (1024*1024)).toFixed(2)}MB, chunk size: ${(chunkSize / (1024*1024)).toFixed(2)}MB)`);

        // Register file with session manager
        sessionManager.addFileToUser(socketId, fileId);
        
        await this.initializeProgress(fileId, fileName, fileSize, totalChunks, socketId, filePath);
        await redis.setex(`file_lock:${fileId}`, 3600, totalChunks.toString());

        // Calculate initial priority based on file size and user
        const basePriority = fileSize < 50 * 1024 * 1024 ? QUEUE_PRIORITIES.HIGH : QUEUE_PRIORITIES.NORMAL;
        
        await fileProcessingQueue.add('process-file', {
          fileId,
          fileName,
          fileSize,
          filePath,
          totalChunks,
          chunkSize,
          roomName,
          userId,
          socketId
        }, {
          priority: basePriority,
          delay: 0,
          attempts: 2
        });

        console.log(`[FileProcessor] File ${fileId} queued for processing with priority ${basePriority}`);
        return { fileId, totalChunks, message: "File processing initiated" };

      } catch (error) {
        console.error(`[FileProcessor] Error processing file ${fileName}:`, error.message);
        
        // Clean up on error
        try {
          if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
            console.log(`[Cleanup] Deleted file on processing error: ${filePath}`);
          }
        } catch (cleanupError) {
          console.error(`[Cleanup] Error deleting file:`, cleanupError.message);
        }
        
        throw error;
      }
    }

    async initializeProgress(fileId, fileName, totalSize, totalChunks, socketId, filePath) {
      const progressData = {
        fileId,
        fileName,
        totalSize,
        totalChunks,
        uploadedChunks: 0,
        uploadedBytes: 0,
        progress: 0,
        status: 'processing',
        socketId,
        filePath,  // Store file path for offline handling
        startTime: Date.now(),
        errors: [],
        lastUpdate: Date.now()
      };

      try {
        await redis.setex(`progress:${fileId}`, 3600, JSON.stringify(progressData));
        console.log(`[Progress] Initialized progress tracking for file ${fileId}`);
        
        if (socketId) {
          io.to(socketId).emit("upload-progress", {
            fileId,
            fileName,
            progress: 0,
            uploadedMB: 0,
            totalSizeMB: (totalSize / (1024 * 1024)).toFixed(2),
            uploadedChunks: 0,
            totalChunks,
            status: 'processing',
            message: 'File processing started'
          });
        }
      } catch (error) {
        console.error(`[Progress] Error initializing progress for ${fileId}:`, error.message);
        throw error;
      }
    }

    async updateProgress(fileId, chunkIndex, chunkSize, socketId, progressData = null) {
      try {
        const progressKey = `progress:${fileId}`;
        const currentProgressData = JSON.parse(await redis.get(progressKey) || '{}');
        
        if (!currentProgressData.fileId) {
          console.warn(`[Progress] No progress data found for file ${fileId}`);
          return;
        }
        
        currentProgressData.uploadedChunks = (currentProgressData.uploadedChunks || 0) + 1;
        currentProgressData.uploadedBytes = (currentProgressData.uploadedBytes || 0) + chunkSize;
        currentProgressData.progress = (currentProgressData.uploadedBytes / currentProgressData.totalSize) * 100;
        currentProgressData.lastUpdate = Date.now();

        if (progressData) {
          currentProgressData.currentChunkProgress = progressData.chunkProgress;
          currentProgressData.currentChunkIndex = chunkIndex;
        }

        await redis.setex(progressKey, 3600, JSON.stringify(currentProgressData));

        if (socketId && sessionManager.isUserOnline(socketId)) {
          const progressUpdate = {
            fileId,
            fileName: currentProgressData.fileName,
            progress: currentProgressData.progress,
            uploadedMB: (currentProgressData.uploadedBytes / (1024 * 1024)).toFixed(2),
            totalSizeMB: (currentProgressData.totalSize / (1024 * 1024)).toFixed(2),
            uploadedChunks: currentProgressData.uploadedChunks,
            totalChunks: currentProgressData.totalChunks,
            chunkIndex,
            currentChunkProgress: progressData?.chunkProgress || 0,
            speed: progressData?.speed || 0,
            estimatedTimeRemaining: this.calculateETA(currentProgressData),
            status: 'uploading'
          };
          
          io.to(socketId).emit("upload-progress", progressUpdate);
          console.log(`[Progress] Updated: ${currentProgressData.uploadedChunks}/${currentProgressData.totalChunks} chunks (${currentProgressData.progress.toFixed(1)}%)`);
        }

        if (currentProgressData.uploadedChunks >= currentProgressData.totalChunks) {
          await this.markFileComplete(fileId, socketId);
        }
      } catch (error) {
        console.error(`[Progress] Error updating progress for ${fileId}:`, error.message);
      }
    }

    calculateETA(progressData) {
      const elapsed = Date.now() - progressData.startTime;
      const progress = progressData.progress / 100;
      
      if (progress > 0.01) {
        const totalEstimated = elapsed / progress;
        const remaining = totalEstimated - elapsed;
        return Math.max(0, Math.round(remaining / 1000));
      }
      
      return null;
    }

    async markFileComplete(fileId, socketId) {
      try {
        const progressKey = `progress:${fileId}`;
        const progressData = JSON.parse(await redis.get(progressKey) || '{}');
        
        if (!progressData.fileId) {
          console.warn(`[Progress] No progress data found for completing file ${fileId}`);
          return;
        }
        
        progressData.status = 'completed';
        progressData.completedAt = Date.now();
        progressData.progress = 100;
        
        await redis.setex(progressKey, 3600, JSON.stringify(progressData));
        await redis.del(`file_lock:${fileId}`);
        
        const totalTime = (progressData.completedAt - progressData.startTime) / 1000;
        
        if (socketId && sessionManager.isUserOnline(socketId)) {
          io.to(socketId).emit("file-upload-complete", {
            fileId,
            fileName: progressData.fileName,
            message: "File upload completed successfully",
            totalTime: totalTime,
            totalSize: progressData.totalSize,
            totalChunks: progressData.totalChunks
          });
        }

        if (progressData.roomName) {
          pusher.trigger(progressData.roomName, "new-content", {
            type: "file",
            fileId,
            fileName: progressData.fileName,
            message: "New file uploaded",
            fileSize: progressData.totalSize
          });
        }

        console.log(`[FileProcessor] File ${fileId} (${progressData.fileName}) upload completed in ${totalTime.toFixed(2)}s`);
      } catch (error) {
        console.error(`[Progress] Error marking file complete for ${fileId}:`, error.message);
      }
    }

    async handleChunkError(fileId, chunkIndex, error, socketId) {
      try {
        const progressKey = `progress:${fileId}`;
        const progressData = JSON.parse(await redis.get(progressKey) || '{}');
        
        if (!progressData.fileId) {
          console.warn(`[Progress] No progress data found for error handling ${fileId}`);
          return;
        }
        
        if (!progressData.errors) progressData.errors = [];
        progressData.errors.push({
          chunkIndex,
          error: error.message,
          timestamp: Date.now()
        });

        if (progressData.errors.length > Math.ceil(progressData.totalChunks * 0.1)) {
          progressData.status = 'failed';
          progressData.failedAt = Date.now();
          
          if (socketId && sessionManager.isUserOnline(socketId)) {
            io.to(socketId).emit("file-upload-failed", {
              fileId,
              fileName: progressData.fileName,
              error: `Too many chunk failures: ${progressData.errors.length}`,
              totalErrors: progressData.errors.length
            });
          }
        }

        await redis.setex(progressKey, 3600, JSON.stringify(progressData));

        if (socketId && sessionManager.isUserOnline(socketId)) {
          io.to(socketId).emit("upload-error", {
            fileId,
            chunkIndex,
            error: error.message,
            totalErrors: progressData.errors.length,
            maxErrors: Math.ceil(progressData.totalChunks * 0.1)
          });
        }

        console.warn(`[Progress] Chunk ${chunkIndex} error for file ${fileId}: ${error.message} (${progressData.errors.length} total errors)`);
      } catch (error) {
        console.error(`[Progress] Error handling chunk error for ${fileId}:`, error.message);
      }
    }
  }

  const fileProcessor = new OptimizedFileProcessor();

  // =============================================================================
  // ENHANCED QUEUE PROCESSORS WITH PRIORITY MANAGEMENT
  // =============================================================================
  
  // File processing queue processor
  fileProcessingQueue.process('process-file', 1, async (job) => {
    const { fileId, fileName, fileSize, filePath, totalChunks, chunkSize, roomName, userId, socketId } = job.data;
    
    console.log(`[FileQueue] Processing file ${fileId} (${fileName}) with ${totalChunks} chunks from ${filePath}`);

    try {
      if (!fs.existsSync(filePath)) {
        throw new Error(`File not found during processing: ${filePath}`);
      }

      const stats = fs.statSync(filePath);
      if (stats.size !== fileSize) {
        throw new Error(`File size mismatch. Expected: ${fileSize}, Actual: ${stats.size}`);
      }

      // Queue all chunks with dynamic priority calculation
      const chunkPromises = [];
      for (let i = 0; i < totalChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, fileSize);
        const currentChunkSize = end - start;
        
        console.log(`[FileQueue] Queueing chunk ${i + 1}/${totalChunks} (${(currentChunkSize / (1024*1024)).toFixed(2)}MB)`);
        
        const chunkData = {
          fileId,
          fileName,
          filePath,
          fileSize,
          chunkIndex: i,
          chunkSize: currentChunkSize,
          startByte: start,
          totalChunks,
          roomName,
          userId,
          socketId,
          isOffline: false
        };

        const priority = await priorityManager.calculateJobPriority(chunkData);
        
        const chunkPromise = chunkUploadQueue.add('upload-chunk', chunkData, {
          priority: priority,
          attempts: RETRY_ATTEMPTS,
          backoff: {
            type: 'exponential',
            delay: 1000
          },
          delay: i * 50 // Small staggered delay
        });
        
        chunkPromises.push(chunkPromise);
      }

      await Promise.all(chunkPromises);
      console.log(`[FileQueue] All ${totalChunks} chunks for file ${fileId} have been queued successfully`);
      
      return { message: `${totalChunks} chunks queued for upload` };

    } catch (error) {
      console.error(`[FileQueue] Error processing file ${fileId}:`, error.message);
      
      if (socketId && sessionManager.isUserOnline(socketId)) {
        io.to(socketId).emit("file-upload-error", {
          fileId,
          fileName,
          error: error.message,
          stage: 'file-processing'
        });
      }
      
      try {
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
          console.log(`[Cleanup] Deleted temp file on processing error: ${filePath}`);
        }
      } catch (cleanupError) {
        console.error(`[Cleanup] Error deleting temp file:`, cleanupError.message);
      }
      
      throw error;
    }
  });

  // Enhanced chunk upload queue processor with priority awareness
  chunkUploadQueue.process('upload-chunk', MAX_CONCURRENT_UPLOADS, async (job) => {
    const chunkData = job.data;
    const { fileId, fileName, filePath, fileSize, chunkIndex, chunkSize, startByte, totalChunks, roomName, socketId, isOffline } = chunkData;
    
    console.log(`[ChunkQueue] Processing chunk ${chunkIndex + 1}/${totalChunks} for file ${fileId} (Priority: ${job.opts.priority}, Offline: ${isOffline})`);

    // Update processing time for fair share
    await priorityManager.updateProcessingTime(fileId);

    try {
      // Check if user is still online (unless it's a resumed offline file)
      if (!isOffline && socketId && !sessionManager.isUserOnline(socketId)) {
        console.log(`[ChunkQueue] User ${socketId} appears offline, demoting chunk ${chunkIndex}`);
        // Mark chunk as offline and reduce priority
        await job.update({
          ...chunkData,
          isOffline: true,
          offlineAt: Date.now()
        });
        throw new Error(`User went offline during upload`);
      }

      if (!fs.existsSync(filePath)) {
        throw new Error(`Temp file no longer exists: ${filePath}`);
      }

      const stats = fs.statSync(filePath);
      if (startByte >= stats.size) {
        throw new Error(`Invalid chunk bounds: start ${startByte} >= file size ${stats.size}`);
      }

      let eligibleAccounts = await dropboxManager.getEligibleAccounts(chunkSize);
      
      if (eligibleAccounts.length === 0) {
        console.log(`[ChunkQueue] No eligible Dropbox accounts for chunk ${chunkIndex}, adding to wait queue`);
        const waitId = await dropboxManager.waitForAvailableAccount(chunkData);
        throw new Error(`No eligible Dropbox accounts available, queued for waiting (${waitId})`);
      }

      console.log(`[ChunkQueue] Found ${eligibleAccounts.length} eligible accounts for chunk ${chunkIndex}`);

      let uploadResult = null;
      let lastError = null;

      for (const account of eligibleAccounts) {
        try {
          console.log(`[ChunkQueue] Attempting upload of chunk ${chunkIndex} to account ${account.id}`);
          
          const onProgress = (progressInfo) => {
            if (socketId && sessionManager.isUserOnline(socketId)) {
              io.to(socketId).emit("chunk-progress", {
                fileId,
                fileName,
                chunkIndex,
                chunkProgress: progressInfo.chunkProgress,
                totalProgress: progressInfo.totalProgress,
                uploadedMB: (progressInfo.absolutePosition / (1024 * 1024)).toFixed(2),
                totalSizeMB: (fileSize / (1024 * 1024)).toFixed(2),
                speed: progressInfo.speed || 0
              });
            }
          };

          uploadResult = await dropboxManager.uploadChunkStream(account, {
            ...chunkData,
            fileId  // Pass fileId for lock ownership
          }, onProgress);
          console.log(`[ChunkQueue] Chunk ${chunkIndex} uploaded successfully to account ${account.id}`);
          break;
          
        } catch (error) {
          lastError = error;
          console.warn(`[ChunkQueue] Failed to upload chunk ${chunkIndex} to account ${account.id}: ${error.message}`);
          
          if (error.message.includes('Failed to acquire lock')) {
            continue;
          }
          
          if (error.message.includes('token') || error.message.includes('auth')) {
            console.warn(`[ChunkQueue] Authentication error, trying next account`);
            continue;
          }
          
          continue;
        }
      }

      if (!uploadResult) {
        throw new Error(`All ${eligibleAccounts.length} Dropbox accounts failed for chunk ${chunkIndex}: ${lastError?.message || 'Unknown error'}`);
      }

      // Store chunk metadata
      await storeChunkMetadata({
        fileId,
        fileName,
        chunkIndex,
        totalChunks,
        roomName,
        url: uploadResult.url,
        size: chunkSize,
        checksum: uploadResult.checksum || "N/A"
      });

      // Update progress
      await fileProcessor.updateProgress(fileId, chunkIndex, chunkSize, socketId);
      console.log(`[ChunkQueue] Chunk ${chunkIndex + 1}/${totalChunks} completed successfully`);

      // Check if this was the last chunk and clean up the file
      const remainingChunks = await redis.decr(`file_lock:${fileId}`);
      if (remainingChunks <= 0) {
        try {
          if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
            console.log(`[Cleanup] Deleted temp file after all chunks completed: ${filePath}`);
          }
        } catch (cleanupError) {
          console.error(`[Cleanup] Error deleting temp file:`, cleanupError.message);
        }
      }

      return { 
        message: `Chunk ${chunkIndex} uploaded successfully`,
        url: uploadResult.url,
        size: chunkSize
      };

    } catch (error) {
      console.error(`[ChunkQueue] Error uploading chunk ${chunkIndex}:`, error.message);
      
      if (error.message.includes('No eligible Dropbox accounts available')) {
        throw error;
      }

      if (error.message.includes('User went offline')) {
        // Don't treat offline as a hard error, just delay processing  
        throw error;
      }

      await fileProcessor.handleChunkError(fileId, chunkIndex, error, socketId);
      throw error;
    }
  });

  // Enhanced wait queue processor
  dropboxWaitQueue.process('wait-for-account', 3, async (job) => {
    const chunkData = job.data;
    const { waitId, startTime, chunkSize, chunkIndex, fileId, socketId } = chunkData;
    
    console.log(`[WaitQueue] Processing waiting chunk ${chunkIndex} (${waitId})`);
    
    if (Date.now() - startTime > DROPBOX_WAIT_TIMEOUT) {
      throw new Error(`Wait timeout exceeded for chunk ${chunkIndex} (${waitId})`);
    }

    // Check if user is still online
    if (socketId && !sessionManager.isUserOnline(socketId)) {
      console.log(`[WaitQueue] User ${socketId} went offline, marking chunk as offline`);
      chunkData.isOffline = true;
      chunkData.offlineAt = Date.now();
    }

    const eligibleAccounts = await dropboxManager.getEligibleAccounts(chunkSize);
    
    if (eligibleAccounts.length > 0) {
      console.log(`[WaitQueue] Account available for waiting chunk ${chunkIndex} (${waitId}), requeuing for upload`);
      
      // Calculate priority for requeued chunk
      const priority = await priorityManager.calculateJobPriority(chunkData);
      
      await chunkUploadQueue.add('upload-chunk', chunkData, {
        priority: priority,
        attempts: RETRY_ATTEMPTS
      });
      
      return { message: 'Chunk requeued for upload', waitId, chunkIndex, priority };
    } else {
      console.log(`[WaitQueue] Still no eligible accounts for chunk ${chunkIndex} (${waitId})`);
      throw new Error(`Still no eligible accounts for chunk ${chunkIndex}`);
    }
  });

  // Process waiting chunks when accounts become available
  dropboxWaitQueue.process('process-waiting-chunks', 1, async (job) => {
    console.log('[WaitQueue] Processing waiting chunks after account became available');
    
    try {
      const waitingJobs = await dropboxWaitQueue.getJobs(['waiting', 'delayed']);
      console.log(`[WaitQueue] Found ${waitingJobs.length} waiting jobs`);
      
      let processed = 0;
      for (const waitingJob of waitingJobs.slice(0, 5)) {
        try {
          const chunkData = waitingJob.data;
          if (chunkData.chunkIndex !== undefined) {
            const eligibleAccounts = await dropboxManager.getEligibleAccounts(chunkData.chunkSize);
            if (eligibleAccounts.length > 0) {
              const priority = await priorityManager.calculateJobPriority(chunkData);
              await chunkUploadQueue.add('upload-chunk', chunkData, {
                priority: priority,
                attempts: RETRY_ATTEMPTS
              });
              await waitingJob.remove();
              console.log(`[WaitQueue] Moved chunk ${chunkData.chunkIndex} to upload queue with priority ${priority}`);
              processed++;
            }
          }
        } catch (error) {
          console.error('[WaitQueue] Error processing waiting chunk:', error.message);
        }
      }
      
      return { processed, total: waitingJobs.length };
    } catch (error) {
      console.error('[WaitQueue] Error processing waiting chunks:', error.message);
      throw error;
    }
  });

  // Priority management queue processor
  priorityManagementQueue.process('rebalance-priorities', 1, async (job) => {
    console.log('[PriorityQueue] Starting priority rebalancing');
    await priorityManager.rebalanceQueue();
    return { message: 'Priority rebalancing completed' };
  });

  // Offline detection queue processor
  offlineDetectionQueue.process('detect-offline-users', 1, async (job) => {
    console.log('[OfflineQueue] Starting offline user detection');
    
    const now = Date.now();
    let detectedOfflineUsers = 0;
    
    for (const [socketId, lastHeartbeat] of sessionManager.socketHeartbeats) {
      if (now - lastHeartbeat > OFFLINE_DETECTION_TIMEOUT) {
        await sessionManager.handleUserOffline(socketId);
        detectedOfflineUsers++;
      }
    }
    
    return { detectedOfflineUsers, timestamp: now };
  });

  // Schedule periodic jobs
  setInterval(async () => {
    await priorityManagementQueue.add('rebalance-priorities', {}, {
      priority: QUEUE_PRIORITIES.NORMAL,
      attempts: 1
    });
  }, QUEUE_REBALANCE_INTERVAL);

  setInterval(async () => {
    await offlineDetectionQueue.add('detect-offline-users', {}, {
      priority: QUEUE_PRIORITIES.HIGH,
      attempts: 1
    });
  }, HEARTBEAT_INTERVAL);

  // =============================================================================
  // DATABASE OPERATIONS WITH CONNECTION ROTATION
  // =============================================================================
  async function ensureChunksTables(client) {
    try {
      await client.query(`
        CREATE TABLE IF NOT EXISTS message_chunks (
          id SERIAL PRIMARY KEY,
          room_name VARCHAR(255) NOT NULL,
          message_id UUID NOT NULL,
          chunk_index INT NOT NULL,
          total_chunks INT NOT NULL,
          content TEXT NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          CONSTRAINT unique_message_chunk UNIQUE (message_id, chunk_index)
        );
        
        CREATE TABLE IF NOT EXISTS file_chunks (
          id SERIAL PRIMARY KEY,
          room_name VARCHAR(255) NOT NULL,
          file_id UUID NOT NULL,
          file_name VARCHAR(255) NOT NULL,
          chunk_index INT NOT NULL,
          total_chunks INT NOT NULL,
          checksum TEXT NOT NULL,
          filechunk_url TEXT NOT NULL,
          chunk_size BIGINT DEFAULT 0,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          CONSTRAINT unique_file_chunk UNIQUE (file_id, chunk_index)
        );
        
        CREATE TABLE IF NOT EXISTS rooms (
          id SERIAL PRIMARY KEY,
          room_name VARCHAR(255) UNIQUE NOT NULL,
          room_password VARCHAR(255) NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_file_chunks_room_file ON file_chunks(room_name, file_id);
        CREATE INDEX IF NOT EXISTS idx_message_chunks_room_msg ON message_chunks(room_name, message_id);
        CREATE INDEX IF NOT EXISTS idx_chunks_created_at ON file_chunks(created_at);
        CREATE INDEX IF NOT EXISTS idx_file_chunks_room_created ON file_chunks(room_name, created_at DESC);
      `);

      const checkResult = await client.query(`
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'file_chunks' AND column_name = 'chunk_size'
      `);
      
      if (checkResult.rows.length === 0) {
        console.log('[DB] Adding chunk_size column to file_chunks table');
        await client.query('ALTER TABLE file_chunks ADD COLUMN chunk_size BIGINT DEFAULT 0');
      }
    } catch (error) {
      console.error('[DB] Error ensuring tables exist:', error.message);
      throw error;
    }
  }

  async function getDatabaseSizeMB(client) {
    try {
      const result = await client.query("SELECT pg_database_size(current_database()) AS size");
      return result.rows[0].size / (1024 * 1024);
    } catch (error) {
      console.error('[DB] Error getting database size:', error.message);
      return 0;
    }
  }

  async function storeChunkMetadata(chunkData) {
    console.log(`[DB] Storing metadata for chunk ${chunkData.chunkIndex} of file ${chunkData.fileId}`);
    
    try {
      const { record, pool, freeSpace } = await getBestNeonConnection();
      console.log(`[DB] Selected connection ${record.id} with ${freeSpace.toFixed(2)}MB free`);
      
      const client = await pool.connect();

      try {
        await ensureChunksTables(client);
        const result = await client.query(`
          INSERT INTO file_chunks (
            room_name, file_id, file_name, chunk_index, total_chunks, 
            checksum, filechunk_url, chunk_size, created_at
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
          ON CONFLICT (file_id, chunk_index) DO UPDATE SET
            filechunk_url = EXCLUDED.filechunk_url,
            chunk_size = EXCLUDED.chunk_size,
            created_at = NOW()
          RETURNING id
        `, [
          chunkData.roomName,
          chunkData.fileId,
          chunkData.fileName,
          chunkData.chunkIndex,
          chunkData.totalChunks,
          chunkData.checksum,
          chunkData.url,
          chunkData.size
        ]);

        console.log(`[DB] Stored metadata for chunk ${chunkData.chunkIndex} in DB ${record.id} (record ID: ${result.rows[0].id})`);

      } finally {
        client.release();
      }
    } catch (error) {
      console.error(`[DB] Error storing chunk metadata:`, error.message);
      throw error;
    }
  }

  // =============================================================================
  // MESSAGE PROCESSING (SIMPLIFIED)
  // =============================================================================
  class MessageProcessor {
    static async processMessage(message, roomName, userId) {
      const messageId = uuidv4();
      const chunks = this.splitMessage(message, MESSAGE_CHUNK_SIZE);
      console.log(`[Message] Processing message ${messageId} with ${chunks.length} chunks`);

      try {
        const connections = await getOptimizedConnections();
        const tasks = chunks.map((chunk, index) => 
          this.storeMessageChunk({
            messageId,
            roomName,
            chunkIndex: index,
            totalChunks: chunks.length,
            content: chunk,
            connections: connections.records
          })
        );

        await Promise.all(tasks);
        
        pusher.trigger(roomName, "new-content", {
          type: "message",
          messageId,
          preview: message.substring(0, 100) + (message.length > 100 ? '...' : ''),
          message: "New message posted",
          timestamp: new Date().toISOString()
        });

        console.log(`[Message] Message ${messageId} processed successfully`);
        return { messageId, chunks: chunks.length };
      } catch (error) {
        console.error(`[Message] Error processing message ${messageId}:`, error.message);
        throw error;
      }
    }

    static splitMessage(message, chunkSize) {
      const chunks = [];
      for (let i = 0; i < message.length; i += chunkSize) {
        chunks.push(message.substring(i, i + chunkSize));
      }
      return chunks;
    }

    static async storeMessageChunk({ messageId, roomName, chunkIndex, totalChunks, content, connections }) {
      const selectedConnection = connections[chunkIndex % connections.length];
      const pool = getConnectionPool(selectedConnection);
      const client = await pool.connect();

      try {
        await ensureChunksTables(client);
        await client.query(`
          INSERT INTO message_chunks (room_name, message_id, chunk_index, total_chunks, content)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (message_id, chunk_index) DO NOTHING
        `, [roomName, messageId, chunkIndex, totalChunks, content]);
      } finally {
        client.release();
      }
    }
  }

  // =============================================================================
  // CONTENT RETRIEVAL (OPTIMIZED)
  // =============================================================================
  class ContentRetriever {
    static async getRoomContent(roomName, limit = 50, offset = 0) {
      const cacheKey = `room_content:${roomName}:${limit}:${offset}`;
      
      try {
        const cached = await redis.get(cacheKey);
        if (cached) {
          console.log(`[Cache] Using cached content for room ${roomName}`);
          return JSON.parse(cached);
        }

        const connections = await getOptimizedConnections();
        const [messages, files] = await Promise.all([
          this.getMessages(roomName, connections.records, limit, offset),
          this.getFiles(roomName, connections.records, limit, offset)
        ]);

        const content = [...messages, ...files]
          .sort((a, b) => new Date(b.created_at) - new Date(a.created_at))
          .slice(0, limit);

        const result = { content, total: content.length };
        await redis.setex(cacheKey, 60, JSON.stringify(result));
        return result;
      } catch (error) {
        console.error(`[Content] Error retrieving room content:`, error.message);
        throw error;
      }
    }

    static async getMessages(roomName, connections, limit, offset) {
      const messageResults = await Promise.all(
        connections.map(async (record) => {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            await ensureChunksTables(client);
            const result = await client.query(`
              SELECT message_id, chunk_index, total_chunks, content, created_at
              FROM message_chunks 
              WHERE room_name = $1
              ORDER BY created_at DESC
              LIMIT $2 OFFSET $3
            `, [roomName, limit * 2, offset]);
            return result.rows;
          } catch (error) {
            console.warn(`[Content] Error fetching messages from DB ${record.id}:`, error.message);
            return [];
          } finally {
            client.release();
          }
        })
      );

      const allChunks = messageResults.flat();
      const messageMap = new Map();
      
      allChunks.forEach(chunk => {
        if (!messageMap.has(chunk.message_id)) {
          messageMap.set(chunk.message_id, {
            chunks: [],
            created_at: chunk.created_at
          });
        }
        messageMap.get(chunk.message_id).chunks.push(chunk);
      });

      const messages = [];
      for (const [messageId, data] of messageMap) {
        const expectedChunks = data.chunks[0]?.total_chunks || 0;
        if (data.chunks.length === expectedChunks) {
          data.chunks.sort((a, b) => a.chunk_index - b.chunk_index);
          const fullMessage = data.chunks.map(c => c.content).join('');
          messages.push({
            type: 'message',
            message_id: messageId,
            message: fullMessage,
            created_at: data.created_at,
            chunks: data.chunks.length
          });
        }
      }

      return messages.slice(0, limit);
    }

    static async getFiles(roomName, connections, limit, offset) {
      const fileResults = await Promise.all(
        connections.map(async (record) => {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            await ensureChunksTables(client);
            const result = await client.query(`
              SELECT DISTINCT file_id, file_name, 
                     MIN(created_at) as created_at,
                     COUNT(*) as available_chunks,
                     MAX(total_chunks) as total_chunks,
                     SUM(chunk_size) as file_size
              FROM file_chunks 
              WHERE room_name = $1
              GROUP BY file_id, file_name
              ORDER BY created_at DESC
              LIMIT $2 OFFSET $3
            `, [roomName, limit, offset]);
            return result.rows;
          } catch (error) {
            console.warn(`[Content] Error fetching files from DB ${record.id}:`, error.message);
            return [];
          } finally {
            client.release();
          }
        })
      );

      const allFiles = fileResults.flat();
      const uniqueFiles = new Map();
      
      allFiles.forEach(file => {
        const existingFile = uniqueFiles.get(file.file_id);
        if (!existingFile || new Date(file.created_at) < new Date(existingFile.created_at)) {
          uniqueFiles.set(file.file_id, {
            ...file,
            isComplete: parseInt(file.available_chunks) === parseInt(file.total_chunks)
          });
        }
      });

      return Array.from(uniqueFiles.values()).map(file => ({
        type: 'file',
        file_id: file.file_id,
        file_name: file.file_name,
        created_at: file.created_at,
        file_size: parseInt(file.file_size || 0),
        available_chunks: parseInt(file.available_chunks),
        total_chunks: parseInt(file.total_chunks),
        is_complete: file.isComplete,
        progress: file.isComplete ? 100 : (parseInt(file.available_chunks) / parseInt(file.total_chunks)) * 100
      })).slice(0, limit);
    }
  }

  // =============================================================================
  // FILE DOWNLOAD (SIMPLIFIED)
  // =============================================================================
  class FileDownloader {
    static async streamFileChunk(roomName, fileId, chunkIndex, res) {
      try {
        console.log(`[Download] Streaming chunk ${chunkIndex} for file ${fileId}`);
        
        const connections = await getOptimizedConnections();
        let chunkUrl = null;
        let chunkSize = 0;

        for (const record of connections.records) {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            const result = await client.query(`
              SELECT filechunk_url, chunk_size FROM file_chunks 
              WHERE room_name = $1 AND file_id = $2 AND chunk_index = $3
              LIMIT 1
            `, [roomName, fileId, parseInt(chunkIndex)]);
            
            if (result.rows.length > 0) {
              chunkUrl = result.rows[0].filechunk_url;
              chunkSize = parseInt(result.rows[0].chunk_size || 0);
              break;
            }
          } finally {
            client.release();
          }
        }

        if (!chunkUrl) {
          return res.status(404).json({ error: 'Chunk not found' });
        }

        console.log(`[Download] Fetching chunk from Dropbox: ${chunkUrl}`);
        const dropboxResponse = await fetch(chunkUrl, {
          timeout: UPLOAD_TIMEOUT
        });
        
        if (!dropboxResponse.ok) {
          throw new Error(`Failed to fetch from Dropbox: ${dropboxResponse.status}`);
        }

        const totalSize = parseInt(dropboxResponse.headers.get('content-length') || chunkSize.toString());
        
        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader('Content-Length', totalSize);
        res.setHeader('Cache-Control', 'public, max-age=3600');
        res.setHeader('Access-Control-Allow-Origin', '*');

        await pipeline(dropboxResponse.body, res);
        console.log(`[Download] Chunk ${chunkIndex} streamed successfully`);

      } catch (error) {
        console.error(`[Download] Error streaming chunk ${chunkIndex}:`, error.message);
        if (!res.headersSent) {
          res.status(500).json({ error: 'Download failed', details: error.message });
        }
      }
    }

    static async getFileMetadata(roomName, fileId) {
      const cacheKey = `file_metadata:${fileId}`;
      let cached = fileMetadataCache.get(cacheKey);
      if (cached) {
        return cached;
      }

      try {
        const connections = await getOptimizedConnections();
        let metadata = null;

        for (const record of connections.records) {
          const pool = getConnectionPool(record);
          const client = await pool.connect();
          try {
            const result = await client.query(`
              SELECT COUNT(*) as available_chunks, 
                     MAX(total_chunks) as expected_chunks,
                     MAX(chunk_size) as chunk_size,
                     SUM(chunk_size) as total_size,
                     file_name,
                     MIN(created_at) as created_at,
                     MAX(created_at) as last_updated
              FROM file_chunks 
              WHERE room_name = $1 AND file_id = $2
              GROUP BY file_name
            `, [roomName, fileId]);
            
            if (result.rows.length > 0) {
              const row = result.rows[0];
              metadata = {
                totalChunks: parseInt(row.expected_chunks),
                availableChunks: parseInt(row.available_chunks),
                chunkSize: parseInt(row.chunk_size || 0),
                totalSize: parseInt(row.total_size || 0),
                fileName: row.file_name,
                isComplete: parseInt(row.available_chunks) === parseInt(row.expected_chunks),
                createdAt: row.created_at,
                lastUpdated: row.last_updated,
                downloadProgress: (parseInt(row.available_chunks) / parseInt(row.expected_chunks)) * 100
              };
              break;
            }
          } finally {
            client.release();
          }
        }

        if (metadata) {
          fileMetadataCache.set(cacheKey, metadata);
        }

        return metadata;
      } catch (error) {
        console.error(`[Download] Error getting file metadata:`, error.message);
        return null;
      }
    }
  }

  // =============================================================================
  // ENHANCED API ENDPOINTS WITH QUEUE MANAGEMENT
  // =============================================================================

  app.get('/health', async (req, res) => {
    try {
      const redisHealth = redis.status === 'ready';
      const queueHealths = await Promise.all(allQueues.map(queue => queue.isReady()));
      
      const queueStats = await Promise.all(allQueues.map(queue => 
        queue.getJobCounts().catch(() => ({ waiting: 0, active: 0, completed: 0, failed: 0 }))
      ));
      
      res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        worker: process.pid,
        redis: redisHealth,
        queues: {
          fileProcessing: queueHealths[0],
          chunkUpload: queueHealths[1],
          dropboxWait: queueHealths[2],
          priorityManagement: queueHealths[3],
          offlineDetection: queueHealths[4]
        },
        stats: {
          fileQueue: queueStats[0],
          chunkQueue: queueStats[1],
          waitQueue: queueStats[2],
          priorityQueue: queueStats[3],
          offlineQueue: queueStats[4]
        },
        sessionStats: {
          onlineUsers: sessionManager.socketHeartbeats.size,
          offlineFiles: sessionManager.offlineFiles.size,
          activeRooms: sessionManager.roomSessions.size
        },
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        version: '4.0.0-enhanced'
      });
    } catch (error) {
      res.status(503).json({
        status: 'unhealthy',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  app.post('/create-room', async (req, res) => {
    const { roomName, roomPassword } = req.body;
    
    if (!roomName || !roomPassword) {
      return res.status(400).json({ error: 'Missing roomName or roomPassword' });
    }

    if (roomName.length > 100 || roomPassword.length > 100) {
      return res.status(400).json({ error: 'Room name or password too long' });
    }

    try {
      console.log(`[Room] Creating room "${roomName}"`);
      const { record, pool, freeSpace } = await getBestNeonConnection();
      console.log(`[DB] Selected connection ${record.id} with ${freeSpace.toFixed(2)}MB free`);
      
      const client = await pool.connect();
      try {
        await ensureChunksTables(client);
        const result = await client.query(
          'SELECT room_name FROM rooms WHERE room_name = $1',
          [roomName]
        );
        if (result.rows.length > 0) {
          return res.status(400).json({ error: 'Room already exists' });
        }

        await client.query(
          'INSERT INTO rooms (room_name, room_password) VALUES ($1, $2)',
          [roomName, roomPassword]
        );
        console.log(`[Room] Created room "${roomName}" in DB ${record.id}`);
        res.json({ message: 'Room created successfully', roomName });
      } finally {
        client.release();
      }

    } catch (error) {
      console.error('[Room] Error creating room:', error.message);
      res.status(500).json({ error: 'Failed to create room', details: error.message });
    }
  });

  app.post('/join-room', async (req, res) => {
    const { roomName, roomPassword } = req.body;
    
    if (!roomName || !roomPassword) {
      return res.status(400).json({ error: 'Missing roomName or roomPassword' });
    }

    try {
      console.log(`[Room] Attempting to join room "${roomName}"`);
      const connections = await getOptimizedConnections();
      let roomFound = false;
      
      for (const record of connections.records) {
        const pool = getConnectionPool(record);
        const client = await pool.connect();
        try {
          await ensureChunksTables(client);
          const result = await client.query(
            'SELECT room_password FROM rooms WHERE room_name = $1',
            [roomName]
          );
          if (result.rows.length > 0 && result.rows[0].room_password === roomPassword) {
            roomFound = true;
            break;
          }
        } finally {
          client.release();
        }
      }

      if (roomFound) {
        console.log(`[Room] Successfully joined room "${roomName}"`);
        res.json({ message: 'Room joined successfully', roomName });
      } else {
        console.warn(`[Room] Failed to join room "${roomName}" - invalid credentials`);
        res.status(400).json({ error: 'Invalid room name or password' });
      }

    } catch (error) {
      console.error('[Room] Error joining room:', error.message);
      res.status(500).json({ error: 'Failed to join room', details: error.message });
    }
  });

  app.post('/room/:roomName/post-message', async (req, res) => {
    const { roomName } = req.params;
    const { message, userId } = req.body;
    
    if (!message) {
      return res.status(400).json({ error: 'Missing message' });
    }

    if (message.length > 1000000) {
      return res.status(400).json({ error: 'Message too long' });
    }

    try {
      console.log(`[Message] Posting message to room "${roomName}" (${message.length} characters)`);
      const result = await MessageProcessor.processMessage(message, roomName, userId || 'guest');
      res.json({ 
        message: 'Message posted successfully', 
        messageId: result.messageId,
        chunks: result.chunks
      });
    } catch (error) {
      console.error('[Message] Error posting message:', error.message);
      res.status(500).json({ error: 'Failed to post message', details: error.message });
    }
  });

  app.post('/room/:roomName/upload-file', upload.single('file'), async (req, res) => {
    const { roomName } = req.params;
    const file = req.file;
    const userId = req.body.userId || 'guest';
    const socketId = req.body.socketId;

    console.log(`[Upload] File upload request for room "${roomName}"`);
    console.log(`[Upload] File info:`, file ? {
      originalname: file.originalname,
      size: file.size,
      mimetype: file.mimetype,
      path: file.path
    } : 'No file');

    if (!file) {
      return res.status(400).json({ error: 'No file uploaded' });
    }

    if (!fs.existsSync(file.path)) {
      return res.status(400).json({ error: 'File upload failed - file not found' });
    }

    const stats = fs.statSync(file.path);
    if (stats.size === 0) {
      try {
        fs.unlinkSync(file.path);
      } catch (e) {}
      return res.status(400).json({ error: 'Cannot upload empty file' });
    }

    if (stats.size > 100 * 1024 * 1024 * 1024) {
      try {
        fs.unlinkSync(file.path);
      } catch (e) {}
      return res.status(400).json({ error: 'File too large (max 100GB)' });
    }

    try {
      console.log(`[Upload] Processing file: ${file.originalname} (${(stats.size / (1024*1024)).toFixed(2)}MB)`);
      
      const result = await fileProcessor.processFile(
        file.path,
        file.originalname,
        roomName,
        userId,
        socketId
      );
      
      console.log(`[Upload] File processing initiated successfully: ${result.fileId}`);
      res.json({
        ...result,
        fileName: file.originalname,
        fileSize: stats.size,
        fileSizeMB: (stats.size / (1024*1024)).toFixed(2)
      });
    } catch (error) {
      console.error('[Upload] Error uploading file:', error.message);
      
      try {
        if (fs.existsSync(file.path)) {
          fs.unlinkSync(file.path);
          console.log(`[Cleanup] Deleted file on upload error: ${file.path}`);
        }
      } catch (cleanupError) {
        console.error('[Cleanup] Error deleting file on upload error:', cleanupError.message);
      }
      
      res.status(500).json({ 
        error: 'Failed to upload file', 
        details: error.message,
        fileName: file.originalname
      });
    }
  });

  app.get('/room/:roomName/get-file-chunk/:fileId', async (req, res) => {
    const { roomName, fileId } = req.params;
    const chunkIndex = req.query.index;
    
    if (chunkIndex == null) {
      return res.status(400).json({ error: 'Missing chunk index' });
    }
    
    await FileDownloader.streamFileChunk(roomName, fileId, chunkIndex, res);
  });

  app.get('/room/:roomName/get-file-metadata/:fileId', async (req, res) => {
    const { roomName, fileId } = req.params;
    
    try {
      const metadata = await FileDownloader.getFileMetadata(roomName, fileId);
      if (!metadata) {
        return res.status(404).json({ error: 'File not found' });
      }
      res.json(metadata);
    } catch (error) {
      console.error('[Metadata] Error retrieving file metadata:', error.message);
      res.status(500).json({ error: 'Failed to retrieve file metadata', details: error.message });
    }
  });

  app.get('/room/:roomName/get-content', async (req, res) => {
    const { roomName } = req.params;
    const limit = Math.min(parseInt(req.query.limit) || 50, 100);
    const offset = Math.max(parseInt(req.query.offset) || 0, 0);
    
    try {
      const content = await ContentRetriever.getRoomContent(roomName, limit, offset);
      res.json(content);
    } catch (error) {
      console.error('[Content] Error retrieving room content:', error.message);
      res.status(500).json({ error: 'Failed to retrieve room content', details: error.message });
    }
  });

  app.get('/upload-progress/:fileId', async (req, res) => {
    const { fileId } = req.params;
    
    try {
      const progressData = await redis.get(`progress:${fileId}`);
      if (progressData) {
        res.json(JSON.parse(progressData));
      } else {
        res.status(404).json({ error: 'Progress not found' });
      }
    } catch (error) {
      console.error('[Progress] Error retrieving upload progress:', error.message);
      res.status(500).json({ error: 'Failed to retrieve progress', details: error.message });
    }
  });

  // =============================================================================
  // ENHANCED ADMIN ENDPOINTS FOR QUEUE MANAGEMENT
  // =============================================================================

  app.get('/admin/queue-stats', async (req, res) => {
    try {
      const queueStats = await Promise.all(allQueues.map(queue => queue.getJobCounts()));
      const queueNames = ['fileProcessing', 'chunkUpload', 'dropboxWait', 'priorityManagement', 'offlineDetection'];

      const activeJobs = await Promise.all([
        chunkUploadQueue.getJobs(['active']),
        dropboxWaitQueue.getJobs(['waiting', 'delayed']),
        chunkUploadQueue.getJobs(['waiting'])
      ]);

      // Get priority distribution
      const waitingJobs = activeJobs[2];
      const priorityDistribution = {
        critical: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.CRITICAL).length,
        high: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.HIGH && job.opts.priority < QUEUE_PRIORITIES.CRITICAL).length,
        normal: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.NORMAL && job.opts.priority < QUEUE_PRIORITIES.HIGH).length,
        low: waitingJobs.filter(job => job.opts.priority >= QUEUE_PRIORITIES.LOW && job.opts.priority < QUEUE_PRIORITIES.NORMAL).length,
        offline: waitingJobs.filter(job => job.opts.priority < QUEUE_PRIORITIES.LOW).length
      };

      const stats = {};
      queueNames.forEach((name, index) => {
        stats[name] = queueStats[index];
      });

      res.json({
        ...stats,
        activeUploads: activeJobs[0].length,
        waitingForDropbox: activeJobs[1].length,
        priorityDistribution,
        offlineFiles: sessionManager.offlineFiles.size,
        onlineUsers: sessionManager.socketHeartbeats.size,
        activeRooms: sessionManager.roomSessions.size,
        timestamp: new Date().toISOString(),
        systemLoad: os.loadavg(),
        memoryUsage: process.memoryUsage(),
        connectionPools: connectionPools.size
      });
    } catch (error) {
      console.error('[Admin] Error retrieving queue stats:', error.message);
      res.status(500).json({ error: 'Failed to retrieve queue stats', details: error.message });
    }
  });

  app.post('/admin/queue-action', async (req, res) => {
    const { action, queue } = req.body;
    
    if (!action || !queue) {
      return res.status(400).json({ error: 'Missing action or queue parameter' });
    }

    try {
      let targetQueue;
      switch (queue) {
        case 'file': targetQueue = fileProcessingQueue; break;
        case 'chunk': targetQueue = chunkUploadQueue; break;
        case 'wait': targetQueue = dropboxWaitQueue; break;
        case 'priority': targetQueue = priorityManagementQueue; break;
        case 'offline': targetQueue = offlineDetectionQueue; break;
        default: return res.status(400).json({ error: 'Invalid queue name' });
      }

      switch (action) {
        case 'pause': 
          await targetQueue.pause(); 
          console.log(`[Admin] Paused ${queue} queue`);
          break;
        case 'resume': 
          await targetQueue.resume(); 
          console.log(`[Admin] Resumed ${queue} queue`);
          break;
        case 'clean':
          await targetQueue.clean(5000, 'completed');
          await targetQueue.clean(10000, 'failed');
          console.log(`[Admin] Cleaned ${queue} queue`);
          break;
        default: return res.status(400).json({ error: 'Invalid action' });
      }

      res.json({ message: `Action ${action} executed on ${queue} queue` });
    } catch (error) {
      console.error('[Admin] Error executing queue action:', error.message);
      res.status(500).json({ error: 'Failed to execute queue action', details: error.message });
    }
  });

  app.post('/admin/clear-all-queues', async (req, res) => {
    const { confirm } = req.body;
    
    if (confirm !== 'yes-clear-all-queues') {
      return res.status(400).json({ 
        error: 'Confirmation required',
        message: 'Send { "confirm": "yes-clear-all-queues" } to confirm queue clearing'
      });
    }

    try {
      console.log('[Admin] CLEARING ALL QUEUES - This will remove all pending jobs');
      
      const results = {};
      
      for (let i = 0; i < allQueues.length; i++) {
        const queue = allQueues[i];
        const queueNames = ['fileProcessing', 'chunkUpload', 'dropboxWait', 'priorityManagement', 'offlineDetection'];
        const queueName = queueNames[i];
        
        try {
          // Get job counts before clearing
          const beforeStats = await queue.getJobCounts();
          
          // Remove all jobs
          await queue.empty();
          await queue.clean(0, 'completed');
          await queue.clean(0, 'failed');
          await queue.clean(0, 'active');
          await queue.clean(0, 'waiting');
          await queue.clean(0, 'delayed');
          
          // Get job counts after clearing
          const afterStats = await queue.getJobCounts();
          
          results[queueName] = {
            before: beforeStats,
            after: afterStats,
            cleared: beforeStats.waiting + beforeStats.active + beforeStats.delayed
          };
          
          console.log(`[Admin] Cleared ${queueName} queue: ${results[queueName].cleared} jobs removed`);
        } catch (error) {
          results[queueName] = { error: error.message };
          console.error(`[Admin] Error clearing ${queueName} queue:`, error.message);
        }
      }
      
      // Clear session manager offline files
      const offlineFilesCleared = sessionManager.offlineFiles.size;
      sessionManager.offlineFiles.clear();
      
      // Clear Redis progress data (be careful with this)
      const progressKeys = await redis.keys('progress:*');
      const lockKeys = await redis.keys('file_lock:*');
      const offlineKeys = await redis.keys('offline_file:*');
      const waitTimeKeys = await redis.keys('file_wait_time:*');
      
      const allKeysToDelete = [...progressKeys, ...lockKeys, ...offlineKeys, ...waitTimeKeys];
      if (allKeysToDelete.length > 0) {
        await redis.del(...allKeysToDelete);
      }
      
      res.json({
        message: 'All queues and related data cleared successfully',
        results,
        offlineFilesCleared,
        redisKeysCleared: allKeysToDelete.length,
        timestamp: new Date().toISOString(),
        warning: 'All upload progress has been lost. Users will need to restart their uploads.'
      });
      
      // Notify all connected clients
      io.emit('system-notification', {
        type: 'queues-cleared',
        message: 'All upload queues have been cleared by administrator. Please restart any pending uploads.',
        timestamp: new Date().toISOString()
      });
      
    } catch (error) {
      console.error('[Admin] Error clearing all queues:', error.message);
      res.status(500).json({ error: 'Failed to clear all queues', details: error.message });
    }
  });

  app.get('/admin/dropbox-accounts', async (req, res) => {
    try {
      const accounts = await dropboxManager.fetchDropboxAccounts();
      const accountsWithStatus = await Promise.all(
        accounts.map(async (account) => {
          const isLocked = await dropboxManager.isAccountLocked(account.id);
          const used = parseFloat(account.fields.storage || "0");
          const free = DROPBOX_ACCOUNT_MAX_CAPACITY - used;
          return {
            id: account.id,
            name: account.fields.name || 'Unnamed',
            storageUsed: used,
            storageFree: free,
            storageUsedMB: (used / (1024 * 1024)).toFixed(2),
            storageFreeMB: (free / (1024 * 1024)).toFixed(2),
            storageUsedPercent: ((used / DROPBOX_ACCOUNT_MAX_CAPACITY) * 100).toFixed(1),
            isLocked,
            isEligible: free >= MIN_CHUNK_SIZE && !isLocked && used < DROPBOX_ROTATION_THRESHOLD
          };
        })
      );

      res.json({
        total: accountsWithStatus.length,
        eligible: accountsWithStatus.filter(acc => acc.isEligible).length,
        locked: accountsWithStatus.filter(acc => acc.isLocked).length,
        nearCapacity: accountsWithStatus.filter(acc => acc.storageUsed > DROPBOX_ROTATION_THRESHOLD).length,
        accounts: accountsWithStatus
      });
    } catch (error) {
      console.error('[Admin] Error retrieving Dropbox accounts status:', error.message);
      res.status(500).json({ error: 'Failed to retrieve Dropbox accounts status', details: error.message });
    }
  });

  app.get('/admin/offline-files', async (req, res) => {
    try {
      const offlineFilesArray = Array.from(sessionManager.offlineFiles.values());
      const offlineStats = {
        totalOfflineFiles: offlineFilesArray.length,
        filesByRoom: {},
        filesByAge: {
          lessThan5Min: 0,
          lessThan30Min: 0,
          lessThan1Hour: 0,
          moreThan1Hour: 0
        }
      };

      const now = Date.now();
      offlineFilesArray.forEach(file => {
        // Group by room
        if (!offlineStats.filesByRoom[file.roomName]) {
          offlineStats.filesByRoom[file.roomName] = 0;
        }
        offlineStats.filesByRoom[file.roomName]++;

        // Group by age
        const ageMinutes = (now - file.offlineAt) / (1000 * 60);
        if (ageMinutes < 5) offlineStats.filesByAge.lessThan5Min++;
        else if (ageMinutes < 30) offlineStats.filesByAge.lessThan30Min++;
        else if (ageMinutes < 60) offlineStats.filesByAge.lessThan1Hour++;
        else offlineStats.filesByAge.moreThan1Hour++;
      });

      res.json({
        stats: offlineStats,
        files: offlineFilesArray.map(file => ({
          fileId: file.fileId,
          fileName: file.progressData.fileName,
          roomName: file.roomName,
          progress: file.progressData.progress,
          offlineTime: now - file.offlineAt,
          offlineAt: new Date(file.offlineAt).toISOString(),
          fileSize: file.progressData.totalSize,
          uploadedChunks: file.progressData.uploadedChunks,
          totalChunks: file.progressData.totalChunks
        }))
      });
    } catch (error) {
      console.error('[Admin] Error retrieving offline files:', error.message);
      res.status(500).json({ error: 'Failed to retrieve offline files', details: error.message });
    }
  });

  app.post('/admin/cleanup-offline-files', async (req, res) => {
    const { maxAgeMinutes = 60, confirm } = req.body;
    
    if (confirm !== 'yes-cleanup-offline-files') {
      return res.status(400).json({ 
        error: 'Confirmation required',
        message: 'Send { "confirm": "yes-cleanup-offline-files", "maxAgeMinutes": 60 } to confirm cleanup'
      });
    }

    try {
      console.log(`[Admin] Cleaning up offline files older than ${maxAgeMinutes} minutes`);
      
      const now = Date.now();
      const maxAge = maxAgeMinutes * 60 * 1000;
      const filesToCleanup = [];

      for (const [fileId, data] of sessionManager.offlineFiles) {
        if (now - data.offlineAt > maxAge) {
          filesToCleanup.push({ fileId, ...data });
        }
      }

      let cleanedUp = 0;
      const errors = [];

      for (const fileData of filesToCleanup) {
        try {
          await sessionManager.deleteOfflineFile(fileData.fileId);
          cleanedUp++;
          console.log(`[Admin] Cleaned up offline file: ${fileData.fileId}`);
        } catch (error) {
          errors.push({ fileId: fileData.fileId, error: error.message });
          console.error(`[Admin] Error cleaning up file ${fileData.fileId}:`, error.message);
        }
      }

      res.json({
        message: `Cleanup completed`,
        totalFound: filesToCleanup.length,
        cleanedUp,
        errors: errors.length,
        errorDetails: errors,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      console.error('[Admin] Error during offline files cleanup:', error.message);
      res.status(500).json({ error: 'Failed to cleanup offline files', details: error.message });
    }
  });

  app.get('/admin/queue-analysis', async (req, res) => {
    try {
      const analysis = {
        timestamp: new Date().toISOString(),
        chunkUploadQueue: {
          jobs: {},
          priorities: {},
          fileDistribution: {},
          userDistribution: {},
          offlineJobs: 0
        }
      };

      // Analyze chunk upload queue in detail
      const allChunkJobs = await chunkUploadQueue.getJobs(['waiting', 'active', 'delayed', 'completed', 'failed']);
      
      ['waiting', 'active', 'delayed', 'completed', 'failed'].forEach(state => {
        analysis.chunkUploadQueue.jobs[state] = allChunkJobs.filter(job => 
          job.finishedOn ? (job.failedReason ? state === 'failed' : state === 'completed') :
          job.processedOn ? state === 'active' :
          job.delay && job.delay > Date.now() ? state === 'delayed' :
          state === 'waiting'
        ).length;
      });

      // Analyze priorities
      Object.values(QUEUE_PRIORITIES).forEach(priority => {
        analysis.chunkUploadQueue.priorities[priority] = allChunkJobs.filter(job => 
          job.opts.priority === priority && !job.finishedOn
        ).length;
      });

      // Analyze file distribution
      const fileChunkCounts = {};
      const userChunkCounts = {};
      
      allChunkJobs.filter(job => !job.finishedOn).forEach(job => {
        const fileId = job.data.fileId;
        const socketId = job.data.socketId;
        
        if (fileId) {
          fileChunkCounts[fileId] = (fileChunkCounts[fileId] || 0) + 1;
        }
        
        if (socketId) {
          userChunkCounts[socketId] = (userChunkCounts[socketId] || 0) + 1;
        }

        if (job.data.isOffline) {
          analysis.chunkUploadQueue.offlineJobs++;
        }
      });

      analysis.chunkUploadQueue.fileDistribution = {
        totalFiles: Object.keys(fileChunkCounts).length,
        avgChunksPerFile: Object.keys(fileChunkCounts).length > 0 ? 
          (Object.values(fileChunkCounts).reduce((a, b) => a + b, 0) / Object.keys(fileChunkCounts).length).toFixed(2) : 0,
        topFiles: Object.entries(fileChunkCounts)
          .sort(([,a], [,b]) => b - a)
          .slice(0, 10)
          .map(([fileId, count]) => ({ fileId: fileId.substring(0, 8), chunks: count }))
      };

      analysis.chunkUploadQueue.userDistribution = {
        totalUsers: Object.keys(userChunkCounts).length,
        avgChunksPerUser: Object.keys(userChunkCounts).length > 0 ?
          (Object.values(userChunkCounts).reduce((a, b) => a + b, 0) / Object.keys(userChunkCounts).length).toFixed(2) : 0,
        topUsers: Object.entries(userChunkCounts)
          .sort(([,a], [,b]) => b - a)
          .slice(0, 10)
          .map(([userId, count]) => ({ userId: userId.substring(0, 8), chunks: count }))
      };

      res.json(analysis);
    } catch (error) {
      console.error('[Admin] Error during queue analysis:', error.message);
      res.status(500).json({ error: 'Failed to analyze queues', details: error.message });
    }
  });

  app.get('/debug/file/:fileId', async (req, res) => {
    const { fileId } = req.params;
    
    try {
      const [progressData, lockData, offlineData] = await Promise.all([
        redis.get(`progress:${fileId}`),
        redis.get(`file_lock:${fileId}`),
        redis.get(`offline_file:${fileId}`)
      ]);

      const allJobs = await Promise.all([
        fileProcessingQueue.getJobs(['waiting', 'active', 'completed', 'failed']),
        chunkUploadQueue.getJobs(['waiting', 'active', 'completed', 'failed']),
        dropboxWaitQueue.getJobs(['waiting', 'active', 'completed', 'failed']),
        priorityManagementQueue.getJobs(['waiting', 'active', 'completed', 'failed']),
        offlineDetectionQueue.getJobs(['waiting', 'active', 'completed', 'failed'])
      ]);

      const fileRelatedJobs = {
        file: allJobs[0].filter(job => job.data.fileId === fileId),
        chunk: allJobs[1].filter(job => job.data.fileId === fileId),
        wait: allJobs[2].filter(job => job.data.fileId === fileId),
        priority: allJobs[3].filter(job => job.data.fileId === fileId),
        offline: allJobs[4].filter(job => job.data.fileId === fileId)
      };

      // Get session manager data
      const sessionData = {
        isOfflineTracked: sessionManager.offlineFiles.has(fileId),
        offlineFileData: sessionManager.offlineFiles.get(fileId) || null
      };

      // Get chunk job details with priorities
      const chunkJobDetails = fileRelatedJobs.chunk.map(job => ({
        id: job.id,
        chunkIndex: job.data.chunkIndex,
        priority: job.opts.priority,
        isOffline: job.data.isOffline,
        state: job.finishedOn ? (job.failedReason ? 'failed' : 'completed') :
               job.processedOn ? 'active' :
               job.delay && job.delay > Date.now() ? 'delayed' : 'waiting',
        attempts: job.attemptsMade,
        createdAt: new Date(job.timestamp).toISOString(),
        processedAt: job.processedOn ? new Date(job.processedOn).toISOString() : null,
        finishedAt: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
        error: job.failedReason || null
      }));

      res.json({
        fileId,
        progress: progressData ? JSON.parse(progressData) : null,
        lock: lockData,
        offline: offlineData ? JSON.parse(offlineData) : null,
        sessionData,
        jobs: fileRelatedJobs,
        chunkJobDetails,
        priorityLevels: QUEUE_PRIORITIES,
        timestamp: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({ error: 'Debug info retrieval failed', details: error.message });
    }
  });

  app.get('/', (req, res) => {
    res.json({
      message: 'Enhanced Professional Infinity Share API Server v4.0',
      version: '4.0.0-enhanced',
      worker: process.pid,
      status: 'running',
      features: [
        'Advanced Priority Queue Management',
        'Offline/Online Detection & Recovery',
        'Fair Share Scheduling Algorithm',
        'Dynamic Chunk Size Optimization',
        'Real-time Session Management',
        'Circuit Breaker Protection',
        'Enhanced Error Recovery',
        'Comprehensive Admin Controls',
        'Intelligent Dropbox Account Allocation',
        'WebSocket Real-time Updates',
        'Round-robin Fair Processing',
        'Automatic Priority Boosting',
        'Deadlock Prevention',
        'Neon DB Connection Rotation',
        'Dropbox Account Rotation',
        'Automatic Lock Management'
      ],
      enhancements: [
        'Session-based offline file detection',
        'Priority-based queue processing',
        'Fair share time slicing',
        'Automatic queue rebalancing',
        'Comprehensive admin controls',
        'Queue analysis tools',
        'Offline file recovery system',
        'Real-time priority adjustments',
        'Connection refresh every 1 minute',
        'DB rotation at 400MB threshold',
        'Dropbox rotation at 1.5GB threshold'
      ],
      queuePriorities: QUEUE_PRIORITIES,
      endpoints: {
        health: 'GET /health',
        createRoom: 'POST /create-room',
        joinRoom: 'POST /join-room',
        postMessage: 'POST /room/:roomName/post-message',
        uploadFile: 'POST /room/:roomName/upload-file',
        getContent: 'GET /room/:roomName/get-content',
        downloadChunk: 'GET /room/:roomName/get-file-chunk/:fileId?index=N',
        fileMetadata: 'GET /room/:roomName/get-file-metadata/:fileId',
        uploadProgress: 'GET /upload-progress/:fileId',
        
        // Admin endpoints
        queueStats: 'GET /admin/queue-stats',
        queueAction: 'POST /admin/queue-action',
        clearAllQueues: 'POST /admin/clear-all-queues',
        dropboxStatus: 'GET /admin/dropbox-accounts',
        offlineFiles: 'GET /admin/offline-files',
        cleanupOfflineFiles: 'POST /admin/cleanup-offline-files',
        queueAnalysis: 'GET /admin/queue-analysis',
        
        // Debug endpoints
        debugFile: 'GET /debug/file/:fileId'
      },
      socketEvents: {
        client: [
          'join-room',
          'heartbeat',
          'offline-file-decision',
          'request-upload-status',
          'cancel-upload',
          'get-queue-stats',
          'get-offline-files',
          'ping'
        ],
        server: [
          'room-joined',
          'heartbeat-ack',
          'offline-files-detected',
          'file-resumed',
          'file-deleted',
          'upload-progress',
          'chunk-progress',
          'file-upload-complete',
          'file-upload-failed',
          'upload-error',
          'queue-stats',
          'offline-files-list',
          'system-notification',
          'pong'
        ]
      }
    });
  });

  // Enhanced error handling middleware
  app.use((error, req, res, next) => {
    console.error('[Server] Unhandled error:', error.message);
    console.error('[Server] Error stack:', error.stack);
    
    if (error instanceof multer.MulterError) {
      if (error.code === 'LIMIT_FILE_SIZE') {
        return res.status(413).json({ 
          error: 'File too large', 
          maxSize: '100GB',
          code: 'FILE_TOO_LARGE'
        });
      }
      if (error.code === 'LIMIT_UNEXPECTED_FILE') {
        return res.status(400).json({ 
          error: 'Unexpected file field', 
          code: 'UNEXPECTED_FILE'
        });
      }
      return res.status(400).json({ 
        error: error.message, 
        code: error.code 
      });
    }
    
    if (error.name === 'ValidationError') {
      return res.status(400).json({ 
        error: 'Validation failed', 
        details: error.message,
        code: 'VALIDATION_ERROR'
      });
    }

    res.status(500).json({ 
      error: 'Internal server error',
      code: 'INTERNAL_ERROR',
      details: process.env.NODE_ENV === 'development' ? error.message : 'An unexpected error occurred'
    });
  });

  // Handle 404s
  app.use((req, res) => {
    res.status(404).json({ 
      error: 'Endpoint not found',
      path: req.path,
      method: req.method,
      code: 'NOT_FOUND'
    });
  });

  // =============================================================================
  // ENHANCED GRACEFUL SHUTDOWN
  // =============================================================================
  process.on('SIGTERM', gracefulShutdown);
  process.on('SIGINT', gracefulShutdown);
  process.on('uncaughtException', (error) => {
    console.error('[Process] Uncaught Exception:', error);
    gracefulShutdown('UNCAUGHT_EXCEPTION');
  });
  process.on('unhandledRejection', (reason, promise) => {
    console.error('[Process] Unhandled Rejection at:', promise, 'reason:', reason);
  });

  async function gracefulShutdown(signal) {
    console.log(`[Server] Received ${signal}, initiating graceful shutdown`);
    
    const shutdownTimeout = setTimeout(() => {
      console.log('[Server] Shutdown timeout exceeded, forcing exit');
      process.exit(1);
    }, 30000);

    try {
      server.close(async () => {
        console.log('[Server] HTTP server closed');
        
        try {
          // Pause all queues
          console.log('[Queues] Pausing all queues');
          await Promise.all(allQueues.map(queue => queue.pause()));

          // Wait for active jobs
          console.log('[Queues] Waiting for active jobs to complete');
          const activeJobs = await Promise.all(allQueues.map(queue => queue.getJobs(['active'])));
          const totalActiveJobs = activeJobs.reduce((sum, jobs) => sum + jobs.length, 0);
          console.log(`[Queues] Found ${totalActiveJobs} active jobs`);

          if (totalActiveJobs > 0) {
            console.log('[Queues] Waiting for active jobs to complete...');
            await new Promise(resolve => setTimeout(resolve, 5000));
          }

          // Close database connection pools
          console.log('[DB] Closing connection pools');
          for (const [key, pool] of connectionPools) {
            try {
              await pool.end();
              console.log(`[DB] Closed pool ${key}`);
            } catch (error) {
              console.error(`[DB] Error closing pool ${key}:`, error.message);
            }
          }

          // Close Redis connections
          console.log('[Redis] Closing connections');
          await Promise.all([
            redis.quit(),
            redisPub.quit(),
            redisSub.quit()
          ]);

          // Close all queues
          console.log('[Queues] Closing all queues');
          await Promise.all(allQueues.map(queue => queue.close()));

          // Clean up temp files
          console.log('[Cleanup] Cleaning temp directory');
          try {
            if (fs.existsSync(TEMP_UPLOAD_DIR)) {
              const files = fs.readdirSync(TEMP_UPLOAD_DIR);
              for (const file of files) {
                const filePath = path.join(TEMP_UPLOAD_DIR, file);
                try {
                  const stats = fs.statSync(filePath);
                  if (Date.now() - stats.mtime.getTime() > 3600000) {
                    fs.unlinkSync(filePath);
                    console.log(`[Cleanup] Deleted old temp file: ${file}`);
                  }
                } catch (err) {
                  console.warn(`[Cleanup] Could not process ${filePath}:`, err.message);
                }
              }
            }
          } catch (cleanupError) {
            console.error(`[Cleanup] Error cleaning temp directory:`, cleanupError.message);
          }
          
          clearTimeout(shutdownTimeout);
          console.log('[Server] Enhanced graceful shutdown completed successfully');
          process.exit(0);
          
        } catch (shutdownError) {
          console.error('[Server] Error during shutdown:', shutdownError.message);
          clearTimeout(shutdownTimeout);
          process.exit(1);
        }
      });
      
    } catch (error) {
      console.error('[Server] Error initiating shutdown:', error.message);
      clearTimeout(shutdownTimeout);
      process.exit(1);
    }
  }

  // =============================================================================
  // START ENHANCED SERVER
  // =============================================================================
  const PORT = process.env.PORT || process.env.X_ZOHO_CATALYST_LISTEN_PORT || 3000;
  
  server.listen(PORT, async () => {
    console.log('='.repeat(80));
    console.log(`[Server] Enhanced Professional Infinity Share server v4.0 running on port ${PORT}`);
    console.log(`[Server] Worker PID: ${process.pid}`);
    console.log(`[Server] Environment: ${process.env.NODE_ENV || 'development'}`);
    console.log(`[Server] Version: 4.0.0-enhanced`);
    console.log('='.repeat(80));
    console.log(`[Config] Max chunk size: ${(MAX_CHUNK_SIZE / (1024*1024)).toFixed(2)}MB`);
    console.log(`[Config] Min chunk size: ${(MIN_CHUNK_SIZE / (1024*1024)).toFixed(2)}MB`);
    console.log(`[Config] Max Dropbox capacity: ${(DROPBOX_ACCOUNT_MAX_CAPACITY / (1024*1024*1024)).toFixed(2)}GB`);
    console.log(`[Config] DB rotation threshold: ${DB_ROTATION_THRESHOLD}MB`);
    console.log(`[Config] Dropbox rotation threshold: ${(DROPBOX_ROTATION_THRESHOLD / (1024*1024*1024)).toFixed(2)}GB`);
    console.log(`[Config] Temp upload directory: ${TEMP_UPLOAD_DIR}`);
    console.log(`[Config] Redis URL: ${process.env.REDIS_URL || 'redis://localhost:6379'}`);
    console.log(`[Config] Concurrent uploads: ${MAX_CONCURRENT_UPLOADS}`);
    console.log(`[Config] Parallel chunks: ${MAX_PARALLEL_CHUNKS}`);
    console.log(`[Config] Upload timeout: ${UPLOAD_TIMEOUT/1000}s`);
    console.log(`[Config] Retry attempts: ${RETRY_ATTEMPTS}`);
    console.log(`[Config] Offline detection timeout: ${OFFLINE_DETECTION_TIMEOUT/1000}s`);
    console.log(`[Config] Priority boost interval: ${PRIORITY_BOOST_INTERVAL/1000}s`);
    console.log(`[Config] Fair share time slice: ${FAIR_SHARE_TIME_SLICE/1000}s`);
    console.log(`[Config] Queue rebalance interval: ${QUEUE_REBALANCE_INTERVAL/1000}s`);
    console.log(`[Config] Connection refresh interval: ${CONNECTION_REFRESH_INTERVAL/1000}s`);
    console.log('='.repeat(80));
    console.log('[Queue Priorities]');
    Object.entries(QUEUE_PRIORITIES).forEach(([name, value]) => {
      console.log(`[Config] ${name}: ${value}`);
    });
    console.log('='.repeat(80));
    
    // Test connections
    try {
      await redis.ping();
      console.log('[Redis] Connection test successful');
    } catch (error) {
      console.error('[Redis] Connection test failed:', error.message);
    }

    try {
      const queueHealths = await Promise.all(allQueues.map(queue => queue.isReady()));
      const queueNames = ['File', 'Chunk', 'Wait', 'Priority', 'Offline'];
      console.log(`[Queues] Health check: ${queueNames.map((name, i) => `${name}=${queueHealths[i]}`).join(', ')}`);
    } catch (error) {
      console.error('[Queues] Health check failed:', error.message);
    }

    console.log('[Server] Enhanced server startup completed successfully');
    console.log('[Features] Advanced queue management, offline detection, fair scheduling enabled');
    console.log('='.repeat(80));
  });

  server.on('error', (error) => {
    console.error('[Server] Startup error:', error.message);
    process.exit(1);
  });
}

if (require.main === module) {
} else {
  module.exports = { startServer };
}
