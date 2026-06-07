const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
require('dotenv').config();

const ALLOWED_ORIGINS = [
  'https://chatjeen.online',
  'https://www.chatjeen.online',
  'http://localhost:3000',
  'http://localhost:3001',
  'http://localhost:3002'
];

const app = express();
app.use(cors({
  origin: (origin, callback) => {
    // Allow requests with no origin (server-to-server, Postman, etc.)
    if (!origin) return callback(null, true);
    if (ALLOWED_ORIGINS.includes(origin)) {
      return callback(null, true);
    }
    return callback(new Error(`CORS blocked: ${origin}`), false);
  },
  methods: ['GET', 'POST'],
  credentials: true
}));

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'healthy', activeUsers: io.sockets.sockets.size });
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: ALLOWED_ORIGINS,
    methods: ['GET', 'POST'],
    credentials: true
  }
});

// Matchmaking Queue
// Item: { socket, interests: string[], country: string, joinedAt: number }
let waitingQueue = [];

// Socket storage for tracking states
// Socket properties added: partnerId, roomId, interests, country, blockedList (Set of socket.ids or IPs)
io.on('connection', (socket) => {
  console.log(`⚡ User connected: ${socket.id}`);
  socket.blockedList = new Set();

  // Helper to clean up matchmaking queue for this socket
  const removeFromQueue = (id) => {
    waitingQueue = waitingQueue.filter(item => item.socket.id !== id);
  };

  // Helper to disconnect active room
  const handleDisconnectChat = (reasonSocket) => {
    const partnerId = reasonSocket.partnerId;
    const roomId = reasonSocket.roomId;

    if (partnerId) {
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.emit('partner_left', { reason: 'skipped' });
        partnerSocket.leave(roomId);
        partnerSocket.partnerId = null;
        partnerSocket.roomId = null;
      }
    }
    reasonSocket.leave(roomId);
    reasonSocket.partnerId = null;
    reasonSocket.roomId = null;
  };

  // Matchmaking Event
  socket.on('start_matching', ({ interests = [], country = 'unknown', nickname = 'Stranger' }) => {
    // Clean up from any existing chats or queues
    removeFromQueue(socket.id);
    if (socket.roomId) {
      handleDisconnectChat(socket);
    }

    socket.interests = interests;
    socket.country = country;
    socket.nickname = nickname.trim() || 'Stranger';

    // Matching Algorithm
    let partnerItem = null;

    // 1. Try to find a partner who shares at least one tag and is not blocked
    for (let i = 0; i < waitingQueue.length; i++) {
      const candidate = waitingQueue[i];
      // Skip if blocked
      if (socket.blockedList.has(candidate.socket.id) || candidate.socket.blockedList.has(socket.id)) {
        continue;
      }

      // Check for overlapping interests
      const overlap = interests.filter(tag => candidate.interests.includes(tag));
      if (overlap.length > 0) {
        partnerItem = candidate;
        waitingQueue.splice(i, 1); // Remove candidate from queue
        break;
      }
    }

    // 2. If no tag match found, check if someone has been waiting for more than 2.5 seconds
    if (!partnerItem) {
      const now = Date.now();
      for (let i = 0; i < waitingQueue.length; i++) {
        const candidate = waitingQueue[i];
        if (socket.blockedList.has(candidate.socket.id) || candidate.socket.blockedList.has(socket.id)) {
          continue;
        }

        const waitTime = now - candidate.joinedAt;
        if (waitTime > 2500) {
          partnerItem = candidate;
          waitingQueue.splice(i, 1);
          break;
        }
      }
    }

    // If still no partner, add to queue
    if (!partnerItem) {
      waitingQueue.push({
        socket,
        interests,
        country,
        joinedAt: Date.now()
      });
      socket.emit('waiting', { queueLength: waitingQueue.length });
      console.log(`🔍 Added ${socket.id} (${socket.nickname}) to queue. Queue size: ${waitingQueue.length}`);
    } else {
      // We found a match! Create a room
      const partnerSocket = partnerItem.socket;
      const roomId = `room_${socket.id}_${partnerSocket.id}`;

      socket.partnerId = partnerSocket.id;
      socket.roomId = roomId;
      partnerSocket.partnerId = socket.id;
      partnerSocket.roomId = roomId;

      socket.join(roomId);
      partnerSocket.join(roomId);

      // Identify shared tags
      const sharedInterests = interests.filter(tag => partnerSocket.interests.includes(tag));

      // Emit to both
      socket.emit('match_found', {
        roomId,
        partnerCountry: partnerSocket.country,
        partnerNickname: partnerSocket.nickname || 'Stranger',
        sharedInterests
      });

      partnerSocket.emit('match_found', {
        roomId,
        partnerCountry: socket.country,
        partnerNickname: socket.nickname || 'Stranger',
        sharedInterests
      });

      console.log(`🤝 Match made: ${socket.id} (${socket.nickname}) <-> ${partnerSocket.id} (${partnerSocket.nickname}) in room ${roomId}`);
    }
  });

  // Relay messages
  socket.on('send_message', ({ text }) => {
    const roomId = socket.roomId;
    if (roomId) {
      // Broadcast to partner in room
      socket.to(roomId).emit('message', {
        senderId: socket.id,
        text,
        timestamp: Date.now()
      });
    }
  });

  // Typing indicator
  socket.on('typing', ({ isTyping }) => {
    const roomId = socket.roomId;
    if (roomId) {
      socket.to(roomId).emit('typing', {
        senderId: socket.id,
        isTyping
      });
    }
  });

  // Skip / Next Chat
  socket.on('skip', () => {
    console.log(`⚡ User ${socket.id} skipped`);
    if (socket.roomId) {
      handleDisconnectChat(socket);
    }
  });

  // Report User
  socket.on('report', () => {
    console.log(`🚩 User ${socket.id} reported their partner`);
    const partnerId = socket.partnerId;
    if (partnerId) {
      // Add to blocked lists
      socket.blockedList.add(partnerId);
      const partnerSocket = io.sockets.sockets.get(partnerId);
      if (partnerSocket) {
        partnerSocket.blockedList.add(socket.id);
      }
      handleDisconnectChat(socket);
    }
  });

  // Disconnect handler
  socket.on('disconnect', () => {
    console.log(`❌ User disconnected: ${socket.id}`);
    removeFromQueue(socket.id);
    if (socket.roomId) {
      handleDisconnectChat(socket);
    }
  });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`🚀 Chatjeen backend listening on port ${PORT}`);
});
