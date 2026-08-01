const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require("socket.io");
const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// HTTP & Socket.IO Server Setup
const server = http.createServer(app);
const io = new Server(server, {
  cors: { 
    origin: "*", 
    methods: ["GET", "POST"] 
  }
});

// Middleware
app.use(express.json());
app.use(cors());

// MongoDB Connection URI
const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@cluster0.um9bwdr.mongodb.net/?appName=Cluster0`;
const client = new MongoClient(uri, {
  serverApi: { version: ServerApiVersion.v1, strict: true, deprecationErrors: true }
});

// HSTU Bus Route Base Stops
const baseStops = [
  { id: 1, name: "হাবিপ্রবি মেইন ক্যাম্পাস", position: [25.6984, 88.6543], baseEta: 0 },
  { id: 2, name: "গোপালগঞ্জ মোড়", position: [25.6797, 88.6438], baseEta: 5 },
  { id: 3, name: "চেহেলগাজী মাজার", position: [25.6667, 88.6396], baseEta: 10 },
  { id: 4, name: "সরকারি কলেজ মোড়", position: [25.6469, 88.6357], baseEta: 15 },
  { id: 5, name: "দিনাজপুর বাস টার্মিনাল", position: [25.6383, 88.6362], baseEta: 18 },
  { id: 6, name: "মহারাজা মোড়", position: [25.6335, 88.6398], baseEta: 21 },
  { id: 7, name: "শাহী মসজিদ মোড়", position: [25.6292, 88.6423], baseEta: 24 },
  { id: 8, name: "শহীদ মিনার মোড়", position: [25.6264, 88.6448], baseEta: 26 },
  { id: 9, name: "দিনাজপুর সদর হাসপাতাল মোড়", position: [25.6235, 88.6468], baseEta: 28 },
  { id: 10, name: "গোর-এ-শহীদ বড় ময়দান", position: [25.6194, 88.6495], baseEta: 30 }
];

// Helper: Distance calculation between two coordinates
const getDistance = (pos1, pos2) => 
  Math.sqrt(Math.pow(pos1[0] - pos2[0], 2) + Math.pow(pos1[1] - pos2[1], 2));

// Memory Cache: Location History সেভ লিমিট করার জন্য (৩০ সেকেন্ড পর পর সেভ হবে)
const lastSavedHistoryTime = {};

async function run() {
  try {
    // Connect Database
    await client.connect();
    console.log("✅ Successfully connected to MongoDB Atlas!");

    const db = client.db("UniTransit_Live_tracker_db");
    const busesCollection = db.collection("buses");
    const locationHistoryCollection = db.collection("locationHistory");
    const reviewsCollection = db.collection("reviews");

    // Socket.IO Realtime Connection Event
    io.on("connection", (socket) => {
      console.log(`⚡ Socket Connected: ${socket.id}`);
      
      socket.on("disconnect", () => {
        console.log(`❌ Socket Disconnected: ${socket.id}`);
      });
    });

    // ====================================================
    // 🌐 1. ESP32 Location Telemetry Endpoint (POST)
    // ====================================================
    app.post('/api/bus/location', async (req, res) => {
      try {
        const { busId, lat, lng, speed, tripTime, direction } = req.body;
        
        if (!busId || lat === undefined || lng === undefined) {
          return res.status(400).send({ error: "Missing required fields: busId, lat, or lng" });
        }

        const currentTime = new Date();
        const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
        const dayName = days[currentTime.getDay()];
        const dateString = currentTime.toISOString().split('T')[0];

        const payload = {
          busId, 
          lat: parseFloat(lat), 
          lng: parseFloat(lng), 
          speed: parseFloat(speed || 0),
          tripTime: tripTime || "Normal", 
          direction: direction || "up",
          date: dateString, 
          day: dayName, 
          timestamp: currentTime
        };

        // A. Real-time Live Location (Always update latest position)
        await busesCollection.updateOne({ _id: busId }, { $set: payload }, { upsert: true });

        // B. Smart History Saving (Only insert every 30 seconds to prevent DB bloat)
        const lastSave = lastSavedHistoryTime[busId] || 0;
        if (currentTime.getTime() - lastSave > 30000) {
          await locationHistoryCollection.insertOne(payload);
          lastSavedHistoryTime[busId] = currentTime.getTime();
        }

        // C. Web/App ক্লায়েন্টদের কাছে Socket দিয়ে লাইভ ব্রডকাস্ট
        io.emit("bus-location", payload);

        res.status(200).send({ success: true, message: "Location updated successfully" });
      } catch (e) { 
        console.error("Error updating location:", e);
        res.status(500).send({ error: e.message }); 
      }
    });

    // ====================================================
    // 🚌 2. Active Buses List Get Endpoint (GET)
    // ====================================================
    app.get('/api/bus/locations', async (req, res) => {
      try {
        const activeBuses = await busesCollection.find().toArray();
        res.status(200).send(activeBuses);
      } catch (e) {
        res.status(500).send({ error: e.message });
      }
    });

    // ====================================================
    // 💬 3. Reviews API Endpoints (POST / GET)
    // ====================================================
    app.post('/api/reviews', async (req, res) => {
      try {
        const { name, role, comment, rating } = req.body;
        if (!name || !comment || !rating) {
          return res.status(400).send({ error: "Name, comment, and rating are required!" });
        }

        // নাম থেকে অটোমেটিক অ্যাভাটার ইনিশিয়াল তৈরি (যেমন: Foysal Ahmed -> FA)
        const avatar = name.trim().split(" ").map(n => n[0]).join("").toUpperCase().slice(0, 2);

        const newReview = {
          name,
          role: role || "HSTU Student",
          comment,
          rating: parseInt(rating),
          avatar,
          createdAt: new Date()
        };

        const result = await reviewsCollection.insertOne(newReview);
        res.status(201).send({ success: true, insertedId: result.insertedId });
      } catch (e) {
        res.status(500).send({ error: e.message });
      }
    });

    app.get('/api/reviews', async (req, res) => {
      try {
        // সাম্প্রতিক ৫০ টি রিভিউ দেখাবে
        const reviews = await reviewsCollection.find().sort({ createdAt: -1 }).limit(50).toArray();
        res.status(200).send(reviews);
      } catch (e) {
        res.status(500).send({ error: e.message });
      }
    });

    // ====================================================
    // 🧠 4. Predictive Traffic & Analytics AI Engine (GET)
    // ====================================================
    app.get('/api/analytics/traffic-prediction', async (req, res) => {
      try {
        const days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
        const targetDay = req.query.day || days[new Date().getDay()];
        const currentHour = new Date().getHours();

        // Memory Safe: গত ৫০০টি হিস্ট্রি রেকর্ড নিয়ে প্রসেস করবে
        const history = await locationHistoryCollection
          .find({ day: targetDay })
          .sort({ timestamp: -1 })
          .limit(500)
          .toArray();

        const stopPredictions = baseStops.map(stop => {
          const matchingLogs = history.filter(log => getDistance([log.lat, log.lng], stop.position) < 0.006);
          
          let speedSum = 0;
          let jamCount = 0;
          let totalLogs = matchingLogs.length;

          matchingLogs.forEach(l => {
            speedSum += l.speed;
            if (l.speed <= 6) jamCount++;
          });

          const avgSpeed = totalLogs > 0 ? speedSum / totalLogs : 26;
          const jamRatio = totalLogs > 0 ? jamCount / totalLogs : 0.15;
          const jamIntensity = Math.round(jamRatio * 100);

          let status = "স্বাভাবিক গতিবেগ";
          let addedDelay = 0;
          let reason = "কোনো রুট ট্রাফিক ব্লক নেই";
          let studentLoad = "স্বাভাবিক (Normal)";

          if (stop.id === 1 || stop.id === 4) {
            studentLoad = (currentHour >= 8 && currentHour <= 10) || (currentHour >= 16 && currentHour <= 17) 
              ? "অত্যধিক ভিড় (Peak Load)" 
              : "মাঝারি (Moderate)";
          }

          if (jamIntensity > 45) {
            status = "তীব্র জ্যাম (Critical)";
            addedDelay = 6;
            reason = currentHour >= 16 ? "অফিস ও স্কুল ছুটির কারণে হাইওয়ে বোতলনেক" : "স্থানিয় লোকাল যানবাহনের জটলা";
          } else if (jamIntensity > 20) {
            status = "ধীরগতি (Moderate)";
            addedDelay = 3;
            reason = "স্টপেজে শাটল বাসে শিক্ষার্থীদের অধিক চাপ ও বোর্ডিং টাইম বৃদ্ধি";
          }

          return {
            stopId: stop.id,
            stopName: stop.name,
            avgSpeed: Math.round(avgSpeed),
            jamIntensity: Math.max(5, jamIntensity),
            status,
            addedDelay,
            reason,
            studentLoad
          };
        });




















        // new code

        // ====================================================
// 📊 1. historical AVG ETA & AI Traffic Prediction API
// ====================================================
app.get('/api/analytics/predictive-eta', async (req, res) => {
  try {
    const { day, fromStopId, toStopId } = req.query;
    const db = client.db("UniTransit_Live_tracker_db");

    // ডাটাবেজ থেকে নির্দিষ্ট দিনের সমস্ত হিস্ট্রি ডাটা আনা
    const historyLogs = await db.collection("locationHistory")
      .find({ day: day || "Sunday" })
      .sort({ timestamp: -1 })
      .limit(1000)
      .toArray();

    if (historyLogs.length === 0) {
      return res.status(200).send({ message: "পর্যাপ্ত ডাটা নেই, ডিফল্ট সময় দেখান হচ্ছে", avgEtaMinutes: 30 });
    }

    // ঐতিহাসিক ডাটা থেকে গড় গতি বের করা (Average Speed)
    const totalSpeed = historyLogs.reduce((acc, log) => acc + (log.speed || 0), 0);
    const avgSpeed = totalSpeed / historyLogs.length;

    // জ্যামের ডাটা চেক (স্পিড ৮ কিমি/ঘন্টার নিচে থাকলে জ্যাম ধরা হবে)
    const jamLogs = historyLogs.filter(log => log.speed < 8);
    const jamRatio = jamLogs.length / historyLogs.length;

    // নরমাল ট্রিপ সময় ৩০ মিনিট, জ্যাম রেশিও অনুযায়ী অতিরিক্ত লেট যোগ করা
    let baseTime = 30; // 30 mins standard trip duration
    let predictedDelay = Math.round(jamRatio * 15); // সর্বোচ্চ ১৫ মিনিট স্কেচ
    let totalPredictedTime = baseTime + predictedDelay;

    res.status(200).send({
      day: day,
      averageSpeed: Math.round(avgSpeed),
      jamIntensity: Math.round(jamRatio * 100) + "%",
      baseDuration: baseTime + " Mins",
      aiPredictedDuration: totalPredictedTime + " Mins",
      delayNotice: predictedDelay > 0 ? `অতীত ডাটা অনুযায়ী আজ রুটে প্রায় +${predictedDelay} মিনিট অতিরিক্ত জ্যাম হতে পারে।` : "রুট একদম ক্লিয়ার!"
    });

  } catch (e) {
    res.status(500).send({ error: e.message });
  }
});

// ====================================================
// 🚌 2. Trip Completion Logging System (Automated Trip Saver)
// ====================================================
// ESP32 লোকেশন আপডেট হওয়ার সময় এই ফাংশনটি কল হবে
async function checkAndLogTripCompletion(busId, lat, lng, direction, db) {
  const CAMPUS_POS = [25.6984, 88.6543];
  const BORO_MATH_POS = [25.6194, 88.6495];

  // Geofence Distance (প্রায় ৫০০ মিটার এরিয়া)
  const distToCampus = getDistance([lat, lng], CAMPUS_POS);
  const distToBoroMath = getDistance([lat, lng], BORO_MATH_POS);

  const completedTripsCollection = db.collection("completedTrips");

  // Up Trip (ক্যাম্পাস -> বড় ময়দান) পৌঁছে গেছে
  if (direction === "up" && distToBoroMath < 0.005) {
    const tripId = `${busId}-up-${new Date().toISOString().split('T')[0]}`;
    await completedTripsCollection.updateOne(
      { _id: tripId },
      { 
        $set: { 
          busId, 
          route: "Campus to Boro Math (Up Trip)", 
          status: "SUCCESSFUL ✅", 
          completedAt: new Date(),
          formattedTime: new Date().toLocaleTimeString('bn-BD', { hour: '2-digit', minute: '2-digit' })
        } 
      },
      { upsert: true }
    );
  }

  // Down Trip (বড় ময়দান -> ক্যাম্পাস) পৌঁছে গেছে
  if (direction === "down" && distToCampus < 0.005) {
    const tripId = `${busId}-down-${new Date().toISOString().split('T')[0]}`;
    await completedTripsCollection.updateOne(
      { _id: tripId },
      { 
        $set: { 
          busId, 
          route: "Boro Math to Campus (Down Trip)", 
          status: "SUCCESSFUL ✅", 
          completedAt: new Date(),
          formattedTime: new Date().toLocaleTimeString('bn-BD', { hour: '2-digit', minute: '2-digit' })
        } 
      },
      { upsert: true }
    );
  }
}

// ====================================================
// 📜 3. Completed Trips History Check API
// ====================================================
app.get('/api/bus/trip-history', async (req, res) => {
  try {
    const db = client.db("UniTransit_Live_tracker_db");
    const history = await db.collection("completedTrips")
      .find()
      .sort({ completedAt: -1 })
      .limit(20)
      .toArray();

    res.status(200).send(history);
  } catch (e) {
    res.status(500).send({ error: e.message });
  }
});




    // new code end

        res.status(200).send({ selectedDay: targetDay, stopPredictions });
      } catch (e) { 
        res.status(500).send({ error: e.message }); 
      }
    });

  } catch (err) { 
    console.error("Database connection error:", err); 
  }
}

run().catch(console.dir);

// Start Server
server.listen(port, () => {
  console.log(`🚀 HSTU Bus Tracker Server is live on port ${port}`);
});