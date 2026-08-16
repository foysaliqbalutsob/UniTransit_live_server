// ============================================================
// 🚍 UNITRANSIT LIVE TRACKER
// COMPLETE REAL-TIME BACKEND
// server.jsx
// ============================================================

const express = require("express");
const cors = require("cors");
const http = require("http");
const { Server } = require("socket.io");
const { MongoClient, ServerApiVersion } = require("mongodb");
const crypto = require("crypto");
require("dotenv").config();

// ============================================================
// APP CONFIGURATION
// ============================================================

const app = express();
const PORT = process.env.PORT || 3000;

// ============================================================
// HTTP SERVER
// ============================================================

const server = http.createServer(app);

// ============================================================
// SOCKET.IO
// ============================================================

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE"],
  },

  transports: ["websocket", "polling"],
});

// ============================================================
// MIDDLEWARE
// ============================================================

app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE"],
  })
);

app.use(express.json({ limit: "1mb" }));

// ============================================================
// BASIC ROUTE
// ============================================================

app.get("/", (req, res) => {
  res.json({
    success: true,
    message: "🚍 UniTransit Live Tracker Backend Running",
    version: "2.0.0",
    time: new Date().toISOString(),
  });
});

// ============================================================
// HEALTH CHECK
// ============================================================

app.get("/api/health", (req, res) => {
  res.json({
    success: true,
    server: "online",
    socket: true,
    timestamp: new Date().toISOString(),
  });
});

// ============================================================
// MONGODB
// ============================================================

const DB_NAME = "UniTransit_Live_tracker_db";

const uri =
  `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}` +
  `@cluster0.um9bwdr.mongodb.net/?appName=Cluster0`;

const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});

// ============================================================
// CONSTANTS
// ============================================================

// Host becomes offline after 60 seconds
const HOST_TIMEOUT = 60 * 1000;

// Save history every 30 seconds per bus
const HISTORY_SAVE_INTERVAL = 30 * 1000;

// Trip completion distance
const TRIP_COMPLETION_DISTANCE = 0.0015;

// Prevent duplicate trip completion
const TRIP_COMPLETION_COOLDOWN = 10 * 60 * 1000;

// ============================================================
// DEFAULT BUSES
// ============================================================

const DEFAULT_BUSES = [
  {
    busId: "BUS-01",
    name: "HSTU Bus 01",
  },

  {
    busId: "BUS-02",
    name: "HSTU Bus 02",
  },

  {
    busId: "BUS-03",
    name: "HSTU Bus 03",
  },

  {
    busId: "BUS-0012",
    name: "HSTU Bus 0012",
  },
];

// ============================================================
// HSTU ROUTE STOPS
// ============================================================

const baseStops = [
  {
    id: 1,
    name: "হাবিপ্রবি মেইন ক্যাম্পাস",
    position: [25.6984, 88.6543],
    baseEta: 0,
  },

  {
    id: 2,
    name: "গোপালগঞ্জ মোড়",
    position: [25.6797, 88.6438],
    baseEta: 5,
  },

  {
    id: 3,
    name: "চেহেলগাজী মাজার",
    position: [25.6667, 88.6396],
    baseEta: 10,
  },

  {
    id: 4,
    name: "সরকারি কলেজ মোড়",
    position: [25.6469, 88.6357],
    baseEta: 15,
  },

  {
    id: 5,
    name: "দিনাজপুর বাস টার্মিনাল",
    position: [25.6383, 88.6362],
    baseEta: 18,
  },

  {
    id: 6,
    name: "মহারাজা মোড়",
    position: [25.6335, 88.6398],
    baseEta: 21,
  },

  {
    id: 7,
    name: "শাহী মসজিদ মোড়",
    position: [25.6292, 88.6423],
    baseEta: 24,
  },

  {
    id: 8,
    name: "শহীদ মিনার মোড়",
    position: [25.6264, 88.6448],
    baseEta: 26,
  },

  {
    id: 9,
    name: "দিনাজপুর সদর হাসপাতাল মোড়",
    position: [25.6235, 88.6468],
    baseEta: 28,
  },

  {
    id: 10,
    name: "গোর-এ-শহীদ বড় ময়দান",
    position: [25.6194, 88.6495],
    baseEta: 30,
  },
];

// ============================================================
// DISTANCE
// ============================================================

function getDistance(pos1, pos2) {
  if (!pos1 || !pos2) return Infinity;

  return Math.sqrt(
    Math.pow(pos1[0] - pos2[0], 2) +
      Math.pow(pos1[1] - pos2[1], 2)
  );
}

// ============================================================
// FIND CURRENT STOP
// ============================================================

function findNearestStop(lat, lng, direction = "up") {
  const route =
    direction === "up"
      ? baseStops
      : [...baseStops].reverse();

  let nearestStop = route[0];
  let minDistance = Infinity;

  route.forEach((stop) => {
    const distance = getDistance(
      [lat, lng],
      stop.position
    );

    if (distance < minDistance) {
      minDistance = distance;
      nearestStop = stop;
    }
  });

  return {
    stop: nearestStop,
    distance: minDistance,
  };
}

// ============================================================
// MEMORY CACHE
// ============================================================

const lastSavedHistoryTime = {};
const lastCompletedTrip = {};

// ============================================================
// DATABASE REFERENCES
// ============================================================

let db;
let busesCollection;
let locationHistoryCollection;
let reviewsCollection;
let activeHostsCollection;
let completedTripsCollection;

// ============================================================
// SOCKET CONNECTION
// ============================================================

io.on("connection", (socket) => {
  console.log(`⚡ Socket connected: ${socket.id}`);

  socket.on("disconnect", (reason) => {
    console.log(
      `❌ Socket disconnected: ${socket.id} → ${reason}`
    );
  });
});

// ============================================================
// ENSURE BUS EXISTS
// ============================================================

async function ensureBusExists(busId) {
  if (!busId) return false;

  const existingBus = await busesCollection.findOne({
    _id: busId,
  });

  if (existingBus) {
    return true;
  }

  await busesCollection.insertOne({
    _id: busId,

    busId,

    name: `HSTU ${busId}`,

    lat: 25.6984,

    lng: 88.6543,

    speed: 0,

    accuracy: 0,

    sats: 0,

    tripTime: "Normal",

    direction: "up",

    source: "system",

    active: false,

    timestamp: new Date(),
  });

  console.log(
    `🚌 New bus registered automatically → ${busId}`
  );

  return true;
}

// ============================================================
// UPDATE LIVE BUS
// ============================================================

async function updateLiveBus(payload) {
  const busId = payload.busId;

  if (!busId) return;

  const serverTimestamp = new Date();

  const livePayload = {
    ...payload,

    _id: busId,

    busId,

    active: true,

    timestamp: serverTimestamp,

    lastGpsUpdate: serverTimestamp,
  };

  await busesCollection.updateOne(
    {
      _id: busId,
    },

    {
      $set: livePayload,
    },

    {
      upsert: true,
    }
  );

  return livePayload;
}

// ============================================================
// SAVE LOCATION HISTORY
// ============================================================

async function saveLocationHistory(payload) {
  try {
    const busId = payload.busId;

    if (!busId) return;

    const now = Date.now();

    const lastSave =
      lastSavedHistoryTime[busId] || 0;

    if (
      now - lastSave <
      HISTORY_SAVE_INTERVAL
    ) {
      return;
    }

    await locationHistoryCollection.insertOne({
      ...payload,

      timestamp:
        payload.timestamp || new Date(),

      savedAt: new Date(),
    });

    lastSavedHistoryTime[busId] = now;

    console.log(
      `📚 History saved → ${busId}`
    );
  } catch (error) {
    console.error(
      "❌ History save error:",
      error.message
    );
  }
}

// ============================================================
// CHECK TRIP COMPLETION
// ============================================================

async function checkAndLogTripCompletion(
  busId,
  lat,
  lng,
  direction
) {
  try {
    const route =
      direction === "up"
        ? baseStops
        : [...baseStops].reverse();

    const finalStop =
      route[route.length - 1];

    const distance = getDistance(
      [lat, lng],
      finalStop.position
    );

    if (
      distance >
      TRIP_COMPLETION_DISTANCE
    ) {
      return;
    }

    const now = Date.now();

    if (
      lastCompletedTrip[busId] &&
      now - lastCompletedTrip[busId] <
        TRIP_COMPLETION_COOLDOWN
    ) {
      return;
    }

    const trip = {
      tripId: crypto.randomUUID(),

      busId,

      direction,

      startStop: route[0].name,

      endStop: finalStop.name,

      completedAt: new Date(),

      lat,

      lng,
    };

    await completedTripsCollection.insertOne(
      trip
    );

    lastCompletedTrip[busId] = now;

    console.log(
      `🏁 Trip completed → ${busId}`
    );

    io.emit("trip-completed", trip);
  } catch (error) {
    console.error(
      "Trip completion error:",
      error.message
    );
  }
}

// ============================================================
// BUILD GPS PAYLOAD
// ============================================================

function buildGpsPayload({
  busId,
  lat,
  lng,
  speed,
  accuracy,
  sats,
  tripTime,
  direction,
  source,
  hostId,
  hostName,
}) {
  const latitude = Number(lat);

  const longitude = Number(lng);

  const currentSpeed =
    Number(speed) || 0;

  const currentAccuracy =
    Number(accuracy) || 0;

  const currentSats =
    Number(sats) || 0;

  const finalDirection =
    direction === "down"
      ? "down"
      : "up";

  const nearest = findNearestStop(
    latitude,
    longitude,
    finalDirection
  );

  return {
    busId,

    lat: latitude,

    lng: longitude,

    speed: currentSpeed,

    accuracy: currentAccuracy,

    sats: currentSats,

    tripTime:
      tripTime || "Normal",

    direction: finalDirection,

    source:
      source || "unknown",

    hostId:
      hostId || null,

    hostName:
      hostName || null,

    currentStopId:
      nearest.stop?.id || null,

    currentStopName:
      nearest.stop?.name || "রুটে",

    distanceFromStop:
      nearest.distance,

    active: true,

    // IMPORTANT:
    // Always server time.
    timestamp: new Date(),
  };
}

// ============================================================
// BROADCAST LIVE LOCATION
// ============================================================

function broadcastBusLocation(payload) {
  io.emit(
    "bus-location",
    payload
  );

  console.log(
    `📡 LIVE → ${payload.busId} | ` +
      `${payload.lat.toFixed(6)}, ` +
      `${payload.lng.toFixed(6)} | ` +
      `${payload.source}`
  );
}

// ============================================================
// 1️⃣ ESP32 GPS LOCATION
// ============================================================

app.post(
  "/api/bus/location",
  async (req, res) => {
    try {
      const {
        busId,
        lat,
        lng,
        speed,
        tripTime,
        direction,
        sats,
        accuracy,
      } = req.body;

      if (
        !busId ||
        lat === undefined ||
        lng === undefined
      ) {
        return res.status(400).json({
          success: false,

          error:
            "busId, lat and lng are required",
        });
      }

      const latitude = Number(lat);

      const longitude = Number(lng);

      if (
        !Number.isFinite(latitude) ||
        !Number.isFinite(longitude)
      ) {
        return res.status(400).json({
          success: false,

          error:
            "Invalid latitude or longitude",
        });
      }

      await ensureBusExists(busId);

      const payload =
        buildGpsPayload({
          busId,

          lat: latitude,

          lng: longitude,

          speed,

          accuracy,

          sats,

          tripTime,

          direction,

          source: "esp32",
        });

      // DATABASE
      const livePayload =
        await updateLiveBus(
          payload
        );

      // HISTORY
      await saveLocationHistory(
        livePayload
      );

      // TRIP COMPLETION
      await checkAndLogTripCompletion(
        busId,

        latitude,

        longitude,

        payload.direction
      );

      // REAL TIME SOCKET
      broadcastBusLocation(
        livePayload
      );

      return res.json({
        success: true,

        message:
          "ESP32 GPS location received",

        data: livePayload,
      });
    } catch (error) {
      console.error(
        "ESP32 location error:",
        error
      );

      return res.status(500).json({
        success: false,

        error:
          "Failed to update GPS location",
      });
    }
  }
);

// ============================================================
// 2️⃣ GET ALL LIVE BUSES
// ============================================================

app.get(
  "/api/bus/locations",
  async (req, res) => {
    try {
      const buses =
        await busesCollection
          .find({})
          .sort({
            timestamp: -1,
          })
          .toArray();

      const now = Date.now();

      const result = buses.map(
        (bus) => {
          const timestamp =
            bus.timestamp
              ? new Date(
                  bus.timestamp
                ).getTime()
              : 0;

          const isOnline =
            timestamp > 0 &&
            now - timestamp <=
              HOST_TIMEOUT;

          return {
            ...bus,

            active:
              isOnline,

            online:
              isOnline,
          };
        }
      );

      res.json(result);
    } catch (error) {
      console.error(
        "Get buses error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to get buses",
      });
    }
  }
);

// ============================================================
// 3️⃣ GET SINGLE BUS
// ============================================================

app.get(
  "/api/bus/:busId",
  async (req, res) => {
    try {
      const busId =
        req.params.busId;

      const bus =
        await busesCollection.findOne({
          _id: busId,
        });

      if (!bus) {
        return res.status(404).json({
          success: false,

          error: "Bus not found",
        });
      }

      const timestamp =
        bus.timestamp
          ? new Date(
              bus.timestamp
            ).getTime()
          : 0;

      const online =
        timestamp > 0 &&
        Date.now() - timestamp <=
          HOST_TIMEOUT;

      res.json({
        success: true,

        data: {
          ...bus,

          active: online,

          online,
        },
      });
    } catch (error) {
      console.error(
        "Single bus error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to get bus",
      });
    }
  }
);

// ============================================================
// 4️⃣ PHONE HOST START
// ============================================================

app.post(
  "/api/bus/host/start",
  async (req, res) => {
    try {
      const {
        busId,
        hostId,
        hostName,
      } = req.body;

      if (!busId || !hostId) {
        return res.status(400).json({
          success: false,

          error:
            "busId and hostId are required",
        });
      }

      await ensureBusExists(busId);

      const now = new Date();

      await activeHostsCollection.updateOne(
        {
          busId,

          hostId,
        },

        {
          $set: {
            busId,

            hostId,

            hostName:
              hostName || "Phone Host",

            active: true,

            startedAt: now,

            lastUpdate: now,

            socketId: null,
          },
        },

        {
          upsert: true,
        }
      );

      await busesCollection.updateOne(
        {
          _id: busId,
        },

        {
          $set: {
            active: true,

            source: "phone",

            hostId,

            hostName:
              hostName || "Phone Host",

            timestamp: now,

            lastGpsUpdate: now,
          },
        }
      );

      const response = {
        busId,

        hostId,

        hostName:
          hostName || "Phone Host",

        active: true,

        timestamp: now,
      };

      io.emit(
        "bus-host-started",
        response
      );

      console.log(
        `📱 Host started → ${busId} → ${hostId}`
      );

      res.json({
        success: true,

        message:
          "Phone GPS host started",

        data: response,
      });
    } catch (error) {
      console.error(
        "Host start error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to start host",
      });
    }
  }
);

// ============================================================
// 5️⃣ PHONE HOST LOCATION
// ============================================================

app.post(
  "/api/bus/host/location",
  async (req, res) => {
    try {
      const {
        busId,
        hostId,
        hostName,
        lat,
        lng,
        speed,
        accuracy,
        direction,
        tripTime,
      } = req.body;

      if (
        !busId ||
        !hostId ||
        lat === undefined ||
        lng === undefined
      ) {
        return res.status(400).json({
          success: false,

          error:
            "busId, hostId, lat and lng are required",
        });
      }

      const latitude = Number(lat);

      const longitude = Number(lng);

      if (
        !Number.isFinite(latitude) ||
        !Number.isFinite(longitude)
      ) {
        return res.status(400).json({
          success: false,

          error:
            "Invalid coordinates",
        });
      }

      await ensureBusExists(busId);

      const now = new Date();

      // --------------------------------------------------------
      // UPDATE HOST
      // --------------------------------------------------------

      await activeHostsCollection.updateOne(
        {
          busId,

          hostId,
        },

        {
          $set: {
            busId,

            hostId,

            hostName:
              hostName || "Phone Host",

            active: true,

            lastUpdate: now,
          },
        },

        {
          upsert: true,
        }
      );

      // --------------------------------------------------------
      // BUILD PAYLOAD
      // --------------------------------------------------------

      const payload =
        buildGpsPayload({
          busId,

          lat: latitude,

          lng: longitude,

          speed,

          accuracy,

          sats: 0,

          tripTime,

          direction,

          source: "phone",

          hostId,

          hostName,
        });

      // --------------------------------------------------------
      // DATABASE
      // --------------------------------------------------------

      const livePayload =
        await updateLiveBus(
          payload
        );

      // --------------------------------------------------------
      // HISTORY
      // --------------------------------------------------------

      await saveLocationHistory(
        livePayload
      );

      // --------------------------------------------------------
      // TRIP COMPLETION
      // --------------------------------------------------------

      await checkAndLogTripCompletion(
        busId,

        latitude,

        longitude,

        payload.direction
      );

      // --------------------------------------------------------
      // SOCKET
      // --------------------------------------------------------

      broadcastBusLocation(
        livePayload
      );

      res.json({
        success: true,

        message:
          "Phone GPS location updated",

        data: livePayload,
      });
    } catch (error) {
      console.error(
        "Phone GPS error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to update phone GPS",
      });
    }
  }
);

// ============================================================
// 6️⃣ PHONE HOST STOP
// ============================================================

app.post(
  "/api/bus/host/stop",
  async (req, res) => {
    try {
      const {
        busId,
        hostId,
      } = req.body;

      if (!busId || !hostId) {
        return res.status(400).json({
          success: false,

          error:
            "busId and hostId are required",
        });
      }

      await activeHostsCollection.updateOne(
        {
          busId,

          hostId,
        },

        {
          $set: {
            active: false,

            stoppedAt:
              new Date(),
          },
        }
      );

      // Check if another host is still active
      const anotherHost =
        await activeHostsCollection.findOne({
          busId,

          active: true,
        });

      if (!anotherHost) {
        await busesCollection.updateOne(
          {
            _id: busId,
          },

          {
            $set: {
              active: false,

              timestamp:
                new Date(),
            },
          }
        );
      }

      const eventData = {
        busId,

        hostId,

        active: false,

        timestamp:
          new Date(),
      };

      io.emit(
        "bus-host-stopped",
        eventData
      );

      console.log(
        `🛑 Host stopped → ${busId}`
      );

      res.json({
        success: true,

        message:
          "Host stopped",

        data: eventData,
      });
    } catch (error) {
      console.error(
        "Host stop error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to stop host",
      });
    }
  }
);

// ============================================================
// 7️⃣ GET ACTIVE HOSTS
// ============================================================

app.get(
  "/api/bus/hosts",
  async (req, res) => {
    try {
      const hosts =
        await activeHostsCollection
          .find({
            active: true,
          })
          .toArray();

      res.json({
        success: true,

        data: hosts,
      });
    } catch (error) {
      res.status(500).json({
        success: false,

        error:
          "Failed to get hosts",
      });
    }
  }
);

// ============================================================
// 8️⃣ LOCATION HISTORY
// ============================================================

app.get(
  "/api/bus/:busId/history",
  async (req, res) => {
    try {
      const busId =
        req.params.busId;

      const limit =
        Math.min(
          Number(req.query.limit) || 100,
          1000
        );

      const history =
        await locationHistoryCollection
          .find({
            busId,
          })
          .sort({
            timestamp: -1,
          })
          .limit(limit)
          .toArray();

      res.json({
        success: true,

        busId,

        count: history.length,

        data: history,
      });
    } catch (error) {
      console.error(
        "History error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to load history",
      });
    }
  }
);

// ============================================================
// 9️⃣ COMPLETED TRIPS
// ============================================================

app.get(
  "/api/trips/completed",
  async (req, res) => {
    try {
      const busId =
        req.query.busId;

      const filter = busId
        ? { busId }
        : {};

      const trips =
        await completedTripsCollection
          .find(filter)
          .sort({
            completedAt: -1,
          })
          .limit(100)
          .toArray();

      res.json({
        success: true,

        data: trips,
      });
    } catch (error) {
      console.error(
        "Completed trips error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to load trips",
      });
    }
  }
);

// ============================================================
// 🔟 TRAFFIC / AI PREDICTION
// ============================================================

function generateTrafficPrediction(
  day
) {
  const dayFactors = {
    Sunday: 0.55,

    Monday: 0.85,

    Tuesday: 0.90,

    Wednesday: 0.88,

    Thursday: 0.92,

    Friday: 0.60,

    Saturday: 0.70,
  };

  const factor =
    dayFactors[day] || 0.75;

  return baseStops.map(
    (stop, index) => {
      // Different traffic pattern
      // for different stops
      const locationFactor =
        1 +
        Math.sin(
          index * 1.7
        ) *
          0.15;

      const rawIntensity =
        25 +
        factor *
          60 *
          locationFactor;

      const jamIntensity =
        Math.max(
          0,
          Math.min(
            100,
            Math.round(
              rawIntensity
            )
          )
        );

      let addedDelay = 0;

      if (jamIntensity >= 75) {
        addedDelay =
          Math.round(
            jamIntensity / 25
          );
      } else if (
        jamIntensity >= 50
      ) {
        addedDelay =
          Math.round(
            jamIntensity / 35
          );
      } else {
        addedDelay = 0;
      }

      let status = "Low";

      if (jamIntensity >= 75) {
        status = "High";
      } else if (
        jamIntensity >= 50
      ) {
        status = "Moderate";
      }

      let studentLoad =
        "স্বাভাবিক (Normal)";

      if (jamIntensity >= 75) {
        studentLoad =
          "অত্যধিক ভিড় (Peak Load)";
      } else if (
        jamIntensity >= 50
      ) {
        studentLoad =
          "মাঝারি (Moderate)";
      }

      let reason =
        "রুট স্বাভাবিক";

      if (jamIntensity >= 75) {
        reason =
          "ব্যস্ত সময় ও বেশি যাত্রী চাপ";
      } else if (
        jamIntensity >= 50
      ) {
        reason =
          "মাঝারি ট্রাফিক ও যাত্রী চাপ";
      }

      return {
        stopId: stop.id,

        stopName: stop.name,

        jamIntensity,

        addedDelay,

        status,

        reason,

        studentLoad,
      };
    }
  );
}

// ============================================================
// AI TRAFFIC API
// ============================================================

app.get(
  "/api/analytics/traffic-prediction",
  async (req, res) => {
    try {
      const days = [
        "Sunday",
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
      ];

      const day =
        days.includes(
          req.query.day
        )
          ? req.query.day
          : days[
              new Date().getDay()
            ];

      const stopPredictions =
        generateTrafficPrediction(
          day
        );

      const totalDelay =
        stopPredictions.reduce(
          (sum, item) =>
            sum +
            Number(
              item.addedDelay || 0
            ),

          0
        );

      res.json({
        success: true,

        day,

        generatedAt:
          new Date(),

        totalDelay,

        stopPredictions,
      });
    } catch (error) {
      console.error(
        "AI prediction error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to generate prediction",
      });
    }
  }
);

// ============================================================
// 1️⃣1️⃣ REVIEWS
// ============================================================

app.post(
  "/api/reviews",
  async (req, res) => {
    try {
      const {
        busId,
        name,
        rating,
        comment,
      } = req.body;

      if (
        !name ||
        !rating ||
        !comment
      ) {
        return res.status(400).json({
          success: false,

          error:
            "name, rating and comment are required",
        });
      }

      const numericRating =
        Number(rating);

      if (
        numericRating < 1 ||
        numericRating > 5
      ) {
        return res.status(400).json({
          success: false,

          error:
            "Rating must be between 1 and 5",
        });
      }

      const review = {
        busId:
          busId || null,

        name,

        rating: numericRating,

        comment,

        createdAt:
          new Date(),
      };

      const result =
        await reviewsCollection.insertOne(
          review
        );

      res.status(201).json({
        success: true,

        message:
          "Review submitted",

        data: {
          ...review,

          _id: result.insertedId,
        },
      });
    } catch (error) {
      console.error(
        "Review error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to submit review",
      });
    }
  }
);

// ============================================================
// 1️⃣2️⃣ GET REVIEWS
// ============================================================

app.get(
  "/api/reviews",
  async (req, res) => {
    try {
      const busId =
        req.query.busId;

      const filter = busId
        ? { busId }
        : {};

      const reviews =
        await reviewsCollection
          .find(filter)
          .sort({
            createdAt: -1,
          })
          .limit(100)
          .toArray();

      res.json({
        success: true,

        data: reviews,
      });
    } catch (error) {
      console.error(
        "Reviews fetch error:",
        error.message
      );

      res.status(500).json({
        success: false,

        error:
          "Failed to load reviews",
      });
    }
  }
);

// ============================================================
// 1️⃣3️⃣ SERVER TIME
// ============================================================

app.get(
  "/api/server-time",
  (req, res) => {
    res.json({
      success: true,

      timestamp:
        new Date().toISOString(),

      unix:
        Date.now(),
    });
  }
);

// ============================================================
// 1️⃣4️⃣ HOST TIMEOUT CHECKER
// ============================================================

async function checkHostTimeouts() {
  try {
    const now =
      Date.now();

    const activeHosts =
      await activeHostsCollection
        .find({
          active: true,
        })
        .toArray();

    for (const host of activeHosts) {
      const lastUpdate =
        host.lastUpdate
          ? new Date(
              host.lastUpdate
            ).getTime()
          : 0;

      if (!lastUpdate) {
        continue;
      }

      const elapsed =
        now - lastUpdate;

      if (
        elapsed >
        HOST_TIMEOUT
      ) {
        console.log(
          `⏰ HOST TIMEOUT → ${host.busId}`
        );

        await activeHostsCollection.updateOne(
          {
            _id: host._id,
          },

          {
            $set: {
              active: false,

              expiredAt:
                new Date(),
            },
          }
        );

        // Check if another host
        // for same bus is alive
        const anotherHost =
          await activeHostsCollection.findOne(
            {
              busId:
                host.busId,

              active: true,
            }
          );

        if (!anotherHost) {
          await busesCollection.updateOne(
            {
              _id:
                host.busId,
            },

            {
              $set: {
                active: false,

                timestamp:
                  new Date(),
              },
            }
          );

          io.emit(
            "bus-host-expired",
            {
              busId:
                host.busId,

              hostId:
                host.hostId,

              active: false,

              timestamp:
                new Date(),
            }
          );
        }
      }
    }

    // --------------------------------------------------------
    // ALSO CHECK BUS TIMESTAMP
    // --------------------------------------------------------

    const buses =
      await busesCollection
        .find({
          active: true,
        })
        .toArray();

    for (const bus of buses) {
      const timestamp =
        bus.timestamp
          ? new Date(
              bus.timestamp
            ).getTime()
          : 0;

      if (
        timestamp &&
        now - timestamp >
          HOST_TIMEOUT
      ) {
        await busesCollection.updateOne(
          {
            _id:
              bus._id,
          },

          {
            $set: {
              active: false,
            },
          }
        );

        io.emit(
          "bus-host-expired",
          {
            busId:
              bus.busId,

            active: false,

            timestamp:
              new Date(),
          }
        );

        console.log(
          `🔴 BUS OFFLINE → ${bus.busId}`
        );
      }
    }
  } catch (error) {
    console.error(
      "Host timeout checker error:",
      error.message
    );
  }
}

// ============================================================
// PERIODIC TIMEOUT CHECK
// ============================================================

// Every 5 seconds
setInterval(
  checkHostTimeouts,
  5000
);

// ============================================================
// START SERVER
// ============================================================

async function startServer() {
  try {
    console.log(
      "=========================================="
    );

    console.log(
      "🚀 Starting UniTransit Backend..."
    );

    console.log(
      "=========================================="
    );

    // --------------------------------------------------------
    // CONNECT MONGODB
    // --------------------------------------------------------

    await client.connect();

    await client.db(
      "admin"
    ).command({
      ping: 1,
    });

    console.log(
      "✅ MongoDB Atlas Connected"
    );

    // --------------------------------------------------------
    // DATABASE
    // --------------------------------------------------------

    db =
      client.db(DB_NAME);

    // --------------------------------------------------------
    // COLLECTIONS
    // --------------------------------------------------------

    busesCollection =
      db.collection("buses");

    locationHistoryCollection =
      db.collection(
        "locationHistory"
      );

    reviewsCollection =
      db.collection("reviews");

    activeHostsCollection =
      db.collection(
        "activeHosts"
      );

    completedTripsCollection =
      db.collection(
        "completedTrips"
      );

    // --------------------------------------------------------
    // INDEXES
    // --------------------------------------------------------

    try {
      await activeHostsCollection.createIndex(
        {
          busId: 1,

          hostId: 1,
        },

        {
          unique: true,
        }
      );

      await activeHostsCollection.createIndex(
        {
          busId: 1,

          active: 1,
        }
      );

      await busesCollection.createIndex(
        {
          timestamp: -1,
        }
      );

      await locationHistoryCollection.createIndex(
        {
          busId: 1,

          timestamp: -1,
        }
      );

      await completedTripsCollection.createIndex(
        {
          busId: 1,

          completedAt: -1,
        }
      );

      await reviewsCollection.createIndex(
        {
          createdAt: -1,
        }
      );

      console.log(
        "✅ MongoDB indexes ready"
      );
    } catch (error) {
      console.log(
        "⚠️ Index setup:",
        error.message
      );
    }

    // --------------------------------------------------------
    // REGISTER DEFAULT BUSES
    // --------------------------------------------------------

    for (const bus of DEFAULT_BUSES) {
      try {
        await busesCollection.updateOne(
          {
            _id:
              bus.busId,
          },

          {
            $setOnInsert: {
              _id:
                bus.busId,

              busId:
                bus.busId,

              name:
                bus.name,

              lat:
                25.6984,

              lng:
                88.6543,

              speed:
                0,

              accuracy:
                0,

              sats:
                0,

              tripTime:
                "Normal",

              direction:
                "up",

              source:
                "system",

              active:
                false,

              timestamp:
                new Date(),
            },
          },

          {
            upsert: true,
          }
        );
      } catch (error) {
        console.log(
          `Bus registration error ${bus.busId}:`,
          error.message
        );
      }
    }

    console.log(
      "🚌 Default buses ready"
    );

    // --------------------------------------------------------
    // START HTTP SERVER
    // --------------------------------------------------------

    server.listen(
      PORT,
      "0.0.0.0",
      () => {
        console.log(
          "=========================================="
        );

        console.log(
          `🚀 Server running on port ${PORT}`
        );

        console.log(
          `🌐 http://localhost:${PORT}`
        );

        console.log(
          "📡 Socket.IO ready"
        );

        console.log(
          "🚌 Real-time GPS ready"
        );

        console.log(
          "📱 Phone Host ready"
        );

        console.log(
          "📡 ESP32 GPS ready"
        );

        console.log(
          "🤖 AI Traffic Prediction ready"
        );

        console.log(
          "=========================================="
        );
      }
    );
  } catch (error) {
    console.error(
      "❌ SERVER START ERROR:",
      error
    );

    process.exit(1);
  }
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

async function shutdown() {
  console.log(
    "\n🛑 Shutting down server..."
  );

  try {
    await client.close();

    server.close(() => {
      console.log(
        "✅ Server closed"
      );

      process.exit(0);
    });
  } catch (error) {
    console.error(
      "Shutdown error:",
      error.message
    );

    process.exit(1);
  }
}

process.on(
  "SIGINT",
  shutdown
);

process.on(
  "SIGTERM",
  shutdown
);

// ============================================================
// START
// ============================================================

startServer();