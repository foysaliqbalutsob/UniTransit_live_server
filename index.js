// const express = require('express');
// const cors = require('cors')


// const app = express();
// require('dotenv').config();
// const { MongoClient, ServerApiVersion } = require('mongodb');



// const port = process.env.PORT || 3000

// // Middleware

// app.use(express.json());
// app.use(cors());



// const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@cluster0.um9bwdr.mongodb.net/?appName=Cluster0`;

// // Create a MongoClient with a MongoClientOptions object to set the Stable API version
// const client = new MongoClient(uri, {
//   serverApi: {
//     version: ServerApiVersion.v1,
//     strict: true,
//     deprecationErrors: true,
//   }
// });


// async function run() {
//   try {
//     // Connect the client to the server	(optional starting in v4.7)
//     await client.connect();








//     const db = client.db("UniTransit_Live_tracker_db");
//     const liveLocationCollection = db.collection("liveLocation");

//     // Live location

//     app.get('/liveLocations', async(req, res)=>{

//     });



//   app.post('/liveLocations', async(req, res)=>{
//     const data =req.body;
//     const result = await liveLocationCollection.insertOne(data);
//     res.send(result);

//     })
















//     // Send a ping to confirm a successful connection
//     await client.db("admin").command({ ping: 1 });
//     console.log("Pinged your deployment. You successfully connected to MongoDB!");
//   } finally {
//     // Ensures that the client will close when you finish/error
//     // await client.close();
//   }
// }
// run().catch(console.dir);


// app.get('/', (req, res) => {
//   res.send('!UniTransit Live i here')
// })

// app.listen(port, () => {
//   console.log(`Example app listening on port ${port}`)
// })


const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require("socket.io");
const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// HTTP Server & Socket.io Setup
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*", // সব ধরনের ফ্রন্টএন্ড কানেকশন অ্যালাউ করার জন্য
    methods: ["GET", "POST"]
  }
});

// Middleware
app.use(express.json());
app.use(cors());

// MongoDB Connection
const uri = `mongodb+srv://${process.env.DB_USER}:${process.env.DB_PASS}@cluster0.um9bwdr.mongodb.net/?appName=Cluster0`;
const client = new MongoClient(uri, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  }
});

async function run() {
  try {
    await client.connect();
    const db = client.db("UniTransit_Live_tracker_db");
    
    const busesCollection = db.collection("buses");
    const locationHistoryCollection = db.collection("locationHistory");

    console.log("MongoDB Connected Successfully for Mobile Testing!");

    // Socket.io Connection
    io.on("connection", (socket) => {
      console.log(`Device/Browser connected: ${socket.id}`);
      socket.on("disconnect", () => console.log("Device disconnected"));
    });

    // ==========================================
    // REST API ENDPOINTS (Testing + Production)
    // ==========================================

    // ১. লোকেশন রিসিভ করার রুট (ফোন অথবা ESP32—উভয় থেকেই এই রুটে ডেটা আসবে)
    // POST /api/bus/location
    app.post('/api/bus/location', async (req, res) => {
      try {
        const { busId, lat, lng, speed, tripTime } = req.body;
        
        if (!busId) {
          return res.status(400).send({ error: "busId is required!" });
        }

        const currentTime = new Date();

        // ক) কারেন্ট লোকেশন কালেকশনে Upsert করা
        await busesCollection.updateOne(
          { _id: busId },
          {
            $set: {
              lat: parseFloat(lat),
              lng: parseFloat(lng),
              speed: speed ? parseFloat(speed) : 0,
              tripTime: tripTime || "Normal", // ৪:৩০ এর বাস ওলটপালট ট্র্যাকিং এর জন্য ট্রিপ টাইম ট্র্যাক
              updatedAt: currentTime
            }
          },
          { upsert: true }
        );

        // খ) হিস্ট্রি কালেকশনে ডেটা সেভ করা
        const historyData = {
          busId,
          lat: parseFloat(lat),
          lng: parseFloat(lng),
          speed: speed ? parseFloat(speed) : 0,
          tripTime: tripTime || "Normal",
          time: currentTime
        };
        await locationHistoryCollection.insertOne(historyData);

        // গ) Socket.io দিয়ে রিয়েল-টাইম ফ্রন্টএন্ডে ব্রডকাস্ট করা
        io.emit("bus-location", historyData);

        res.status(200).send({ success: true, message: "Location received from device!" });
      } catch (error) {
        res.status(500).send({ error: error.message });
      }
    });

    // ২. গত ৩০ মিনিটে অ্যাক্টিভ থাকা বাসগুলোর লিস্ট নেওয়ার রুট (টাইম-বেসড কুয়েরি)
    // GET /api/buses/active
    app.get('/api/buses/active', async (req, res) => {
      try {
        // বর্তমান সময় থেকে ৩০ মিনিট আগের সময় বের করা
        const thirtyMinutesAgo = new Date(Date.now() - 30 * 60 * 1000);

        // কুয়েরি: যেসব বাসের 'updatedAt' গত ৩০ মিনিটের মধ্যে
        const query = {
          updatedAt: { $gte: thirtyMinutesAgo }
        };

        const activeBuses = await busesCollection.find(query).toArray();
        res.status(200).send(activeBuses);
      } catch (error) {
        res.status(500).send({ error: error.message });
      }
    });

    // ৩. নির্দিষ্ট বাসের আইডি দিয়ে খোঁজা (ব্যাকআপ রুট)
    app.get('/api/bus/:id', async (req, res) => {
      try {
        const bus = await busesCollection.findOne({ _id: req.params.id });
        if (!bus) return res.status(404).send({ message: "Bus not found" });
        res.send(bus);
      } catch (error) {
        res.status(500).send({ error: error.message });
      }
    });

  } catch (error) {
    console.error(error);
  }
}
run().catch(console.dir);

app.get('/', (req, res) => {
  res.send('UniTransit Backend Testing Environment is Live!');
});

server.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});