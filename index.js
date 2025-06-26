const mqtt = require('mqtt')
const { MongoClient, ServerApiVersion, ObjectId } = require('mongodb');
require('dotenv').config();

const express = require('express');
const app = express();
app.use(express.json());

app.get('/', function (req, res) {
    res.send("Alive!")
});

app.get('/trips/:tripUUID/data/distance', async function (req, res) {
    let tripUUID = req.params.tripUUID;

    try {
        if(database == null) throw new Error(`Database not connected`);

        let collectionName = 'OdbData'
        const collection = database.collection(collectionName);

        const documents = await collection
            .find({ tripId: tripUUID })
            .sort({ timestamp: 1 })
            .toArray();

        if (documents.length === 0) {
            return res.status(404).json({ error: 'No data for given trip UUID' });
        }

        const coords = documents.map(doc => {
            const lat = doc.locationData.latitude;
            const lon = doc.locationData.longitude;
            return [lon, lat];
        });
        
        let downsampledCoords = downsampleCoordinates(coords)

        let payload = {
            coordinates: downsampledCoords
        }

        // OpenRouteService API
        const orsResponse = await fetch('https://api.openrouteservice.org/v2/directions/driving-car', {
            method: 'POST',
            headers: {
                'Authorization': process.env.OPENROUTE_SERVICE_API_KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });

        if (!orsResponse.ok) {
            throw new Error(`ORS API error: ${orsResponse.status}`);
        }

        const orsData = await orsResponse.json();

        let distance = null;

        if (orsData.routes && orsData.routes.length > 0 && orsData.routes[0].summary.distance) {
            distance = parseInt(orsData.routes[0].summary.distance);
        }

        res.json({
            tripUUID: tripUUID,
            distance: distance
         });

    } catch (error) {
        res.status(500).json({ error: 'Error while contacting OpenRouteService API' });
    }
});

app.get('/trips/:tripUUID/data/coordinates', async function (req, res) {
    let tripUUID = req.params.tripUUID;

    try {
        if (database == null) throw new Error(`Database not connected`);

        const collectionName = 'OdbData';
        const collection = database.collection(collectionName);

        const documents = await collection
            .find({ tripId: tripUUID })
            .sort({ timestamp: 1 })
            .toArray();

        if (documents.length === 0) {
            return res.status(404).json({ error: 'No data for given trip UUID' });
        }

        // Hack to remove duplacate timestamps and have clean coordinates
        // TODO: remove once the timestamp issue is fixed
        const seenTimestamps = new Set();
        const uniqueDocuments = documents.filter(doc => {
            const ts = doc.timestamp?.$numberLong ?? doc.timestamp;
            if (seenTimestamps.has(ts)) {
                return false;
            } else {
                seenTimestamps.add(ts);
                return true;
            }
        });

        const coordinates = uniqueDocuments.map(doc => {
            const lat = doc.locationData.latitude
            const lon = doc.locationData.longitude
            return {
                latitude: lat,
                longitude: lon
            };
        });

        res.json({
            tripUUID: tripUUID,
            coordinates: coordinates
        });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Error while fetching coordinates from database' });
    }
});

app.get('/trips/:tripId/data/details', async (req, res) => {
    const tripId = req.params.tripId;

    try {
        if (!database) throw new Error('Database not connected');
  
        const collTrip = database.collection('TripSummary');
        const trip = await collTrip.findOne({ "Trip ID": tripId });
  
        if (!trip) return res.status(404).json({ error: 'Trip not found' });
  
        const distance = parseFloat(trip["Travel Distance (km)"]);
        const duration = parseFloat(trip["Trip Duration (min)"]);
        const avgSpeed = parseFloat(trip["Average Speed (km/h)"]);
        const maxSpeed = parseFloat(trip["Max Speed (km/h)"]);
        const avgRpm = parseFloat(trip["Average RPM"]);
        const maxRpm = parseInt(trip["Max RPM"], 10);
        const rapidAccel = parseInt(trip["Rapid Accelerations"], 10);
        const hardDecel = parseInt(trip["Hard Decelerations"], 10);
        const stopGo = parseInt(trip["Stop-and-Go Frequency"], 10);
        const speedLimitCompliance = parseFloat(trip["Speed Limit Compliance (%)"]);
        const overSpeedDuration = parseFloat(trip["Over-Speeding Duration (%)"]);
        const idlingPct = parseFloat(trip["Idling Time (%)"]);
        const idlingTime = (idlingPct / 100) * duration;
  
        // Get first and last trip data
        const collObd = database.collection('OdbData');
        // First document (earliest timestamp)
        const startDoc = await collObd.find({ tripId }).sort({ timestamp: 1 }).limit(1).toArray();
        const first = startDoc[0];

        // Last document (latest timestamp)
        const endDoc = await collObd.find({ tripId }).sort({ timestamp: -1 }).limit(1).toArray();
        const last = endDoc[0];

        if (!first || !last) {
            return res.status(500).json({ error: 'Could not find trip boundaries (start/end)' });
        }

        const startLat = parseFloat(first.locationData.latitude);
        const startLon = parseFloat(first.locationData.longitude);
        const endLat = parseFloat(last.locationData.latitude);
        const endLon = parseFloat(last.locationData.longitude);
  
        const weather = first.weatherData;
        const weatherMain = weather.weather?.[0]?.main ?? null;
        const temperature = weather.main?.temp ?? null;
  
        const [startAddr, endAddr] = await Promise.all([
            revGeocode(startLon, startLat),
            revGeocode(endLon, endLat)
        ]);
  
        // Response
        res.json({
            tripId,
            distance,
            startAddress: startAddr,
            endAddress: endAddr,
            weather: {
                description: weatherMain,
                temperature: temperature
            },
            startTimestamp: first.timestamp.$numberLong ,
            endTimestamp: last.timestamp.$numberLong ,
            duration,
            idlingPct,
            idlingTime,
            avgSpeed,
            maxSpeed,
            avgRpm,
            maxRpm,
            drivingAboveSpeedLimit: overSpeedDuration,
            drivingWithinSpeedLimit: speedLimitCompliance,
            suddenBraking: hardDecel,
            suddenAcceleration: rapidAccel,
            stopAndGo: stopGo
        });
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Error fetching trip details' });
    }
});
  

app.post('/trips/:tripUUID/calculate/distance', async function (req, res) {
    let tripUUID = req.params.tripUUID;
    const reqData = req.body;

    // Sort by timestamp
    reqData.sort((a, b) => a.timestamp - b.timestamp);

    // Convert to list of [longitude, latitude] elements
    const coords = reqData.map(item => [item.longitude, item.latitude]);

    let downsampledCoords = downsampleCoordinates(coords)

    const payload = {
        coordinates: downsampledCoords
    };

    try {
        const response = await fetch('https://api.openrouteservice.org/v2/directions/driving-car', {
            method: 'POST',
            headers: {
                'Authorization': process.env.OPENROUTE_SERVICE_API_KEY,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });
    
        if (!response.ok) {
            // HTTP error
            throw new Error(`HTTP error! Status: ${response.status}`);
        }
    
        const responseData = await response.json();

        let distance = null;

        if (responseData.routes && responseData.routes.length > 0 && responseData.routes[0].summary.distance) {
            distance = parseInt(responseData.routes[0].summary.distance);
        }

        res.json({
            tripUUID: tripUUID,
            distance: distance
         });

    } catch (error) {
        res.status(500).json({ error: 'Error while contacting OpenRouteService API' });
    }
});

app.get('/drivers/:driverId/statistics', async function (req, res) { 
    let driverID = req.params.driverId;

    try {
        if (database == null) throw new Error(`Database not connected`);

        const collectionName = 'DriverSummary';
        const collection = database.collection(collectionName);

        const document = await collection.findOne({ Email: driverID });

        if (!document) {
            return res.status(404).json({ error: 'No statistics found for given driver ID' });
        }

        const totalDistance = parseFloat(document["Total Distance (km)"]);
        const totalDuration = parseFloat(document["Total Duration (min)"]);
        const averageSpeed = parseFloat(document["Average Speed (km/h)"]);
        const averageRpm = parseFloat(document["Average RPM"]);
        const speedLimitCompliance = parseFloat(document["Speed Limit Compliance (%)"]);
        const overSpeedDuration = parseFloat(document["Over-Speeding Duration (%)"]);
        const maxSpeed = parseInt(document["Max Speed (km/h)"]);
        const maxRpm = parseInt(document["Max RPM"]);

        res.json({
            driverId: driverID,
            totalDistance: totalDistance,
            totalDuration: totalDuration,
            averageSpeed: averageSpeed,
            averageRpm: averageRpm,
            drivingWithinSpeedLimit: speedLimitCompliance,
            drivingAboveSpeedLimit: overSpeedDuration,
            maxSpeed: maxSpeed,
            maxRpm: maxRpm
        });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'Error while fetching driver statistics from database' });
    }
});

// Express listens only local requestst. All external requests are forwarded by Nginx reverse proxy.
app.listen(4000, '127.0.0.1', function (err) {
    console.log(`Server running`)
});

function downsampleCoordinates(coordinates, maxPoints = 70) {
    const totalPoints = coordinates.length;

    if (totalPoints <= maxPoints) {
        return coordinates;
    }

    const result = [];

    // Add first point
    result.push(coordinates[0]);

    const step = (totalPoints - 2) / (maxPoints - 2);

    for (let i = 1; i < maxPoints - 1; i++) {
        const index = Math.round(i * step);
        result.push(coordinates[index]);
    }

    // Add last point
    result.push(coordinates[totalPoints - 1]);

    return result;
}

// Reverse geocode
async function revGeocode(lon, lat) {
    const url = `https://api.openrouteservice.org/geocode/reverse?api_key=${process.env.OPENROUTE_SERVICE_API_KEY}&point.lon=${lon}&point.lat=${lat}&size=1`;
    const r = await fetch(url);
    if (!r.ok) throw new Error(`Geocode error ${r.status}`);
    const body = await r.json();
    return body.features?.[0]?.properties?.label || 'Unknown address';
};

const uri = `mongodb+srv://${process.env.MONGO_USER}:${process.env.MONGO_PWD}@${process.env.MONGO_HOST}/?appName=${process.env.MONGO_CLUSTER}`;

// Create a MongoClient with a MongoClientOptions object to set the Stable API version
const mongoClient = new MongoClient(uri, {
    serverApi: {
      version: ServerApiVersion.v1,
      strict: true,
      deprecationErrors: true,
    }
});

var database = null

function connectToMongo() {
    try {
      // Connect the client to the server	(optional starting in v4.7)
      console.log("Connecting to MongoDB...")
      mongoClient.connect();
      // Send a ping to confirm a successful connection
      mongoClient.db("admin").command({ ping: 1 });
      console.log("Pinged your deployment. You successfully connected to MongoDB!");
      database = mongoClient.db("CarPulse");
    } catch(exc) {
      // Ensures that the client will close when you finish/error
      console.error('Connection to MongoDB failed', error)
      mongoClient.close();
      console.log("Closed MongoDB connection!")
    }
}

connectToMongo();


const protocol = 'mqtt'
const clientId = `mqtt_${Math.random().toString(16).slice(3)}`

const connectUrl = `${protocol}://${process.env.MQTT_HOST}:${process.env.MQTT_PORT}`

console.log("Connecting to MQTT Broker...")
const client = mqtt.connect(connectUrl, {
    clientId,
    clean: true,
    connectTimeout: 10000,
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PWD,
    reconnectPeriod: 1000,
})
  
const driverTopic = 'Auto/Drivers'
const tripTopic = 'Auto/Trips'
const driveDataTopic = 'Auto/OdbData'
const reviewTopic = 'Auto/DriversReviewTrip'
let topics = [driverTopic, tripTopic, driveDataTopic, reviewTopic]

client.on('connect', () => {
    console.log('Connected to MQTT Broker.')
    client.subscribe(topics, (topic) => {
      console.log(`Subscribe to topic: '${topic}'`)
    })
})
  
client.on('message', (topic, payload) => {
    let strMessage = payload.toString();
    let objMessage = JSON.parse(strMessage);
    console.log('Received Message:', topic, objMessage)
    saveToDatabase(topic, objMessage)
})

client.on('error', (error) => {
    console.error('Connection to MQTT Broker failed', error)
})

// we should define the _id of the insert document in order not to duplicate the same documents
async function saveToDatabase(topic, obj) {
    if(database) {
        let document
        let documentId

        if(topic === driverTopic) {
            document = obj[0]
            documentId = obj[0].Email

        } else if (topic === reviewTopic || topic === tripTopic) {
            document = obj[0]
            documentId = obj[0].tripId

        } else if (topic === driveDataTopic) {
            document = obj
            documentId = new ObjectId();
        }
        
        document._id = documentId
        console.log(document)

        const query = { _id: documentId };
        const update = { $set: document};
        const options = { upsert: true };

        let collection = topic.split("/")[1]

        try {
            await database.collection(collection).updateOne(query, update, options);
            console.log("Document successfully inserted!");
        } catch(e) {
            console.error("Cannot save to database ", e)
        }
        
    } else {
        console.log("Cannot save to database!")
    }
}
