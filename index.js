const mqtt = require('mqtt')
const { MongoClient, ServerApiVersion } = require('mongodb');
require('dotenv').config();


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


const protocol = 'mqtts'
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
            documentId = obj.tripId
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
