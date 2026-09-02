const express = require('express');
const { Kafka, Partitioners } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const KAFKA_BROKERS = process.env.KAFKA_BROKERS || 'my-kafka-cluster-kafka-brokers.kafka-dev.svc.cluster.local:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'events';
const ENABLE_AUTO_EVENTS = process.env.ENABLE_AUTO_EVENTS === 'true';

const kafka = new Kafka({
  clientId: 'my-producer',
  brokers: KAFKA_BROKERS.split(','),
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

producer.connect().then(() => {
  console.log('✅ Producer connected to Kafka');
}).catch(err => {
  console.error('❌ Failed to connect to Kafka:', err);
  process.exit(1);
});

// 🔴 Health check endpoint (GET)
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', timestamp: new Date().toISOString() });
});

// 🔴 Endpoint /send-event (POST)
app.post('/send-event', async (req, res) => {
  try {
    const event = req.body;

    if (!event.eventId) {
      event.eventId = uuidv4();
    }
    if (!event.timestamp) {
      event.timestamp = new Date().toISOString();
    }

    await producer.send({
      topic: TOPIC,
      messages: [{ value: JSON.stringify(event) }],
    });

    console.log('📤 Event sent:', event.eventId);
    res.status(200).json({
      message: 'Event sent successfully',
      event
    });
  } catch (error) {
    console.error('❌ Error sending event:', error);
    res.status(500).json({ error: error.message });
  }
});

// 🔴 Generador automático de eventos (controlado por variable)
if (ENABLE_AUTO_EVENTS) {
  console.log('🔄 Auto-events enabled');
  setInterval(async () => {
    try {
      const event = {
        eventId: uuidv4(),
        customerId: `customer-${Math.floor(Math.random() * 5) + 1}`,
        type: ['ORDER_CREATED', 'ORDER_UPDATED', 'PAYMENT_RECEIVED'][Math.floor(Math.random() * 3)],
        amount: parseFloat((Math.random() * 1000).toFixed(2)),
        timestamp: new Date().toISOString()
      };

      await producer.send({
        topic: TOPIC,
        messages: [{ value: JSON.stringify(event) }],
      });

      console.log('📤 Auto-event sent:', event.eventId);
    } catch (error) {
      console.error('❌ Error sending auto-event:', error);
    }
  }, 3000);
} else {
  console.log('⏸️ Auto-events disabled (ENABLE_AUTO_EVENTS=false)');
}

const shutdown = async () => {
  console.log('🛑 Shutting down...');
  await producer.disconnect();
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 Producer API listening on port ${PORT}`);
});