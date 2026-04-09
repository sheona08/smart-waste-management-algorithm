/**
 * ttn_mqtt_client.js
 * S26-21 Smart Waste Bin - TTN MQTT Subscriber
 *
 * Connects to The Things Network, subscribes to uplink messages,
 * displays waste bin data in real-time, and saves the latest reading
 * for each bin to latest_bin_readings.json for use by the route algorithm.
 *
 * Install dependencies:
 *   npm install mqtt dotenv
 *
 * Run:
 *   node ttn_mqtt_client.js
 */

const mqtt = require('mqtt');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

// ══════════════════════════════════════════════════════════════════════════
// TTN MQTT Configuration
// ══════════════════════════════════════════════════════════════════════════

const config = {
  host: process.env.HOST_NAME || 'nam1.cloud.thethings.network',
  port: parseInt(process.env.PORT_NUMBER, 10) || 1883,
  username: process.env.USER_NAME,  // e.g., 's26-21@ttn'
  password: process.env.API_KEY,    // API key from TTN Console
  topic: process.env.TOPIC || '#'   // e.g., v3/app@ttn/devices/device-id/up
};

const OUTPUT_FILE = path.join(__dirname, 'latest_bin_readings.json');

// Validate required environment variables
if (!config.username || !config.password) {
  console.error('ERROR: Missing required environment variables.');
  console.error('Please create a .env file with:');
  console.error('  HOST_NAME=nam1.cloud.thethings.network');
  console.error('  PORT_NUMBER=1883');
  console.error('  USER_NAME=your-app-id@ttn');
  console.error('  API_KEY=your-api-key');
  console.error('  TOPIC=#');
  process.exit(1);
}

// ══════════════════════════════════════════════════════════════════════════
// MQTT Client Setup
// ══════════════════════════════════════════════════════════════════════════

const client = mqtt.connect({
  host: config.host,
  port: config.port,
  protocol: 'mqtt',
  username: config.username,
  password: config.password
});

// ══════════════════════════════════════════════════════════════════════════
// File Helpers
// ══════════════════════════════════════════════════════════════════════════

function loadLatestReadings() {
  try {
    if (fs.existsSync(OUTPUT_FILE)) {
      const raw = fs.readFileSync(OUTPUT_FILE, 'utf8');
      return raw.trim() ? JSON.parse(raw) : {};
    }
  } catch (err) {
    console.error('ERROR: Could not read latest_bin_readings.json:', err.message);
  }
  return {};
}

function saveLatestReadings(data) {
  try {
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(data, null, 2), 'utf8');
  } catch (err) {
    console.error('ERROR: Could not write latest_bin_readings.json:', err.message);
  }
}

// ══════════════════════════════════════════════════════════════════════════
// Helper Functions
// ══════════════════════════════════════════════════════════════════════════

function formatTimestamp(isoString) {
  if (!isoString) return 'unknown';
  const date = new Date(isoString);

  if (Number.isNaN(date.getTime())) {
    return 'invalid timestamp';
  }

  return date.toLocaleString('en-US', {
    month: '2-digit',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  });
}

function getStatusEmoji(status) {
  const emojis = {
    healthy: '✅',
    degraded: '⚠️',
    warning: '⚠️',
    critical: '🚨'
  };
  return emojis[status] || '❓';
}

function printDivider(char = '─', length = 60) {
  console.log(char.repeat(length));
}

function saveReadingForAlgorithm({
  binId,
  fillPercent,
  batteryPercent,
  status,
  errors,
  warnings,
  time,
  deviceId,
  appId
}) {
  if (binId === undefined || binId === null) {
    console.log('⚠️  Skipping save: bin_id missing');
    return;
  }

  const latest = loadLatestReadings();

  latest[String(binId)] = {
    bin_id: binId,
    fill_percent: fillPercent,
    battery_percent: batteryPercent,
    status: status,
    errors: errors,
    warnings: warnings,
    received_at: time,
    device_id: deviceId,
    application_id: appId
  };

  saveLatestReadings(latest);
}

function displayWasteBinData(payload) {
  try {
    const time = payload.received_at || new Date().toISOString();
    const deviceId = payload.end_device_ids?.device_id || 'unknown';
    const appId = payload.end_device_ids?.application_ids?.application_id || 'unknown';

    const data = payload.uplink_message?.decoded_payload;

    if (!data) {
      console.log('⚠️  Received message without decoded payload');
      return;
    }

    // Extract waste bin data
    const binId = data.bin_id;
    const fillPercent = data.fill_percent;
    const batteryPercent = data.battery_percent;
    const status = data.status || 'unknown';
    const errors = data.errors || {};
    const warnings = data.warnings || [];

    // Save latest reading for route algorithm
    saveReadingForAlgorithm({
      binId,
      fillPercent,
      batteryPercent,
      status,
      errors,
      warnings,
      time,
      deviceId,
      appId
    });

    // Display formatted output
    console.log('\n');
    printDivider('═');
    console.log(`${getStatusEmoji(status)} WASTE BIN DATA RECEIVED ${getStatusEmoji(status)}`);
    printDivider('═');
    console.log(`Time:        ${formatTimestamp(time)}`);
    console.log(`Device:      ${deviceId}`);
    console.log(`Application: ${appId}`);
    printDivider();
    console.log(`Bin ID:      ${binId}`);
    console.log(`Fill Level:  ${fillPercent}%`);
    console.log(`Battery:     ${batteryPercent}%`);
    console.log(`Status:      ${String(status).toUpperCase()}`);
    console.log(`Saved To:    ${OUTPUT_FILE}`);

    // Display error flags if any
    if (
      errors.fallback_used ||
      errors.low_confidence ||
      errors.sensor_timeout ||
      errors.high_rejection
    ) {
      printDivider();
      console.log('Error Flags:');
      if (errors.fallback_used) console.log('  🚨 Fallback mode (all sensors rejected)');
      if (errors.low_confidence) console.log('  ⚠️  Low confidence (only 1 sensor used)');
      if (errors.sensor_timeout) console.log('  ⚠️  Sensor timeout occurred');
      if (errors.high_rejection) console.log('  ⚠️  High rejection rate (2+ sensors)');
    }

    // Display warnings if any
    if (warnings.length > 0) {
      printDivider();
      console.log('Warnings:');
      warnings.forEach((warning) => {
        console.log(`  ⚠️  ${warning}`);
      });
    }

    printDivider('═');
    console.log('\n');
  } catch (error) {
    console.error('ERROR: Failed to parse message:', error.message);
  }
}

// ══════════════════════════════════════════════════════════════════════════
// MQTT Event Handlers
// ══════════════════════════════════════════════════════════════════════════

client.on('connect', () => {
  console.log('\n✅ Connected to TTN MQTT broker');
  console.log(`   Host: ${config.host}:${config.port}`);
  console.log(`   User: ${config.username}`);
  console.log('\n📡 Subscribing to uplink messages...');

  client.subscribe(config.topic, (err) => {
    if (err) {
      console.error('ERROR: Failed to subscribe:', err);
      process.exit(1);
    }
    console.log(`✅ Subscribed to topic: ${config.topic}`);
    console.log('\n⏳ Waiting for waste bin data...\n');
    printDivider('═', 60);
  });
});

client.on('message', (topic, message) => {
  try {
    const payload = JSON.parse(message.toString());

    // Filter for uplink messages only
    if (payload.uplink_message) {
      displayWasteBinData(payload);
    }
  } catch (error) {
    console.error('ERROR: Failed to parse MQTT message:', error.message);
  }
});

client.on('error', (error) => {
  console.error('MQTT Error:', error.message);
  process.exit(1);
});

client.on('close', () => {
  console.log('\n❌ Disconnected from TTN MQTT broker');
});

// ══════════════════════════════════════════════════════════════════════════
// Graceful Shutdown
// ══════════════════════════════════════════════════════════════════════════

process.on('SIGINT', () => {
  console.log('\n\n⏹️  Shutting down gracefully...');
  client.end();
  process.exit(0);
});

// ══════════════════════════════════════════════════════════════════════════
// Startup Message
// ══════════════════════════════════════════════════════════════════════════

console.log('\n╔═══════════════════════════════════════════════════════════╗');
console.log('║   S26-21 Smart Waste Bin - TTN MQTT Client               ║');
console.log('╚═══════════════════════════════════════════════════════════╝');
console.log('\nConnecting to TTN...');