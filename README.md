# S26-21 TTN MQTT Client

Simple JavaScript MQTT client that subscribes to The Things Network and displays waste bin sensor data in real-time.

## 📋 Prerequisites

- Node.js (v14 or higher) - [Download here](https://nodejs.org/)
- TTN account with configured application
- API key from TTN Console

## 🚀 Quick Start

### Step 1: Install Dependencies

```bash
npm install
```

This installs:
- `mqtt` - MQTT client library
- `dotenv` - Environment variable management

### Step 2: Configure TTN Credentials

1. Copy the template file:
   ```bash
   cp .env.template .env
   ```

2. Get your credentials from TTN Console:
   - Go to: TTN Console → Your Application → Integrations → MQTT
   - Copy the values shown

3. Edit `.env` file with your credentials:
   ```env
   HOST_NAME=nam1.cloud.thethings.network
   PORT_NUMBER=1883
   USER_NAME=s26-21@ttn
   API_KEY=NNSXS.XXXXXXXXXXXXXXXXXXXXXXXXX
   TOPIC=#
   ```

   **Where to find these values:**
   - `HOST_NAME`: Public address (usually `nam1.cloud.thethings.network`)
   - `PORT_NUMBER`: Use `1883` (standard MQTT port)
   - `USER_NAME`: Your application ID + `@ttn`
   - `API_KEY`: Click "Generate new API key" in TTN Console
   - `TOPIC`: Use `#` to subscribe to all uplink messages

### Step 3: Run the Client

```bash
npm start
```

Or directly:
```bash
node ttn_mqtt_client.js
```

## 📊 Example Output

When waste bin data is received, you'll see:

```
═══════════════════════════════════════════════════════════
✅ WASTE BIN DATA RECEIVED ✅
═══════════════════════════════════════════════════════════
Time:        03/30/2026, 12:45:30
Device:      s26-21-sensor-node-01
Application: s26-21
────────────────────────────────────────────────────────────
Bin ID:      1
Fill Level:  45%
Battery:     100%
Status:      HEALTHY
═══════════════════════════════════════════════════════════
```

### Example with Warnings:

```
═══════════════════════════════════════════════════════════
⚠️  WASTE BIN DATA RECEIVED ⚠️
═══════════════════════════════════════════════════════════
Time:        03/30/2026, 12:47:15
Device:      s26-21-sensor-node-01
Application: s26-21
────────────────────────────────────────────────────────────
Bin ID:      1
Fill Level:  82%
Battery:     45%
Status:      WARNING
────────────────────────────────────────────────────────────
Error Flags:
  ⚠️  Low confidence (only 1 sensor used)
  ⚠️  Sensor timeout occurred
────────────────────────────────────────────────────────────
Warnings:
  ⚠️  Only 1 sensor used — low confidence result
  ⚠️  Sensor timeout detected
═══════════════════════════════════════════════════════════
```

## 📡 Data Structure

The client expects this decoded payload from TTN:

```json
{
  "bin_id": 1,
  "fill_percent": 45,
  "battery_percent": 100,
  "status": "healthy",
  "errors": {
    "fallback_used": false,
    "low_confidence": false,
    "sensor_timeout": false,
    "high_rejection": false
  },
  "warnings": []
}
```

## 🎨 Status Indicators

| Status | Emoji | Meaning |
|--------|-------|---------|
| healthy | ✅ | All systems normal |
| degraded | ⚠️  | Minor issues (timeouts) |
| warning | ⚠️  | Some sensors rejected or low confidence |
| critical | 🚨 | Fallback mode active |

## 🛑 Stopping the Client

Press `Ctrl+C` to gracefully disconnect and exit.

## 🔧 Troubleshooting

### Connection Failed

**Error:** `ERROR: Failed to subscribe`

**Solutions:**
1. Verify `USER_NAME` is correct (should be `your-app-id@ttn`)
2. Check `API_KEY` - regenerate if needed
3. Ensure you're using the correct TTN cluster (nam1 for North America)

### No Data Received

**Problem:** Client connects but no messages appear

**Solutions:**
1. Verify your device is transmitting (check TTN Live Data)
2. Check device is sending to the correct application
3. Ensure payload formatter is configured on TTN
4. Try changing `TOPIC` to specific device: `v3/your-app@ttn/devices/device-id/up`

### Authentication Failed

**Error:** MQTT connection rejected

**Solutions:**
1. Regenerate API key in TTN Console
2. Update `.env` file with new key
3. Ensure no extra spaces in credentials

## 📝 Notes

- The client automatically filters for uplink messages only
- Messages are displayed in real-time as they arrive
- No data is logged to files (console output only)
- Safe to run 24/7 for continuous monitoring

## 🔗 Related Files

- `ttn_mqtt_client.js` - Main client code
- `package.json` - Node.js dependencies
- `.env.template` - Configuration template
- `.env` - Your credentials (not tracked in git)

## 📚 Additional Resources

- [TTN MQTT Documentation](https://www.thethingsindustries.com/docs/integrations/mqtt/)
- [Paho MQTT.js Documentation](https://github.com/mqttjs/MQTT.js)
