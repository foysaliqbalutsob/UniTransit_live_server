# Backend

This backend serves as the communication bridge between the ESP32 device and the mobile application. It receives GPS coordinates sent by the ESP32, stores the latest location, and provides an API for the frontend to fetch and display the bus's real-time position.

---

## Features

- Receive GPS coordinates from ESP32
- Store the latest bus location
- Provide REST API endpoints
- Lightweight and easy to deploy
- Supports real-time location updates

---

## API Endpoints

### Update Bus Location

```http
POST /location
```

**Request Body**

```json
{
  "latitude": 23.8103,
  "longitude": 90.4125
}
```

**Response**

```json
{
  "message": "Location updated successfully."
}
```

---

### Get Current Bus Location

```http
GET /location
```

**Response**

```json
{
  "latitude": 23.8103,
  "longitude": 90.4125
}
```

---

## Project Structure

```
Backend/
│
├── server.js
├── routes/
├── controllers/
├── package.json
└── README.md
```

---

## Getting Started

### Install Dependencies

```bash
npm install
```

### Run the Server

```bash
npm start
```

The backend will start on:

```
http://localhost:3000
```

---

## Data Flow

```
ESP32 + GPS
      │
      ▼
HTTP POST
      │
      ▼
Backend Server
      │
      ▼
REST API
      │
      ▼
Mobile Application
```

---

## Future Improvements

- Authentication
- Database integration
- Location history
- WebSocket support
- Multiple bus tracking
- Cloud deployment