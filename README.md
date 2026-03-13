[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/Z_kNVedj)

# Fleeton - Real-Time Delivery Rerouting System

Fleeton is an end-to-end delivery optimization system that detects delivery exceptions from real-time vehicle data and dynamically recalculates delivery routes to reduce delays and improve ETA reliability.

## System Architecture

- Backend: responsible for vehicle data ingestion, event detection, ETA impact analysis, and dynamic route recalculation.
- Frontend: driver-facing application for real-time vehicle location, task list, alerts, and rerouting decisions.
- Backend and Frontend communicate through HTTP APIs and long-polling.

## Project Structure

- `Backend - Lily/`: backend services, routing engine, and database-related modules.
- `Frontend - Haichao/`: driver app frontend (React + TypeScript).
- `README.md`: project overview and documentation entry point.

## Frontend

Frontend driver application responsible for:

- visualizing vehicle telemetry and delivery stops
- receiving backend event updates
- displaying alerts and reroute suggestions
- map-based route visualization

Frontend implementation is located in:

`Frontend - Haichao/`

---

# Backend

## Backend Architecture

The backend system processes vehicle telemetry data, detects delivery incidents, and dynamically recalculates delivery routes to reduce delays.

The overall data flow is:

1. Vehicles collect telemetry data (GPS, speed, engine status).
2. **AWS API Gateway + Lambda** ingest the telemetry data.
3. Events are processed and stored in **PostgreSQL/PostGIS**.
4. If an incident impacts delivery ETA, the **Routing Engine (OSRM)** recalculates the route.
5. Updated routes and ETA are pushed to the **mobile app / dispatcher dashboard**.

Key technologies used:

- AWS Lambda
- AWS API Gateway
- PostgreSQL + PostGIS
- Redis (real-time cache)
- OSRM routing engine
- WebSocket API for live updates

This system collects vehicle telemetry, detects incidents using lightweight rule-based logic, processes events in the cloud, dynamically re-routes vehicles using a routing engine, and pushes updates back to drivers and dispatchers in near real time.

---

### Main Modules

**Ingestion Service**

- Receives telemetry data from vehicles simulator
- Validates payload
- Stores raw incidents in the database

**Event Processor**

- Analyzes incoming events
- Calculates ETA impact
- Determines whether rerouting is needed

**Routing Engine**

- Calls OSRM routing service
- Computes optimized delivery sequence
- Returns updated route and ETA

**Notification Service**

- Sends route updates and alerts to the frontend via HTTP API

---

# Database Design

The backend uses **PostgreSQL with PostGIS** to support spatial queries.

Core tables:

- `vehicles`
- `delivery_plans`
- `delivery_stops`
- `incidents`
- `stop_resequence_results`

Example schema relationships:

```
vehicles
   │
   ├── delivery_plans
   │       │
   │       └── delivery_stops
   │
   └── incidents
           │
           └── stop_resequence_results
```

Key data stored:

| Table | Purpose |
|------|------|
| vehicles | Vehicle status and latest location |
| delivery_plans | Delivery schedules |
| delivery_stops | Stop order and ETA windows |
| incidents | Traffic, breakdown, or delay events |
| stop_resequence_results | Optimized route sequence |



---


# Setup and Installation

## 1. Clone Repository

```bash
git clone https://github.com/your-repo/project-name.git
cd backend

## 2. Install Dependencies

Main dependencies:

- psycopg2  
- redis  
- requests  
- fastapi  
- uvicorn  

---

## 3. Configure Environment Variables

Create a `.env` file.

```env
DB_HOST=xxxx
DB_PORT=5432
DB_NAME=delivery_demo
DB_USER=postgres
DB_PASSWORD=password

REDIS_HOST=xxxx
REDIS_PORT=6379

ROUTING_ENGINE_URL=http://localhost:5000
```

---

## 4. Initialize Database

Run the schema:

```bash
psql -U postgres -d delivery_demo -f database/schema.sql
```

Optional seed data:

```bash
psql -U postgres -d delivery_demo -f database/seed_data.sql
```

---

# Running the Backend

Start the API server:

```bash
uvicorn api.routes:app --reload
```

Server will start at:

```
http://localhost:8000
```

---

# Testing the System

You can simulate telemetry data using **curl** or **Postman**.

Example request:

```bash
curl -X POST http://localhost:8000/ingest \
-H "Content-Type: application/json" \
-d '{
  "vehicle_id": "vehicle_001",
  "timestamp": "2026-03-01T10:00:00Z",
  "location": {
    "lat": 47.6062,
    "lon": -122.3321
  },
  "event_type": "TRAFFIC_JAM",
  "GPS_speed": 1.5,
  "CAN_speed": 1.2
}'
```

---

# Expected System Behavior

After executing the system:

1. Telemetry data is ingested and stored in the database.

2. The event processor evaluates whether the event impacts delivery ETA.

3. If ETA impact exceeds the threshold:

```
ETA_delta / ETA_current > 10%
```

the system triggers the routing engine.

4. The routing engine calculates:

- new delivery sequence  
- updated ETA  
- optimized route polyline  

5. The results are stored in:

```
stop_resequence_results
```

6. Updated route information is pushed to the frontend dashboard.

The frontend can retrieve:

- vehicle location  
- incident type  
- severity  
- suggested route  
- updated ETA  

---

# Demo Notes

For the demo environment:

- Some telemetry data is simulated  
- OSRM routing runs locally or on EC2  
- Vehicle telemetry is generated by a simulator rather than real vehicles  

This allows safe testing of the full pipeline:

```
incident detection → ETA analysis → route optimization → real-time updates
```