/**
 * Test script: Backend stops with GPS coordinates
 * This demonstrates the expected backend payload with GPS data
 */

const sampleBackendPayload = {
  item: {
    incident: {
      incident_id: `TEST_GPS_${Date.now()}`,
      vehicle_id: 'TRUCK_001',
      event_type: 'TRAFFIC_JAM',
      severity: 'MEDIUM',
      timestamp: new Date().toISOString(),
      location: { lat: 47.6098, lon: -122.201 },
      gps_speed: 45,
      engine_status: 'ON',
      dtc_codes: [],
      eta_impact_min: 15,
      requires_reorder: true,
      reason: 'Heavy traffic on I-5 North',
      description: 'Traffic congestion detected with GPS coordinates'
    },
    vehicle_status: {
      vehicle_id: 'TRUCK_001',
      status: 'ON_ROUTE',
      last_location: { lat: 47.6098, lon: -122.201 },
      last_update_at: new Date().toISOString()
    },
    original_task_sequence: [
      {
        task_id: 'BACKEND_STOP_1',
        order: 1,
        address: '123 First Ave, Seattle, WA',
        location: { lat: 47.6283, lon: -122.1833 },  // GPS coordinates
        planned_time_start: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 3
      },
      {
        task_id: 'BACKEND_STOP_2',
        order: 2,
        address: '456 Second St, Seattle, WA',
        gps: { lat: 47.6356, lon: -122.1978 },  // Alternative field name
        planned_time_start: new Date(Date.now() + 45 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 2
      },
      {
        task_id: 'BACKEND_STOP_3',
        order: 3,
        address: '789 Third Rd, Seattle, WA',
        lat: 47.6412,  // Separate lat/lon fields
        lon: -122.2045,
        planned_time_start: new Date(Date.now() + 75 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 90 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 5
      },
      {
        task_id: 'BACKEND_STOP_4',
        order: 4,
        address: '321 Fourth Blvd, Seattle, WA',
        // No GPS - should show "GPS: —"
        planned_time_start: new Date(Date.now() + 105 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 120 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 1
      },
      {
        task_id: 'BACKEND_STOP_5',
        order: 5,
        address: '654 Fifth Way, Seattle, WA',
        location: { lat: 47.6543, lon: -122.2156 },  // GPS coordinates
        planned_time_start: new Date(Date.now() + 135 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 150 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 4
      }
    ]
  },
  next_after_time: new Date().toISOString(),
  next_after_id: `${Date.now()}`
};

console.log('\n📍 Testing Backend Stops with GPS Coordinates\n');
console.log('Sample backend payload includes:');
console.log(`  - Incident: ${sampleBackendPayload.item.incident.event_type}`);
console.log(`  - Task Sequence: ${sampleBackendPayload.item.original_task_sequence.length} stops\n`);

console.log('GPS Coordinates by Stop:');
sampleBackendPayload.item.original_task_sequence.forEach((task, i) => {
  const gps = task.location || task.gps || (task.lat && task.lon ? { lat: task.lat, lon: task.lon } : null);
  if (gps) {
    console.log(`  ${i + 1}. ${task.task_id}: GPS: ${gps.lat.toFixed(4)}, ${gps.lon.toFixed(4)}`);
  } else {
    console.log(`  ${i + 1}. ${task.task_id}: GPS: — (missing)`);
  }
});

console.log('\n🔍 Expected UI behavior:');
console.log('  - Stop cards show GPS coordinates in grey subtitle');
console.log('  - Format: "GPS: 47.6283, -122.1833"');
console.log('  - If GPS missing, shows: "GPS: —"');
console.log('  - Title remains as stop address (black, bold)');

console.log('\n✅ Check frontend console logs:');
console.log('  [BACKEND_STOPS] loaded N stops, sample[0]={stop_id, lat, lon}');
console.log('  Should show actual GPS coordinates from backend\n');

console.log('💡 Supported GPS field names:');
console.log('  - task.location (object with lat/lon)');
console.log('  - task.gps (object with lat/lon)');
console.log('  - task.lat + task.lon (separate fields)\n');
