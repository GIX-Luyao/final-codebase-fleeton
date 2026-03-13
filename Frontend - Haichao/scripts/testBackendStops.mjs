/**
 * Test script to send backend event with original_task_sequence
 * This simulates the backend sending stops/task data with an incident
 */

const API_URL = 'https://15eqsal673.execute-api.us-west-2.amazonaws.com/event';

// Sample backend event with original_task_sequence
const sampleEvent = {
  item: {
    incident: {
      incident_id: `TEST_STOPS_${Date.now()}`,
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
      description: 'Traffic congestion detected, rerouting recommended'
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
        planned_time_start: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 30 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 3
      },
      {
        task_id: 'BACKEND_STOP_2',
        order: 2,
        address: '456 Second St, Seattle, WA',
        planned_time_start: new Date(Date.now() + 45 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 60 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 2
      },
      {
        task_id: 'BACKEND_STOP_3',
        order: 3,
        address: '789 Third Rd, Seattle, WA',
        planned_time_start: new Date(Date.now() + 75 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 90 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 5
      },
      {
        task_id: 'BACKEND_STOP_4',
        order: 4,
        address: '321 Fourth Blvd, Seattle, WA',
        planned_time_start: new Date(Date.now() + 105 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + 120 * 60 * 1000).toISOString(),
        status: 'PENDING',
        package_count: 1
      },
      {
        task_id: 'BACKEND_STOP_5',
        order: 5,
        address: '654 Fifth Way, Seattle, WA',
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

console.log('\n📦 Testing Backend Stops Integration\n');
console.log('Event will include:');
console.log(`  - Incident: ${sampleEvent.item.incident.event_type} (${sampleEvent.item.incident.severity})`);
console.log(`  - Vehicle Status: ${sampleEvent.item.vehicle_status.vehicle_id}`);
console.log(`  - Task Sequence: ${sampleEvent.item.original_task_sequence.length} stops`);
console.log('\nFirst 2 stops:');
sampleEvent.item.original_task_sequence.slice(0, 2).forEach((task, i) => {
  console.log(`  ${i + 1}. ${task.task_id} - ${task.address} (order: ${task.order})`);
});

console.log('\n🔍 What to check in frontend console:');
console.log('  1. [LONG_POLL] message showing taskCount > 0');
console.log('  2. [BACKEND_STOPS] log with mapped stop count');
console.log('  3. Task List / Manifest page shows backend stops instead of mock data');
console.log('  4. Stop addresses match backend data (e.g., "123 First Ave, Seattle, WA")');

console.log('\n✅ Sample event data prepared!');
console.log('\n💡 Note: This is a sample event structure.');
console.log('   To test: Send this structure via your backend API endpoint.');
console.log(`   The long-polling hook will receive it and map stops automatically.\n`);
