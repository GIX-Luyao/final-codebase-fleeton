#!/usr/bin/env node

const baseUrl = 'https://15eqsal673.execute-api.us-west-2.amazonaws.com/event';
const params = new URLSearchParams({
  vehicle_id: 'TRUCK_001',
  after_time: new Date().toISOString(),
  after_id: '0',
  wait_s: '20',
});

const url = `${baseUrl}?${params}`;
console.log(`\n🔗 Request URL:\n${url}\n`);

try {
  const response = await fetch(url);
  console.log(`📊 Status: ${response.status} ${response.statusText}`);

  const text = await response.text();
  console.log(`📝 Raw Response:\n${text}\n`);

  if (!response.ok) {
    console.error(`❌ Response not ok (${response.status})`);
    process.exit(1);
  }

  const data = JSON.parse(text);
  console.log(`✅ Parsed JSON:\n${JSON.stringify(data, null, 2)}\n`);

  if (data.item) {
    console.log(`🎯 Event found: ${JSON.stringify(data.item)}`);
  } else {
    console.log(`⏳ No event (timeout or empty): item is null`);
  }

  process.exit(0);
} catch (error) {
  console.error(`❌ Error: ${error.message}`);
  process.exit(1);
}
