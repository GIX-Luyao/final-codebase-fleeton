import type { DeliveryStop } from '../app/fleetTypes';

// TODO(DEMO_ROUTE): deterministic demo route constants for frontend-only stops control
const DEFAULT_VEHICLE_ID = 'TRUCK_001';
const DEFAULT_PLAN_ID = 'P001';
export const ROUTE_VARIANTS = ['A', 'B'] as const;

export type DemoRouteVariant = (typeof ROUTE_VARIANTS)[number];

type DemoStopSeed = {
  stop_id: string;
  address: string;
  location: { lat: number; lon: number };
  planned_time_start: string;
  planned_time_end: string;
};

// TODO(DEMO_ROUTE): fixed 10-stop seed copied from FleetProvider generateMockStops10 realStops
const DEMO_STOP_SEEDS: DemoStopSeed[] = [
  {
    stop_id: 'STOP_001',
    address: 'Seattle City Hall, 600 4th Ave, Seattle, WA 98104',
    location: { lat: 47.6062, lon: -122.3321 },
    planned_time_start: '2026-01-27T07:30:00Z',
    planned_time_end: '2026-01-27T07:45:00Z',
  },
  {
    stop_id: 'STOP_002',
    address: '2nd Ave & Pike St (Downtown Seattle), Seattle, WA 98101',
    location: { lat: 47.6098, lon: -122.3321 },
    planned_time_start: '2026-01-27T07:45:00Z',
    planned_time_end: '2026-01-27T08:00:00Z',
  },
  {
    stop_id: 'STOP_003',
    address: 'Pike Place Market, 85 Pike St, Seattle, WA 98101',
    location: { lat: 47.6097, lon: -122.3397 },
    planned_time_start: '2026-01-27T08:00:00Z',
    planned_time_end: '2026-01-27T08:15:00Z',
  },
  {
    stop_id: 'STOP_004',
    address: 'Seattle Center, 305 Harrison St, Seattle, WA 98109',
    location: { lat: 47.6205, lon: -122.3493 },
    planned_time_start: '2026-01-27T08:15:00Z',
    planned_time_end: '2026-01-27T08:30:00Z',
  },
  {
    stop_id: 'STOP_005',
    address: 'Kerry Park, 211 W Highland Dr, Seattle, WA 98119',
    location: { lat: 47.6295, lon: -122.3599 },
    planned_time_start: '2026-01-27T08:30:00Z',
    planned_time_end: '2026-01-27T08:45:00Z',
  },
  {
    stop_id: 'STOP_006',
    address: 'Fremont Troll, N 36th St, Seattle, WA 98103',
    location: { lat: 47.6511, lon: -122.3472 },
    planned_time_start: '2026-01-27T08:45:00Z',
    planned_time_end: '2026-01-27T09:00:00Z',
  },
  {
    stop_id: 'STOP_007',
    address: 'Seattle-Tacoma International Airport (SEA), 17801 International Blvd, SeaTac, WA 98158',
    location: { lat: 47.4502, lon: -122.3088 },
    planned_time_start: '2026-01-27T09:00:00Z',
    planned_time_end: '2026-01-27T09:15:00Z',
  },
  {
    stop_id: 'STOP_008',
    address: 'Fife / Milton area (east of Tacoma), WA (approx for lat=47.2396, lon=-122.3572)',
    location: { lat: 47.2396, lon: -122.3572 },
    planned_time_start: '2026-01-27T09:15:00Z',
    planned_time_end: '2026-01-27T09:30:00Z',
  },
  {
    stop_id: 'STOP_009',
    address: 'Downtown Tacoma / Museum District (approx near 1701 Pacific Ave, Tacoma, WA 98402)',
    location: { lat: 47.2529, lon: -122.4443 },
    planned_time_start: '2026-01-27T09:30:00Z',
    planned_time_end: '2026-01-27T09:45:00Z',
  },
  {
    stop_id: 'STOP_010',
    address: 'Tacoma Dome, 2727 East D St, Tacoma, WA 98421',
    location: { lat: 47.2396, lon: -122.3572 },
    planned_time_start: '2026-01-27T09:45:00Z',
    planned_time_end: '2026-01-27T10:00:00Z',
  },
];

// TODO(DEMO_ROUTE): deterministic route order maps
const ROUTE_A_ORDER: Record<string, number> = {
  STOP_001: 1,
  STOP_002: 2,
  STOP_003: 3,
  STOP_004: 4,
  STOP_005: 5,
  STOP_006: 6,
  STOP_007: 7,
  STOP_008: 8,
  STOP_009: 9,
  STOP_010: 10,
};

const ROUTE_B_ORDER: Record<string, number> = {
  STOP_005: 1,
  STOP_001: 2,
  STOP_002: 3,
  STOP_003: 4,
  STOP_004: 5,
  STOP_006: 6,
  STOP_007: 7,
  STOP_008: 8,
  STOP_009: 9,
  STOP_010: 10,
};

function getRouteOrderMap(variant: DemoRouteVariant) {
  return variant === 'A' ? ROUTE_A_ORDER : ROUTE_B_ORDER;
}

function getInProgressStopId(variant: DemoRouteVariant) {
  return variant === 'A' ? 'STOP_001' : 'STOP_005';
}

function buildDemoStops(variant: DemoRouteVariant): DeliveryStop[] {
  const routeOrderMap = getRouteOrderMap(variant);
  const inProgressStopId = getInProgressStopId(variant);

  return DEMO_STOP_SEEDS.map((seed) => {
    const sequence = routeOrderMap[seed.stop_id] ?? 999;

    return {
      stop_id: seed.stop_id,
      plan_id: DEFAULT_PLAN_ID,
      vehicle_id: DEFAULT_VEHICLE_ID,
      original_sequence: sequence,
      current_sequence: sequence,
      address: seed.address,
      planned_time_start: seed.planned_time_start,
      planned_time_end: seed.planned_time_end,
      current_time_start: null,
      current_time_end: null,
      status: seed.stop_id === inProgressStopId ? 'IN_PROGRESS' : 'PENDING',
      package_count: 3,
      stop_order: sequence,
      eta: seed.planned_time_start,
      location: { ...seed.location },
      gps: { ...seed.location },
      lat: seed.location.lat,
      lon: seed.location.lon,
    };
  }).sort((a, b) => a.current_sequence - b.current_sequence);
}

export const ROUTE_A_STOPS: DeliveryStop[] = buildDemoStops('A');
export const ROUTE_B_STOPS: DeliveryStop[] = buildDemoStops('B');

// TODO(DEMO_ROUTE): exported deterministic stop source used by FleetProvider/hook
export function getDemoStops(variant: DemoRouteVariant): DeliveryStop[] {
  return (variant === 'A' ? ROUTE_A_STOPS : ROUTE_B_STOPS).map((stop) => ({
    ...stop,
    location: stop.location ? { ...stop.location } : undefined,
    gps: stop.gps ? { ...stop.gps } : undefined,
  }));
}
