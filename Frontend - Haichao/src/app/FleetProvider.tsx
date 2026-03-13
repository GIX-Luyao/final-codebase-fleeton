import {
  createContext,
  useContext,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import type {
  IncidentEvent,
  RoutingDecisionResult,
  TaskSequenceItem,
  VehicleRealtimeStatus,
  DeliveryStop,
  Incident,
  ResequenceResult,
} from './fleetTypes';
import { AlertPolicy, type AlertBehavior } from './AlertPolicy';
import MessageStore from './MessageStore';
import { useEventLongPoll } from './useEventLongPoll';
import { devLogger } from './logger.ts';
import { getDemoStops } from '../demo/demoStops';
import { useDemoStops, type RouteVariant } from '../hooks/useDemoStops';

const console = devLogger;

type AlertStatus = 'pending' | 'accepted' | 'dismissed';
type PageKey = 'home' | 'manifest' | 'map' | 'messages' | 'profile';

type IngestedAlert = {
  incident_id: string;
  severity: 'MEDIUM' | 'HIGH';
  source: 'live' | 'demo';
  event: IncidentEvent;
  behavior: AlertBehavior;
  traceId?: string;
  incident?: Incident;
  resequence?: ResequenceResult | null;
  routing?: RoutingDecisionResult | null;
  taskSequence?: TaskSequenceItem[] | null;
};

type FleetContextValue = {
  routeVariant: RouteVariant;
  setRouteVariant: (
    next: RouteVariant | ((prev: RouteVariant) => RouteVariant)
  ) => void;

  vehicle: VehicleRealtimeStatus;
  currentEvent: IncidentEvent | null;
  routingDecision: RoutingDecisionResult | null;
  alertStatus: AlertStatus | null;
  alertBehavior: AlertBehavior | null;
  activeAlert: {
    incident_id: string;
    severity: 'MEDIUM' | 'HIGH';
    source: 'live' | 'demo';
    event: IncidentEvent;
    behavior: AlertBehavior;
    timestamp: string;
    traceId?: string;
  } | null;
  clearActiveAlert: (reason: string) => void;
  pendingTaskSequence: TaskSequenceItem[] | null;

  stops: DeliveryStop[];
  currentStop: DeliveryStop | null;
  nextStop: DeliveryStop | null;

  currentPage: PageKey;
  setCurrentPage: (p: PageKey) => void;

  acceptRouting: () => void;
  dismissRouting: () => void;
  triggerAutoSendMessage: () => void;
  clearAutoSendMessage: () => void;
  autoSendMessage: boolean;
  scheduleAutoReply: () => void;
  pendingAutoReply: { id: string; text: string } | null;
  triggerHighSeverityDemoAlert: () => void;
  triggerMediumSeverityDemoAlert: () => void;

  // Alert pause/buffer system
  isAlertPaused: boolean;
  toggleAlertPause: () => void;
  bufferedAlerts: IngestedAlert[];

  // Live alert history
  liveAlertHistory: Array<{
    incident_id: string;
    severity: 'MEDIUM' | 'HIGH';
    event_type: string;
    timestamp: string;
    title: string;
    description?: string;
    reason?: string;
    vehicle_id?: string;
    gps_speed?: number;
    engine_status?: string;
    dtc_codes?: string[];
    eta_impact_min?: number;
  }>;
  clearLiveAlertHistory: () => void;

};

const FleetContext = createContext<FleetContextValue | undefined>(undefined);

const DEFAULT_VEHICLE_ID = 'TRUCK_001';
const DEFAULT_PLAN_ID = 'P001';
// TODO(DEMO_ROUTE): central switch for deterministic frontend-only stops demo mode
const DEMO_STOPS_MODE = true;
const LOCATION_API_URL =
  (import.meta.env.VITE_LOCATION_API_URL as string | undefined) ||
  'https://15eqsal673.execute-api.us-west-2.amazonaws.com/location';
const LOCATION_POLL_INTERVAL_MS = 1000;

function addMinutesISO(baseMs: number, minutes: number) {
  return new Date(baseMs + minutes * 60_000).toISOString();
}

// ====== Mock stops (10) ======
// Rule: first 3 completed, 4th current, rest pending
// Generate 10 demo stops matching backend data
function generateMockStops10(): DeliveryStop[] {
  // Real backend data from TRUCK_001 plan
  const realStops = [
    {
      stop_id: 'STOP_001',
      order: 1,
      address: 'Seattle City Hall, 600 4th Ave, Seattle, WA 98104',
      location: { lat: 47.6062, lon: -122.3321 },
      planned_time_start: '2026-01-27T07:30:00Z',
      planned_time_end: '2026-01-27T07:45:00Z',
      status: 'IN_PROGRESS',
    },
    {
      stop_id: 'STOP_002',
      order: 2,
      address: '2nd Ave & Pike St (Downtown Seattle), Seattle, WA 98101',
      location: { lat: 47.6098, lon: -122.3321 },
      planned_time_start: '2026-01-27T07:45:00Z',
      planned_time_end: '2026-01-27T08:00:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_003',
      order: 3,
      address: 'Pike Place Market, 85 Pike St, Seattle, WA 98101',
      location: { lat: 47.6097, lon: -122.3397 },
      planned_time_start: '2026-01-27T08:00:00Z',
      planned_time_end: '2026-01-27T08:15:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_004',
      order: 4,
      address: 'Seattle Center, 305 Harrison St, Seattle, WA 98109',
      location: { lat: 47.6205, lon: -122.3493 },
      planned_time_start: '2026-01-27T08:15:00Z',
      planned_time_end: '2026-01-27T08:30:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_005',
      order: 5,
      address: 'Kerry Park, 211 W Highland Dr, Seattle, WA 98119',
      location: { lat: 47.6295, lon: -122.3599 },
      planned_time_start: '2026-01-27T08:30:00Z',
      planned_time_end: '2026-01-27T08:45:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_006',
      order: 6,
      address: 'Fremont Troll, N 36th St, Seattle, WA 98103',
      location: { lat: 47.6511, lon: -122.3472 },
      planned_time_start: '2026-01-27T08:45:00Z',
      planned_time_end: '2026-01-27T09:00:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_007',
      order: 7,
      address: 'Seattle-Tacoma International Airport (SEA), 17801 International Blvd, SeaTac, WA 98158',
      location: { lat: 47.4502, lon: -122.3088 },
      planned_time_start: '2026-01-27T09:00:00Z',
      planned_time_end: '2026-01-27T09:15:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_008',
      order: 8,
      address: 'Fife / Milton area (east of Tacoma), WA (approx for lat=47.2396, lon=-122.3572)',
      location: { lat: 47.2396, lon: -122.3572 },
      planned_time_start: '2026-01-27T09:15:00Z',
      planned_time_end: '2026-01-27T09:30:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_009',
      order: 9,
      address: 'Downtown Tacoma / Museum District (approx near 1701 Pacific Ave, Tacoma, WA 98402)',
      location: { lat: 47.2529, lon: -122.4443 },
      planned_time_start: '2026-01-27T09:30:00Z',
      planned_time_end: '2026-01-27T09:45:00Z',
      status: 'PENDING',
    },
    {
      stop_id: 'STOP_010',
      order: 10,
      address: 'Tacoma Dome, 2727 East D St, Tacoma, WA 98421',
      location: { lat: 47.2396, lon: -122.3572 },
      planned_time_start: '2026-01-27T09:45:00Z',
      planned_time_end: '2026-01-27T10:00:00Z',
      status: 'PENDING',
    },
  ];

  return realStops.map((stop) => ({
    stop_id: stop.stop_id,
    plan_id: DEFAULT_PLAN_ID,
    vehicle_id: DEFAULT_VEHICLE_ID,
    original_sequence: stop.order,
    current_sequence: stop.order,
    address: stop.address,
    planned_time_start: stop.planned_time_start,
    planned_time_end: stop.planned_time_end,
    current_time_start: null,
    current_time_end: null,
    status: stop.status,
    package_count: 3,
    stop_order: stop.order,
    eta: stop.planned_time_start,
    location: stop.location,
    gps: stop.location,
    lat: stop.location.lat,
    lon: stop.location.lon,
  } as any));
}

// Normalize stops from any source (backend or demo)
function normalizeStops(rawStops: any[]): DeliveryStop[] {
  if (!Array.isArray(rawStops) || rawStops.length === 0) {
    console.log('[NORMALIZE_STOPS] Empty or invalid input, returning empty');
    return [];
  }

  return rawStops.map((stop, index) => {
    try {
      // Extract location from various possible fields
      let location: { lat: number; lon: number } | undefined = undefined;
      
      const candidates = [
        stop?.location,
        stop?.gps,
        stop?.position,
        stop?.stop_location,
        stop?.coordinates,
        stop?.coords,
      ];
      
      for (const candidate of candidates) {
        if (candidate && typeof candidate.lat === 'number' && typeof candidate.lon === 'number') {
          location = { lat: candidate.lat, lon: candidate.lon };
          break;
        }
      }
      
      if (!location) {
        const lat = stop?.lat ?? stop?.latitude;
        const lon = stop?.lon ?? stop?.lng ?? stop?.longitude;
        if (typeof lat === 'number' && typeof lon === 'number') {
          location = { lat, lon };
        }
      }

      // Use stop_id or task_id or generate from index
      const stopId = stop?.stop_id || stop?.task_id || `STOP_${String(index + 1).padStart(3, '0')}`;

      return {
        stop_id: stopId,
        plan_id: stop?.plan_id || DEFAULT_PLAN_ID,
        vehicle_id: stop?.vehicle_id || DEFAULT_VEHICLE_ID,
        original_sequence: stop?.original_sequence ?? stop?.order ?? index + 1,
        current_sequence: stop?.current_sequence ?? stop?.order ?? index + 1,
        address: stop?.address || `Address ${index + 1}`,
        planned_time_start: stop?.planned_time_start || new Date(Date.now() + index * 30 * 60 * 1000).toISOString(),
        planned_time_end: stop?.planned_time_end || new Date(Date.now() + (index + 1) * 30 * 60 * 1000).toISOString(),
        current_time_start: stop?.current_time_start || null,
        current_time_end: stop?.current_time_end || null,
        status: stop?.status || (index < 2 ? 'completed' : index === 2 ? 'IN_PROGRESS' : 'PENDING'),
        package_count: stop?.package_count ?? 1,
        location: location || { lat: 47.6 + index * 0.01, lon: -122.3 + index * 0.01 },
        gps: location || { lat: 47.6 + index * 0.01, lon: -122.3 + index * 0.01 },
        lat: location?.lat || 47.6 + index * 0.01,
        lon: location?.lon || -122.3 + index * 0.01,
      } as any;
    } catch (error) {
      console.error(`[NORMALIZE_STOPS] Error normalizing stop[${index}]:`, error);
      // Return safe fallback
      return {
        stop_id: `STOP_${String(index + 1).padStart(3, '0')}`,
        plan_id: DEFAULT_PLAN_ID,
        vehicle_id: DEFAULT_VEHICLE_ID,
        original_sequence: index + 1,
        current_sequence: index + 1,
        address: `Address ${index + 1}`,
        planned_time_start: new Date(Date.now() + index * 30 * 60 * 1000).toISOString(),
        planned_time_end: new Date(Date.now() + (index + 1) * 30 * 60 * 1000).toISOString(),
        current_time_start: null,
        current_time_end: null,
        status: index < 2 ? 'completed' : index === 2 ? 'IN_PROGRESS' : 'PENDING',
        package_count: 1,
        location: { lat: 47.6 + index * 0.01, lon: -122.3 + index * 0.01 },
        gps: { lat: 47.6 + index * 0.01, lon: -122.3 + index * 0.01 },
        lat: 47.6 + index * 0.01,
        lon: -122.3 + index * 0.01,
      } as any;
    }
  });
}

// Recalculate planned start/end times from current stop order for immediate ETA update
function recalcEtasByOrder(stops: DeliveryStop[]) {
  const now = Date.now();
  return stops
    .slice()
    .sort((a, b) => (a.current_sequence ?? 999) - (b.current_sequence ?? 999))
    .map((s, idx) => {
      const start = addMinutesISO(now, 5 + idx * 10);
      const end = addMinutesISO(now, 5 + idx * 10 + 10);
      return {
        ...s,
        planned_time_start: start,
        planned_time_end: end,
      };
    });
}

// Build resequence: keep completed stops fixed and reverse the rest (10 stops)
function buildMockResequence(stops: DeliveryStop[]): ResequenceResult {
  const sorted = stops
    .slice()
    .sort((a, b) => (a.current_sequence ?? 999) - (b.current_sequence ?? 999));

  const locked = sorted.filter((s) => String(s.status).toLowerCase() === 'completed');
  const rest = sorted.filter((s) => String(s.status).toLowerCase() !== 'completed');

  // Force reverse for demo to make resequence obvious (instead of random shuffle)
  // Use reverse order so changes are clear during testing
  const newRest = rest.reverse();

  const newAll = [...locked, ...newRest];

  const old_sequence = sorted.map((s, idx) => ({ stop_id: s.stop_id, order: idx + 1 }));
  const new_sequence = newAll.map((s, idx) => ({ stop_id: s.stop_id, order: idx + 1 }));

  console.log('[RESEQUENCE_GENERATED]', {
    totalStops: sorted.length,
    lockedCount: locked.length,
    reorderedCount: rest.length,
    old_sequence_head: old_sequence.slice(0, 5).map(x => x.stop_id),
    old_sequence_tail: old_sequence.slice(-3).map(x => x.stop_id),
    new_sequence_head: new_sequence.slice(0, 5).map(x => x.stop_id),
    new_sequence_tail: new_sequence.slice(-3).map(x => x.stop_id),
    isChanged: JSON.stringify(old_sequence) !== JSON.stringify(new_sequence),
  });

  return {
    result_id: `R${Date.now()}`,
    incident_id: `I${Date.now()}`,
    plan_id: DEFAULT_PLAN_ID,
    old_sequence,
    new_sequence,
    total_eta_before: 150, // Demo value (estimated time for 10 stops)
    improvement_pct: 0.08, // Demo value
    created_at: new Date().toISOString(),
  };
}

function resequenceToEventAndRouting(
  incident: Incident,
  reseq: ResequenceResult
): { event: IncidentEvent; routing: RoutingDecisionResult; pendingSeq: TaskSequenceItem[] } {
  const etaDeltaMinutes = Math.max(3, Math.round(reseq.total_eta_before * reseq.improvement_pct));
  const afterEtaMin = Math.max(1, reseq.total_eta_before - etaDeltaMinutes);

  const event: IncidentEvent = {
    event_id: incident.incident_id,
    vehicle_id: incident.vehicle_id,
    event_type: incident.event_type,
    severity: incident.severity,
    location: incident.location,
    start_time: incident.start_time,
    end_time: incident.end_time,
    gps_speed: incident.gps_speed,
    engine_status: incident.engine_status,
    dtc_codes: incident.dtc_codes,
    eta_impact_min: incident.eta_impact_min,
    requires_reorder: incident.requires_reorder,
    impact: {
      eta_delay_minutes: incident.eta_impact_min,
      affected_tasks: 8,
    },
  };

  const routing: RoutingDecisionResult = {
    vehicle_id: incident.vehicle_id,
    routing_decision: {
      required_reroute: true,
      before_eta: addMinutesISO(Date.now(), reseq.total_eta_before),
      after_eta: addMinutesISO(Date.now(), afterEtaMin),
      eta_delta_minutes: -etaDeltaMinutes,
      improvement_ratio: reseq.improvement_pct,
      suggested_route: {
        total_distance_km: 18.4,
        total_eta_minutes: afterEtaMin,
      },
      task_sequence: reseq.new_sequence.map((x) => ({
        task_id: x.stop_id,
        order: x.order,
        eta: '',
      })),
    } as any,
  };

  const pendingSeq: TaskSequenceItem[] = reseq.new_sequence.map((x) => ({
    task_id: x.stop_id,
    order: x.order,
    eta: '',
  }));

  return { event, routing, pendingSeq };
}

// TODO(DEMO_ROUTE): keep legacy helpers referenced during demo mode to avoid dead-code TS errors
const __demoRoutingBuildersKeep = [
  buildMockResequence,
  resequenceToEventAndRouting,
  recalcEtasByOrder,
  generateMockStops10,
];
void __demoRoutingBuildersKeep;

type FleetProviderProps = { children: React.ReactNode };

export function FleetProvider({ children }: FleetProviderProps) {
  // TODO(DEMO_ROUTE): deterministic demo route variant (A -> B on accept)
  const [routeVariant, setRouteVariant] = useState<RouteVariant>('A');

  // NOTE: disabled for deterministic demo (keep for rollback)
  // Legacy mutable stops state retained for rollback/debug; UI no longer reads from this state.
  const [stops, setStops] = useState<DeliveryStop[]>(() => {
    if (DEMO_STOPS_MODE) {
      // TODO(DEMO_ROUTE): deterministic frontend-owned stops source
      const demoStops = getDemoStops('A');
      console.log('[DEMO_ROUTE] initialized deterministic stops route', {
        variant: 'A',
        count: demoStops.length,
      });
      return demoStops;
    }

    // TODO(DEMO_ROUTE): legacy initialization path retained for rollback
    // const demoStops = normalizeStops(generateMockStops10());
    // console.log('[STOPS_WRITE]', {
    //   source: 'demo',
    //   len: demoStops.length,
    //   firstAddress: demoStops[0]?.address,
    //   firstId: demoStops[0]?.stop_id,
    // });
    // console.log('[STOPS_SOURCE]', {
    //   using: 'demo',
    //   reason: 'startup_initialization',
    //   note: 'Temporary fallback - will be replaced by backend',
    // });
    // console.log('[STOPS_COUNTS]', {
    //   backendLen: 0,
    //   demoLen: demoStops.length,
    //   chosenLen: demoStops.length,
    // });
    // console.log('[STOPS_SAMPLE]', {
    //   firstId: demoStops[0]?.stop_id,
    //   firstAddress: demoStops[0]?.address,
    // });
    // console.log('[DEMO_MODE] activated', {
    //   reason: 'startup_initialization',
    //   count: demoStops.length,
    //   note: 'Will be replaced by backend data when available',
    // });
    const demoStops = normalizeStops(generateMockStops10());
    console.log('[STOPS_WRITE]', {
      source: 'demo',
      len: demoStops.length,
      firstAddress: demoStops[0]?.address,
      firstId: demoStops[0]?.stop_id,
    });
    console.log('[STOPS_SOURCE]', {
      using: 'demo',
      reason: 'startup_initialization',
      note: 'Temporary fallback - will be replaced by backend',
    });
    console.log('[STOPS_COUNTS]', {
      backendLen: 0,
      demoLen: demoStops.length,
      chosenLen: demoStops.length,
    });
    console.log('[STOPS_SAMPLE]', {
      firstId: demoStops[0]?.stop_id,
      firstAddress: demoStops[0]?.address,
    });
    console.log('[DEMO_MODE] activated', {
      reason: 'startup_initialization',
      count: demoStops.length,
      note: 'Will be replaced by backend data when available',
    });
    return demoStops;
  });
  // NOTE: disabled for deterministic demo (keep for rollback)
  void setStops;

  const {
    stops: demoStops,
    sortedStops,
    currentStop: demoCurrentStop,
    nextStop: demoNextStop,
  } = useDemoStops(routeVariant);

  // Track whether backend stops are in use (prevents demo overwrite)
  const [useBackendStops, setUseBackendStops] = useState(false);
  // NOTE: disabled for deterministic demo (keep for rollback)
  void setUseBackendStops;
  
  // 🔒 Track last route accept time to prevent immediate backend overwrite
  const lastRouteAcceptTimeRef = useRef<number>(0);
  // Once location API returns valid coordinates, avoid event stream location override
  const hasLocationApiSuccessRef = useRef(false);

  // Diagnostic: monitor stop changes
  useEffect(() => {
    const stack = new Error().stack?.split('\n').slice(2, 8).join('\n') || 'no stack';
    console.log('[STOPS_AFTER_SET] ⚡ stops changed:', {
      stopsLen: stops?.length,
      head: stops?.slice(0, 3).map(s => ({ id: s.stop_id, seq: s.current_sequence })),
      tail: stops?.slice(-2).map(s => ({ id: s.stop_id, seq: s.current_sequence })),
      timestamp: new Date().toISOString(),
    });
    console.log('[STOPS_AFTER_SET] Stack trace:', stack);
  }, [stops]);

  const [vehicle, setVehicle] = useState<VehicleRealtimeStatus>({
    vehicle_id: DEFAULT_VEHICLE_ID,
    status: 'ON_ROUTE',
    last_location: { lat: 47.6101, lon: -122.2015 },
    last_update_at: new Date().toISOString(),
  });

  const [currentEvent, setCurrentEvent] = useState<IncidentEvent | null>(null);
  const [routingDecision, setRoutingDecision] = useState<RoutingDecisionResult | null>(null);
  const [alertStatus, setAlertStatus] = useState<AlertStatus | null>(null);
  const [alertBehavior, setAlertBehavior] = useState<AlertBehavior | null>(null); // Added
  const [pendingTaskSequence, setPendingTaskSequence] = useState<TaskSequenceItem[] | null>(null);

  // Store pending resequence for accept action
  const [pendingResequence, setPendingResequence] = useState<ResequenceResult | null>(null);

  // Single source of truth for alert popup visibility
  const [activeAlert, setActiveAlert] = useState<{
    incident_id: string;
    severity: 'MEDIUM' | 'HIGH';
    source: 'live' | 'demo';
    event: IncidentEvent;
    behavior: AlertBehavior;
    timestamp: string;
    traceId?: string;
  } | null>(null);

  // Alert pause/buffer system
  const [isAlertPaused, setIsAlertPaused] = useState(false);
  const [bufferedAlerts, setBufferedAlerts] = useState<IngestedAlert[]>([]);

  // Store processed incident IDs to prevent duplicate popups
  const processedIncidentsRef = useRef<Set<string>>(new Set());

  const [currentPage, setCurrentPage] = useState<PageKey>('home');

  const [autoSendMessage, setAutoSendMessage] = useState(false);
  const [pendingAutoReply, setPendingAutoReply] = useState<{ id: string; text: string } | null>(null);

  // Live alert history
  const [liveAlertHistory, setLiveAlertHistory] = useState<Array<{
    incident_id: string;
    severity: 'MEDIUM' | 'HIGH';
    event_type: string;
    timestamp: string;
    title: string;
    description?: string;
    reason?: string;
    vehicle_id?: string;
    gps_speed?: number;
    engine_status?: string;
    dtc_codes?: string[];
    eta_impact_min?: number;
  }>>([])

  useEffect(() => {
    if (import.meta.env.DEV) {
      console.log('[ACTIVE_ALERT_CHANGED]', {
        traceId: activeAlert?.traceId ?? null,
        activeAlert,
      });
    }
  }, [activeAlert]);

  const triggerAutoSendMessage = useCallback(() => {
    setAutoSendMessage(true);
  }, []);

  // Bug fix: standalone auto-reply scheduler
  const scheduleAutoReply = useCallback(() => {
    setTimeout(() => {
      setPendingAutoReply({
        id: `auto_reply_${Date.now()}`,
        text: 'John, pull over safely and suspend delivery immediately. Stand by for instructions.',
      });
    }, 3000);
  }, []);

  const clearAutoSendMessage = useCallback(() => {
    setAutoSendMessage(false);
  }, []);

  // Clear live alert history
  const clearLiveAlertHistory = useCallback(() => {
    console.log('[HISTORY] Clearing live alert history');
    setLiveAlertHistory([]);
  }, []);

  const appendAlertHistory = useCallback((alert: IngestedAlert) => {
    setLiveAlertHistory((prev) => {
      if (prev.some((h) => h.incident_id === alert.incident_id)) {
        return prev;
      }
      const newEntry = {
        incident_id: alert.incident_id,
        severity: alert.severity,
        event_type: alert.event.event_type,
        timestamp: alert.event.start_time,
        title: alert.event.reason || alert.event.event_type.replace(/_/g, ' '),
        description: (alert.incident as any)?.description || alert.event.reason,
        reason: alert.event.reason,
        vehicle_id: alert.event.vehicle_id,
        gps_speed: alert.incident?.gps_speed,
        engine_status: alert.incident?.engine_status,
        dtc_codes: alert.incident?.dtc_codes,
        eta_impact_min: alert.incident?.eta_impact_min,
      };
      return [newEntry, ...prev].slice(0, 20);
    });
  }, []);

  // Processing stage only: write processing states and modal state
  const pushAlert = useCallback((alert: IngestedAlert) => {
    const traceId = alert.traceId ?? `push-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    if (import.meta.env.DEV) {
      console.log('[LIVE_ALERT_PIPELINE][PUSH_ALERT_START]', {
        traceId,
        incident_id: alert.incident_id,
        severity: alert.severity,
        source: alert.source,
        behavior: alert.behavior,
        isAlertPaused,
        dedupeHit: false,
        bufferLen: bufferedAlerts.length,
        queueLen: bufferedAlerts.length,
      });
    }
    let nextResequence: ResequenceResult | null = alert.resequence ?? null;
    let nextRouting: RoutingDecisionResult | null = alert.routing ?? null;
    let nextTaskSequence: TaskSequenceItem[] | null = alert.taskSequence ?? null;

    if (alert.severity === 'MEDIUM') {
      if (alert.source === 'demo') {
        console.log('[DEMO_MEDIUM] Demo medium alert: UI-only mode, no routing/state mutation');
        nextResequence = null;
        nextRouting = null;
        nextTaskSequence = null;
      } else if (alert.source === 'live') {
        console.log('[LIVE_EVENT] MEDIUM routing decision pending backend response');
        nextResequence = null;
        nextRouting = null;
        nextTaskSequence = null;
      }
    }

    if (alert.severity === 'HIGH' && alert.incident) {
      const msgData = AlertPolicy.generateHighSeverityMessage(alert.incident);
      MessageStore.addMessage({
        id: `MSG_${Date.now()}`,
        thread_id: msgData.thread_id,
        title: msgData.title,
        content: msgData.content,
        timestamp: new Date().toISOString(),
        is_draft: false,
        is_sent: true,
      });
    }

    console.log(`[PUSH_ALERT] 🔔 Setting active alert:`, {
      incident_id: alert.incident_id,
      severity: alert.severity,
      source: alert.source,
      behavior: alert.behavior,
      timestamp: new Date().toISOString(),
    });

    // Explicit log before setting activeAlert
    console.log('[ALERT_MODAL] Setting active alert for modal display', {
      incident_id: alert.incident_id,
      severity: alert.severity,
    });

    setPendingResequence(nextResequence);
    setRoutingDecision(nextRouting);
    setPendingTaskSequence(nextTaskSequence);

    // Set legacy states for backward compatibility
    setCurrentEvent(alert.event);
    setAlertBehavior(alert.behavior);
    setAlertStatus('pending');

    // Set the single source of truth
    const nextActiveAlert = {
      incident_id: alert.incident_id,
      severity: alert.severity,
      source: alert.source,
      event: alert.event,
      behavior: alert.behavior,
      timestamp: new Date().toISOString(),
      traceId,
    };

    if (import.meta.env.DEV) {
      console.log('[LIVE_ALERT_PIPELINE][SET_ACTIVE_ALERT_BEFORE]', {
        traceId,
        incident_id: nextActiveAlert.incident_id,
        severity: nextActiveAlert.severity,
        source: nextActiveAlert.source,
        behavior: nextActiveAlert.behavior,
        isAlertPaused,
        dedupeHit: false,
        bufferLen: bufferedAlerts.length,
        queueLen: bufferedAlerts.length,
      });
    }

    setActiveAlert(nextActiveAlert);

    if (import.meta.env.DEV) {
      console.log('[LIVE_ALERT_PIPELINE][SET_ACTIVE_ALERT_AFTER]', {
        traceId,
        incident_id: nextActiveAlert.incident_id,
        severity: nextActiveAlert.severity,
        source: nextActiveAlert.source,
        behavior: nextActiveAlert.behavior,
        isAlertPaused,
        dedupeHit: false,
        bufferLen: bufferedAlerts.length,
        queueLen: bufferedAlerts.length,
      });
    }

  }, [isAlertPaused, bufferedAlerts.length]);

  // Ingestion stage: log and queue when paused, otherwise process immediately
  const ingestAlert = useCallback((alert: IngestedAlert) => {
    const traceId = alert.traceId ?? `ingest-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    if (import.meta.env.DEV) {
      console.log('[LIVE_ALERT_PIPELINE][INGEST_ALERT]', {
        traceId,
        incident_id: alert.incident_id,
        severity: alert.severity,
        source: alert.source,
        behavior: alert.behavior,
        isAlertPaused,
        dedupeHit: false,
        bufferLen: bufferedAlerts.length,
        queueLen: bufferedAlerts.length,
      });
    }

    appendAlertHistory(alert);

    if (isAlertPaused) {
      console.log('[ALERT_BUFFER] Alert received while paused, buffering:', {
        incident_id: alert.incident_id,
        severity: alert.severity,
      });
      setBufferedAlerts((prev) => {
        const next = [...prev, alert];
        if (import.meta.env.DEV) {
          console.log('[LIVE_ALERT_PIPELINE][BUFFER_ENQUEUE]', {
            traceId,
            reason: 'DROP: paused->buffer',
            incident_id: alert.incident_id,
            severity: alert.severity,
            source: alert.source,
            behavior: alert.behavior,
            isAlertPaused,
            dedupeHit: false,
            bufferLen: next.length,
            queueLen: next.length,
          });
        }
        return next;
      });
      return;
    }

    pushAlert(alert);
  }, [appendAlertHistory, isAlertPaused, pushAlert, bufferedAlerts.length]);

  // Bug fix: clear active alert (user actions only)
  const clearActiveAlert = useCallback((reason: string) => {
    console.log(`[CLEAR_ALERT] ❌ Clearing active alert. Reason: ${reason}`);
    if (import.meta.env.DEV) {
      const stack = new Error().stack?.split('\n').slice(1, 6).join('\n') || 'no stack';
      console.log('[CLEAR_ALERT_CALLSITE]', {
        reason,
        stack,
      });
    }
    setActiveAlert(null);
    setAlertStatus(null);
    setPendingTaskSequence(null);
    setPendingResequence(null);
    setCurrentEvent(null);
    setRoutingDecision(null);
    setAlertBehavior(null);
  }, []);

  // ====== Backend event callback: mark as live and map to IncidentEvent ======
  // Map backend tasks to DeliveryStop (safe version with null checks)
  const mapBackendTasksToStops = useCallback(
    (tasks: any[], planId: string, vehicleId: string): DeliveryStop[] => {
      if (!Array.isArray(tasks)) {
        console.warn('[BACKEND_STOPS] Invalid tasks array, returning empty');
        return [];
      }
      
      return tasks.map((task, index) => {
        try {
          // Extract GPS coordinates - check multiple possible field names (in priority order)
          let location: { lat: number; lon: number } | undefined = undefined;
          
          // Try different object paths
          const candidates = [
            task?.location,
            task?.gps,
            task?.position,
            task?.stop_location,
            task?.coordinates,
            task?.coords,
          ];
          
          for (const candidate of candidates) {
            if (candidate && typeof candidate.lat === 'number' && typeof candidate.lon === 'number') {
              location = { lat: candidate.lat, lon: candidate.lon };
              break;
            }
          }
          
          // Try individual fields with different names
          if (!location) {
            const lat = task?.lat ?? task?.latitude;
            const lon = task?.lon ?? task?.lng ?? task?.longitude;
            if (typeof lat === 'number' && typeof lon === 'number') {
              location = { lat, lon };
            }
          }
          
          return {
            stop_id: task?.task_id || `STOP_${index + 1}`,
            plan_id: planId,
            vehicle_id: vehicleId,
            original_sequence: task?.order ?? index + 1,
            current_sequence: task?.order ?? index + 1,
            address: task?.address || `Address ${index + 1}`,
            planned_time_start: task?.planned_time_start || new Date(Date.now() + index * 30 * 60 * 1000).toISOString(),
            planned_time_end: task?.planned_time_end || new Date(Date.now() + (index + 1) * 30 * 60 * 1000).toISOString(),
            current_time_start: null,
            current_time_end: null,
            status: task?.status || (index === 0 ? 'IN_PROGRESS' : 'PENDING'),
            package_count: task?.package_count ?? 1,
            stop_order: task?.order ?? index + 1,
            eta: task?.planned_time_start || new Date(Date.now() + index * 30 * 60 * 1000).toISOString(),
            location,
            gps: location,
            lat: location?.lat,
            lon: location?.lon,
          };
        } catch (error) {
          console.error(`[BACKEND_STOPS] Error mapping task[${index}]:`, error);
          // Return safe fallback
          return {
            stop_id: `STOP_${index + 1}`,
            plan_id: planId,
            vehicle_id: vehicleId,
            original_sequence: index + 1,
            current_sequence: index + 1,
            address: `Address ${index + 1}`,
            planned_time_start: new Date(Date.now() + index * 30 * 60 * 1000).toISOString(),
            planned_time_end: new Date(Date.now() + (index + 1) * 30 * 60 * 1000).toISOString(),
            current_time_start: null,
            current_time_end: null,
            status: index === 0 ? 'IN_PROGRESS' : 'PENDING',
            package_count: 1,
            stop_order: index + 1,
            eta: new Date(Date.now() + index * 30 * 60 * 1000).toISOString(),
          };
        }
      });
    },
    []
  );

  const handleLiveEvent = useCallback(
    (payload: any) => {
      const traceId = payload?.__traceId ?? `hl-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      try {
        console.log('[LIVE_EVENT_START] 📨 handleLiveEvent called', {
          traceId,
          hasPayload: !!payload,
          currentStopsLen: stops?.length,
          currentStops0: stops[0]?.stop_id,
          useBackendStops,
          timestamp: new Date().toISOString(),
        });

        // Extract components from payload
        const incidentRaw = payload.incident;
        const vehicleStatus = payload.vehicle_status || payload.vehicleStatus || null;
        const backendTasks = payload.original_task_sequence || payload.stops || payload.tasks;

        // Skip only when all data channels are empty
        if (!incidentRaw && !vehicleStatus && (!backendTasks || backendTasks.length === 0)) {
          console.log('[LIVE_EVENT_SKIP] DROP: empty payload channels', {
            traceId,
            reason: 'DROP: empty payload channels',
          });
          return;
        }

      const incidentId = incidentRaw ? String(
        incidentRaw.incident_id ??
          incidentRaw.id ??
          incidentRaw.event_id ??
          `LIVE_${Date.now()}`
      ) : `TASKS_${Date.now()}`;

      const incidentTimestamp =
        (incidentRaw?.timestamp as string | undefined) ??
        (incidentRaw?.start_time as string | undefined) ??
        '';
      const dedupeKey = `${incidentId}|${incidentTimestamp}`;
      const dedupeHit = processedIncidentsRef.current.has(dedupeKey);

      console.log('[EVENT_IN] Event received:', {
        traceId,
        incidentId,
        dedupeKey,
        dedupeHit,
        hasIncident: !!incidentRaw,
        severity: incidentRaw?.severity,
        source: 'live',
        behavior: null,
        isAlertPaused,
        bufferLen: bufferedAlerts.length,
        queueLen: bufferedAlerts.length,
        stopsLen: stops?.length,
        hasVehicleStatus: !!vehicleStatus,
        hasBackendTasks: !!backendTasks,
        backendTasksCount: backendTasks?.length || 0,
        timestamp: new Date().toISOString(),
      });

      // Update vehicle state from vehicle_status if provided
      if (vehicleStatus) {
        const resolvedLocation =
          vehicleStatus.last_location ||
          vehicleStatus.location ||
          vehicleStatus.gps ||
          ((typeof vehicleStatus.lat === 'number' || typeof vehicleStatus.latitude === 'number') &&
          (typeof vehicleStatus.lon === 'number' || typeof vehicleStatus.lng === 'number' || typeof vehicleStatus.longitude === 'number')
            ? {
                lat: vehicleStatus.lat ?? vehicleStatus.latitude,
                lon: vehicleStatus.lon ?? vehicleStatus.lng ?? vehicleStatus.longitude,
              }
            : null);

        const resolvedUpdatedAt =
          vehicleStatus.last_update_at ||
          vehicleStatus.updated_at ||
          vehicleStatus.timestamp ||
          new Date().toISOString();

        // Prevent location flicker: after location API succeeds, event stream only updates metadata (status/id)
        const allowEventLocationWrite = !hasLocationApiSuccessRef.current;

        setVehicle((prev) => ({
          ...prev,
          vehicle_id: vehicleStatus.vehicle_id || prev.vehicle_id,
          status: vehicleStatus.status || prev.status,
          last_location: allowEventLocationWrite ? (resolvedLocation || prev.last_location) : prev.last_location,
          last_update_at: allowEventLocationWrite ? resolvedUpdatedAt : prev.last_update_at,
        }));
        console.log('[LIVE_EVENT] Vehicle state updated from backend', vehicleStatus);
      }

      // Update stops from backend task sequence if provided
      if (backendTasks && Array.isArray(backendTasks) && backendTasks.length > 0) {
        console.log('[BACKEND_LOAD_START] 🔍 Backend tasks detected', {
          currentStopsLen: stops?.length,
          backendTasksLen: backendTasks.length,
          useBackendStops,
          timestamp: new Date().toISOString(),
        });
        
        // Guard: do not overwrite right after route accept
        const timeSinceRouteAccept = Date.now() - lastRouteAcceptTimeRef.current;
        if (timeSinceRouteAccept < 10000) { // 10 seconds grace period
          console.log('[BACKEND_LOAD_BLOCKED] 🛡️ Grace period active - skipping backend overwrite', {
            timeSinceAccept: `${timeSinceRouteAccept}ms`,
            gracePeriod: '10000ms',
          });
          // Skip backend stop update but continue processing the incident
        } else {
          // Proceed with backend stop update
          try {
          const planId = `PLAN_${Date.now()}`;
          const vehicleId = incidentRaw?.vehicle_id || vehicleStatus?.vehicle_id || DEFAULT_VEHICLE_ID;
          
          // One-time debug: log first time only to avoid spam
          if (!useBackendStops) {
            console.log('[BACKEND_LOAD_RAW] 📥 Raw first task:', {
              taskCount: backendTasks.length,
              task0_keys: backendTasks[0] ? Object.keys(backendTasks[0]) : [],
              task0_id: backendTasks[0]?.task_id || backendTasks[0]?.id || backendTasks[0]?.stop_id,
              task0_address: backendTasks[0]?.address,
            });
          }
          
          const mappedStops = mapBackendTasksToStops(backendTasks, planId, vehicleId);
          
          console.log('[BACKEND_LOAD_MAPPED] 📊 After mapping', {
            mappedLen: mappedStops.length,
            mapped0_stop_id: mappedStops[0]?.stop_id,
            mapped0_address: mappedStops[0]?.address,
          });
          
          if (mappedStops.length > 0) {
            // Normalize backend stops to keep schema consistent with demo stops
            const normalizedStops = normalizeStops(mappedStops);
            
            console.log('[BACKEND_LOAD_FINAL] ✅ About to setStops with normalized backend data', {
              source: 'backend',
              normalizedLen: normalizedStops.length,
              normalized0_stop_id: normalizedStops[0]?.stop_id,
              normalized0_address: normalizedStops[0]?.address,
              timestamp: new Date().toISOString(),
            });
            
            console.log('[STOPS_WRITE]', {
              source: 'backend_event',
              len: normalizedStops.length,
              firstAddress: normalizedStops[0]?.address,
              firstId: normalizedStops[0]?.stop_id,
            });
            console.log('[STOPS_SOURCE]', {
              using: 'backend',
              reason: 'event_api_update',
              action: useBackendStops ? 'updating_backend' : 'replacing_demo',
            });
            console.log('[STOPS_COUNTS]', {
              backendLen: normalizedStops.length,
              demoLen: 10,
              chosenLen: normalizedStops.length,
            });
            console.log('[STOPS_SAMPLE]', {
              firstId: normalizedStops[0]?.stop_id,
              firstAddress: normalizedStops[0]?.address,
            });
            console.log('[BACKEND_LOAD_SUCCESS] ✅ Backend stops from event (overriding demo if needed)');

            if (DEMO_STOPS_MODE) {
              // TODO(DEMO_ROUTE): ignore backend stops/tasks to keep deterministic demo route ownership in frontend
              console.log('[DEMO_ROUTE] ignoring backend stops update (deterministic mode)', {
                incomingCount: normalizedStops.length,
                activeVariant: routeVariant,
              });
            } else {
              // NOTE: disabled for deterministic demo (keep for rollback)
              // TODO(DEMO_ROUTE): old backend overwrite path retained for rollback
              // setStops(normalizedStops);
              // setUseBackendStops(true);
              // setStops(normalizedStops);
              // setUseBackendStops(true);
            }
            
            console.log('[BACKEND_LOAD_SUCCESS] ✅ Backend stops loaded and set', {
              count: normalizedStops.length,
              stopIds: normalizedStops.slice(0, 3).map(s => s.stop_id),
            });
          } else {
            console.warn('[BACKEND_LOAD_ZERO] ⚠️ Mapping returned 0 stops, keeping existing', {
              reason: 'mapping_returned_zero',
            });
          }
        } catch (error) {
          console.error('[BACKEND_LOAD_ERROR] ❌ Error mapping backend stops:', error);
          // Keep using current stops on error - do not crash
        }
        } // End of grace period else block
      } else {
        console.log('[BACKEND_LOAD_SKIP] ⏭️ No backend tasks in payload', {
          hasBackendTasks: !!backendTasks,
          isArray: Array.isArray(backendTasks),
          length: backendTasks?.length || 0,
        });
      }

      // Prevent duplicates
      if (dedupeHit) {
        console.log('[EVENT_DROPPED] DROP: dedupe', {
          traceId,
          reason: 'DROP: dedupe',
          incidentId,
          dedupeKey,
        });
        return;
      }

      // Only process incident alert if incident exists
      if (incidentRaw) {
        const rawIncidentId = incidentRaw.incident_id ?? incidentRaw.id ?? incidentRaw.event_id;
        const rawIncidentTs = incidentRaw.timestamp ?? incidentRaw.start_time;
        if (!rawIncidentId || !rawIncidentTs) {
          console.log('[EVENT_DROPPED] DROP: invalid payload missing incident_id/timestamp', {
            traceId,
            reason: 'DROP: invalid payload missing incident_id/timestamp',
            rawIncidentId: rawIncidentId ?? null,
            rawIncidentTs: rawIncidentTs ?? null,
          });
          return;
        }

        const severity = (typeof incidentRaw.severity === 'string'
          ? incidentRaw.severity.toUpperCase()
          : 'MEDIUM') as Incident['severity'];

        const eventType = (typeof incidentRaw.event_type === 'string'
          ? (incidentRaw.event_type as Incident['event_type'])
          : 'TRAFFIC_JAM') as Incident['event_type'];

        const incident: Incident = {
          incident_id: incidentId,
          vehicle_id: (incidentRaw.vehicle_id as string) || DEFAULT_VEHICLE_ID,
          event_type: eventType,
          severity,
          start_time: (incidentRaw.timestamp as string) ?? new Date().toISOString(),
          end_time: null,
          location:
            (incidentRaw.location as any) ?? vehicle.last_location ?? { lat: 47.6098, lon: -122.201 },
          gps_speed: typeof incidentRaw.gps_speed === 'number' ? incidentRaw.gps_speed : 0,
          engine_status: typeof incidentRaw.engine_status === 'string' ? incidentRaw.engine_status : 'ON',
          dtc_codes: Array.isArray((incidentRaw as any).dtc_codes) ? (incidentRaw as any).dtc_codes : [],
          eta_impact_min: typeof incidentRaw.eta_impact_min === 'number' ? incidentRaw.eta_impact_min : 12,
          requires_reorder: typeof incidentRaw.requires_reorder === 'boolean' ? incidentRaw.requires_reorder : severity === 'MEDIUM',
        };

        const behavior = AlertPolicy.getAlertBehavior(incident, 'live');
        console.log('[LIVE_EVENT] AlertPolicy behavior:', { severity, behavior });

        if (!behavior) {
          console.log('[EVENT_DROPPED] DROP: severity filtered', {
            traceId,
            reason: 'DROP: severity filtered',
            severity,
          });
          // Continue to process tasks/stops even if incident is dropped
        } else {
          processedIncidentsRef.current.add(dedupeKey);

          const eventForUi: IncidentEvent = {
            ...AlertPolicy.buildIncidentEvent(incident),
            source: 'live',
            reason: (incidentRaw.reason as string) || incident.event_type.replace(/_/g, ' '),
          };

          // Push alert through unified ingestion pipeline
          console.log('[LIVE_EVENT] Pushing alert to unified pipeline:', { behavior, source: 'live', incidentId });
          ingestAlert({
            incident_id: incidentId,
            severity: severity === 'HIGH' ? 'HIGH' : 'MEDIUM',
            source: 'live',
            event: eventForUi,
            behavior: behavior,
            traceId,
            incident,
            resequence: null,
            routing: null,
            taskSequence: null,
          });
        }
      } else {
        console.log('[LIVE_EVENT] No incident in payload - only processing tasks/stops');
      }
      } catch (err) {
        // Critical fix: swallow all event-handler errors to keep provider alive
        console.error('[LIVE_EVENT_ERROR] Error processing event (provider is safe):', {
          traceId,
          error: err instanceof Error ? err.message : String(err),
          errorName: err instanceof Error ? err.name : 'Unknown',
        });
        // Do NOT re-throw - we want the provider to stay alive even if one event fails
      } finally {
        if (import.meta.env.DEV) {
          console.log('[LIVE_EVENT_END] handleLiveEvent finished', {
            traceId,
            activeAlertIncidentId: activeAlert?.incident_id ?? null,
            isAlertPaused,
            bufferLen: bufferedAlerts.length,
            queueLen: bufferedAlerts.length,
          });
        }
      }
    },
    [vehicle, stops, mapBackendTasksToStops, useBackendStops, ingestAlert, activeAlert, isAlertPaused, bufferedAlerts.length, routeVariant]
  );

  // Backend long polling: fetch events and mark source as live
  useEventLongPoll({ vehicleId: DEFAULT_VEHICLE_ID, onEvent: handleLiveEvent, waitS: 1 });

  // Backend location polling: authoritative vehicle position from simulator→db→lambda→api chain
  useEffect(() => {
    let isDisposed = false;

    const pollLocation = async () => {
      while (!isDisposed) {
        try {
          const params = new URLSearchParams({ vehicle_id: DEFAULT_VEHICLE_ID });
          const response = await fetch(`${LOCATION_API_URL}?${params.toString()}`);

          if (response.ok) {
            const data = await response.json();
            const lat = data?.location?.lat;
            const lon = data?.location?.lon;
            const stale = data?.stale;

            if (data?.ok === true && stale !== true && typeof lat === 'number' && typeof lon === 'number') {
              hasLocationApiSuccessRef.current = true;
              setVehicle((prev) => ({
                ...prev,
                vehicle_id: typeof data.vehicle_id === 'string' ? data.vehicle_id : prev.vehicle_id,
                last_location: { lat, lon },
                last_update_at:
                  typeof data.server_received_at === 'string'
                    ? data.server_received_at
                    : new Date().toISOString(),
              }));
            } else if (data?.ok === true && stale === true) {
              console.warn('[LOCATION_API] Received stale location, skipping update', {
                age_sec: data?.age_sec,
              });
            }
          } else {
            console.warn('[LOCATION_API] Non-OK response', { status: response.status });
          }
        } catch (error) {
          console.warn('[LOCATION_API] Poll failed, will retry', {
            error: error instanceof Error ? error.message : String(error),
          });
        }

        if (!isDisposed) {
          await new Promise((resolve) => setTimeout(resolve, LOCATION_POLL_INTERVAL_MS));
        }
      }
    };

    void pollLocation();

    return () => {
      isDisposed = true;
    };
  }, []);

  // ====== Vehicle movement policy ======
  // Default: static. Vehicle location is authoritative from live vehicle_status only.

  // ====== Trigger Medium Severity Demo Alert (for Blue FAB) ======
  const triggerMediumSeverityDemoAlert = useCallback(() => {
    if (import.meta.env.DEV) {
      console.log('DEMO_MEDIUM_CLICKED');
    }
    console.log('[MEDIUM_DEMO] 🔵 Medium severity demo alert triggered');

    if (activeAlert !== null) {
      console.log('[MEDIUM_DEMO] Skipped - alert already active');
      return;
    }

    // Create MEDIUM severity incident
    const incident: Incident = {
      incident_id: `MEDIUM_DEMO_${Date.now()}`,
      vehicle_id: DEFAULT_VEHICLE_ID,
      event_type: 'TRAFFIC_JAM',
      severity: 'MEDIUM',
      start_time: new Date().toISOString(),
      end_time: null,
      location: { lat: 47.6098, lon: -122.2010 },
      gps_speed: 15.5,
      engine_status: 'ON',
      dtc_codes: [],
      eta_impact_min: 10,
      requires_reorder: true,
    };

    const behavior: AlertBehavior = 'modal';
    console.log('[MEDIUM_DEMO] Generated MEDIUM severity alert:', {
      incident_id: incident.incident_id,
      event_type: incident.event_type,
    });

    // Generate event for UI display
    const event = { ...AlertPolicy.buildIncidentEvent(incident), source: 'demo' as const };

    // Push alert through unified ingestion pipeline
    console.log('[MEDIUM_DEMO] ✅ Ingesting MEDIUM severity demo alert');
    ingestAlert({
      incident_id: incident.incident_id,
      severity: 'MEDIUM',
      source: 'demo',
      event,
      behavior,
      incident,
      resequence: null,
      routing: null,
      taskSequence: null,
    });
  }, [activeAlert, ingestAlert]);

  // ====== Trigger High Severity Demo Alert (for new Red FAB) ======
  const triggerHighSeverityDemoAlert = useCallback(() => {
    console.log('[HIGH_DEMO] 🔴 High severity demo alert triggered');

    // Fix: allow alerts on any page, including messages
    // Users should see critical alerts even while messaging
    if (activeAlert !== null) {
      console.log('[HIGH_DEMO] Skipped - alert already active');
      return;
    }

    // Create HIGH severity incident
    const incident: Incident = {
      incident_id: `HIGH_DEMO_${Date.now()}`,
      vehicle_id: DEFAULT_VEHICLE_ID,
      event_type: 'BREAKDOWN',
      severity: 'HIGH',
      start_time: new Date().toISOString(),
      end_time: null,
      location: { lat: 47.6098, lon: -122.2010 },
      gps_speed: 0,
      engine_status: 'OFF',
      dtc_codes: ['P0300', 'P0301'],
      eta_impact_min: 60,
      requires_reorder: true,
    };

    const behavior: AlertBehavior = 'modal';
    console.log('[HIGH_DEMO] Generated HIGH severity alert:', {
      incident_id: incident.incident_id,
      event_type: incident.event_type,
    });

    // Generate event for UI display
    const event = { ...AlertPolicy.buildIncidentEvent(incident), source: 'demo' as const };

    // Push alert through unified ingestion pipeline
    console.log('[HIGH_DEMO] ✅ Ingesting HIGH severity demo alert');
    ingestAlert({
      incident_id: incident.incident_id,
      severity: 'HIGH',
      source: 'demo',
      event,
      behavior,
      incident,
      resequence: null,
      routing: null,
      taskSequence: null,
    });
  }, [currentPage, activeAlert, ingestAlert]);

  // ====== Toggle Alert Pause ======
  const toggleAlertPause = useCallback(() => {
    // Refactored: avoid nested state setters; use direct state update
    setIsAlertPaused((oldIsPaused) => {
      const newIsPaused = !oldIsPaused;
      console.log('[ALERT_PAUSE] Toggled:', newIsPaused ? 'paused' : 'unpaused');

      // If unpausing, don't process alerts yet - we'll do it in useEffect
      return newIsPaused;
    });
  }, []);

  // Handle dequeuing and display of buffered alerts when unpaused (FIFO)
  useEffect(() => {
    if (isAlertPaused || bufferedAlerts.length === 0 || activeAlert) {
      return; // Don't process if paused or no buffered alerts
    }

    // Unpaused and have buffered alerts - process the oldest one
    const processNextAlert = async () => {
      const oldestAlert = bufferedAlerts[0];

      if (!oldestAlert || typeof oldestAlert !== 'object') {
        console.warn('[ALERT_BUFFER] Invalid buffered alert structure:', oldestAlert);
        setBufferedAlerts((prev) => prev.slice(1));
        return;
      }

      console.log('[ALERT_BUFFER] Popping oldest buffered alert', {
        incident_id: oldestAlert.incident_id,
        severity: oldestAlert.behavior,
        source: oldestAlert.source,
        remaining: bufferedAlerts.length - 1,
      });

      // Remove from buffer first
      setBufferedAlerts((prev) => prev.slice(1));

      try {
        pushAlert(oldestAlert);
      } catch (err) {
        console.error('[ALERT_BUFFER] Error pushing buffered alert:', {
          error: err instanceof Error ? err.message : String(err),
          incidentId: oldestAlert.incident_id,
        });
      }
    };

    // Use a small delay to ensure state updates are applied
    const timeoutId = setTimeout(processNextAlert, 50);
    return () => clearTimeout(timeoutId);
  }, [isAlertPaused, bufferedAlerts, pushAlert, activeAlert]);

  // ====== Accept: apply local resequence, recalculate ETA, close popup ======
  const acceptRouting = useMemo(
    () => () => {
      // NOTE: disabled for deterministic demo (keep for rollback)
      // if (activeAlert?.source === 'demo') {
      //   console.log('[ACCEPT_ROUTING] Demo source detected - NO-OP (UI demo only)');
      //   return;
      // }

      if (DEMO_STOPS_MODE) {
        // TODO(DEMO_ROUTE): deterministic route switch on medium Update Route CTA
        const updatedStops = getDemoStops('B');
        console.log('[DEMO_ROUTE] acceptRouting switching deterministic route', {
          from: routeVariant,
          to: routeVariant === 'A' ? 'B' : 'B',
          demoStopsCount: demoStops.length,
          updatedStopsCount: updatedStops.length,
          newCurrentStopId: updatedStops[0]?.stop_id ?? null,
          newCurrentStopStatus: updatedStops[0]?.status ?? null,
        });
        setRouteVariant((prev) => (prev === 'A' ? 'B' : 'B'));
        lastRouteAcceptTimeRef.current = Date.now();
        clearActiveAlert('user-accept-cta');
        return;
      }

      console.log('[ACCEPT_ROUTING_ENTRY] acceptRouting called', {
        stopsLen: stops?.length,
        hasPendingResequence: !!pendingResequence,
        alertBehavior,
        timestamp: new Date().toISOString(),
      });

      // NOTE: disabled for deterministic demo (keep for rollback)
      // TODO(DEMO_ROUTE): old backend resequence path retained for rollback
      // // MEDIUM: apply backend sequence only when resequence exists
      // if (activeAlert?.severity === 'MEDIUM' && pendingResequence) {
      //   // Log raw sequences from backend
      //   console.log('[ACCEPT_ROUTING] Raw sequences from pendingResequence:', {
      //     old_sequence: pendingResequence.old_sequence.map(x => x.stop_id),
      //     new_sequence: pendingResequence.new_sequence.map(x => x.stop_id),
      //     totalCount: pendingResequence.new_sequence.length,
      //   });
      //
      //   // Step 1: update stops using backend new_sequence (full replacement)
      //   setStops((prev) => {
      //     console.log('[STOPS_WRITE]', {
      //       source: 'user_accept_route_update',
      //       len: prev.length,
      //       firstId: prev[0]?.stop_id,
      //       timestamp: new Date().toISOString(),
      //     });
      //     console.log('[ACCEPT_ROUTING] Raw sequences from pendingResequence:', {
      //       old_sequence: pendingResequence.old_sequence.map(x => x.stop_id),
      //       new_sequence: pendingResequence.new_sequence.map(x => x.stop_id),
      //       totalCount: pendingResequence.new_sequence.length,
      //     });
      //
      //     // Build stop ID map using stop_id as key
      //     // This ensures matching with backend sequence IDs
      //     const byId = new Map<string, DeliveryStop>();
      //     prev.forEach((s) => {
      //       byId.set(s.stop_id, s);
      //     });
      //
      //     // Verify key matching between current stops and backend sequence
      //     const mapKeys = [...byId.keys()];
      //     const newSeqIds = pendingResequence.new_sequence.map((x) => x.stop_id);
      //     const matchedCount = newSeqIds.filter((id) => byId.has(id)).length;
      //     const missingIds = newSeqIds.filter((id) => !byId.has(id));
      //
      //     console.log('[ACCEPT_ROUTING] Stop ID mapping verification:', {
      //       currentStopsCount: prev.length,
      //       mapKeysCount: mapKeys.length,
      //       newSeqIdsCount: newSeqIds.length,
      //       matchedCount,
      //       missingIds: missingIds.length > 0 ? missingIds : 'all matched ✓',
      //       mapKeysSample: mapKeys.slice(0, 3),
      //       newSeqIdsSample: newSeqIds.slice(0, 3),
      //     });
      //
      //     // Rebuild stops array strictly following new_sequence (full replacement)
      //     // No locking, no filtering by status - use exact backend order
      //     const reordered = pendingResequence.new_sequence
      //       .map((seqItem) => byId.get(seqItem.stop_id))
      //       .filter(Boolean) as DeliveryStop[];
      //
      //     console.log('[ACCEPT_ROUTING] After reordering by new_sequence:', {
      //       reorderedLen: reordered.length,
      //       head: reordered.slice(0, 5).map(s => `${s.stop_id} (seq:${s.current_sequence})`),
      //       tail: reordered.slice(-3).map(s => `${s.stop_id} (seq:${s.current_sequence})`),
      //     });
      //
      //     // Update current_sequence to match new array order (1, 2, 3, ...)
      //     const resequenced = reordered.map((s, idx) => ({
      //       ...s,
      //       current_sequence: idx + 1,
      //     }));
      //
      //     // Recalculate ETA (planned_time_start/end)
      //     const result = recalcEtasByOrder(resequenced);
      //
      //     // Final verification logs
      //     console.log('[ACCEPT_ROUTING_FINAL] Final stops after setStops (raw order):', {
      //       finalLen: result.length,
      //       finalStopsOrder: result.map(s => s.stop_id),
      //       finalHead: result.slice(0, 5).map(s => s.stop_id),
      //       finalTail: result.slice(-3).map(s => s.stop_id),
      //       timestamp: new Date().toISOString(),
      //     });
      //
      //     return result;
      //   });
      // } else {
      //   console.log('[ACCEPT_ROUTING] ⚠️ Not applying resequence - condition not met', {
      //     alertSeverity: activeAlert?.severity,
      //     hasPendingResequence: !!pendingResequence,
      //   });
      // }

      // Step 2: clear alert state
      console.log('[ACCEPT_ROUTING_EXIT] About to clear alert', {
        stopsLen: stops.length,
        timestamp: new Date().toISOString(),
      });
      
      // Mark route accept time to prevent immediate backend overwrite
      lastRouteAcceptTimeRef.current = Date.now();
      
      clearActiveAlert('user-accept-cta');
      console.log('[ACCEPT_ROUTING_DONE] Alert cleared, exiting acceptRouting', {
        stopsLen: stops.length,
        timestamp: new Date().toISOString(),
      });
    },
    [clearActiveAlert, routeVariant, demoStops.length]
  );

  // ====== Dismiss: close popup without changing stops ======
  const dismissRouting = useMemo(
    () => () => {
      // Clear alert state
      clearActiveAlert('user-dismiss-close');
    },
    [clearActiveAlert]
  );

  const currentStop = demoCurrentStop;
  const nextStop = demoNextStop;

  // NOTE: disabled for deterministic demo (keep for rollback)
  // Current/next stop (legacy provider-owned derivation)
  // const currentStop = useMemo(() => {
  //   if (stops.length === 0) {
  //     console.log('[CURRENT_STOP] ⚠️ No stops available');
  //     return null;
  //   }
  //
  //   const sorted = stops
  //     .slice()
  //     .sort((a, b) => (a.current_sequence ?? 999) - (b.current_sequence ?? 999));
  //
  //   // Current is the first non-completed stop
  //   const cur =
  //     sorted.find((s) => String(s.status).toLowerCase() !== 'completed') ?? sorted[0];
  //
  //   console.log('[CURRENT_STOP] ✅ Derived current stop:', {
  //     currentStopId: cur?.stop_id,
  //     status: cur?.status,
  //     totalStops: stops.length,
  //     stopsHead: stops.slice(0, 3).map(s => ({ id: s.stop_id, seq: s.current_sequence })),
  //   });
  //   return cur ?? null;
  // }, [stops]);
  //
  // const nextStop = useMemo(() => {
  //   if (!currentStop) {
  //     console.log('[NEXT_STOP] ⚠️ No current stop, cannot determine next stop');
  //     return null;
  //   }
  //
  //   const sorted = stops
  //     .slice()
  //     .sort((a, b) => (a.current_sequence ?? 999) - (b.current_sequence ?? 999));
  //
  //   const idx = sorted.findIndex((s) => s.stop_id === currentStop.stop_id);
  //   if (idx < 0 || idx >= sorted.length - 1) {
  //     console.log('[NEXT_STOP] ⚠️ No next stop available (idx:', idx, 'total:', sorted.length, ')');
  //     return null;
  //   }
  //
  //   console.log('[NEXT_STOP] ✅ Derived next stop:', {
  //     nextStopId: sorted[idx + 1]?.stop_id,
  //     currentIdx: idx,
  //     totalStops: sorted.length,
  //     sortedHead: sorted.slice(0, 3).map(s => ({ id: s.stop_id, seq: s.current_sequence })),
  //   });
  //   return sorted[idx + 1];
  // }, [stops, currentStop]);

  const value = useMemo<FleetContextValue>(
    () => ({
      routeVariant,
      setRouteVariant,
      vehicle,
      currentEvent,
      routingDecision,
      alertStatus,
      alertBehavior,
      activeAlert,
      clearActiveAlert,
      pendingTaskSequence,
      stops: sortedStops,
      currentStop,
      nextStop,
      currentPage,
      setCurrentPage,
      acceptRouting,
      dismissRouting,
      triggerAutoSendMessage,
      clearAutoSendMessage,
      autoSendMessage,
      scheduleAutoReply,
      pendingAutoReply,
      triggerHighSeverityDemoAlert,
      triggerMediumSeverityDemoAlert,
      isAlertPaused,
      toggleAlertPause,
      bufferedAlerts,
      liveAlertHistory,
      clearLiveAlertHistory,
    }),
    [
      vehicle,
      currentEvent,
      routingDecision,
      alertStatus,
      alertBehavior,
      activeAlert,
      clearActiveAlert,
      pendingTaskSequence,
      sortedStops,
      currentStop,
      nextStop,
      currentPage,
      acceptRouting,
      dismissRouting,
      clearAutoSendMessage,
      autoSendMessage,
      scheduleAutoReply,
      pendingAutoReply,
      triggerHighSeverityDemoAlert,
      triggerMediumSeverityDemoAlert,
      isAlertPaused,
      toggleAlertPause,
      bufferedAlerts,
      liveAlertHistory,
      clearLiveAlertHistory,
      routeVariant,
    ]
  );

  return <FleetContext.Provider value={value}>{children}</FleetContext.Provider>;
}

export function useFleet() {
  const ctx = useContext(FleetContext);
  if (!ctx) throw new Error('useFleet must be used within FleetProvider');
  return ctx;
}
