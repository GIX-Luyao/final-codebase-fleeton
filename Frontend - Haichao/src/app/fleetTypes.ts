export type VehicleStatusCode = 'ON_ROUTE' | 'IDLE' | 'OFFLINE';

export interface LatLon {
  lat: number;
  lon: number;
}

export interface VehicleRealtimeStatus {
  vehicle_id: string;
  status: VehicleStatusCode;
  last_location: LatLon;
  last_update_at: string;
}

export type EventType =
  | 'TRAFFIC_JAM'
  | 'ACCIDENT'
  | 'BREAKDOWN'
  | 'ROUTE_DEVIATION'
  | 'ETA_DELAY_RISK';

export type EventSeverity = 'LOW' | 'MEDIUM' | 'HIGH';

export interface IncidentImpact {
  eta_delay_minutes: number;
  affected_tasks: number;
}

export interface IncidentEvent {
  event_id: string;
  vehicle_id: string;
  event_type: EventType;
  severity: EventSeverity;
  source?: 'live' | 'demo';
  reason?: string;
  location: LatLon;
  start_time: string;
  end_time: string | null;
  gps_speed?: number;
  engine_status?: string;
  dtc_codes?: string[];
  eta_impact_min: number;
  requires_reorder: boolean;
  impact: IncidentImpact;
}

export interface SuggestedRoute {
  route_id?: string;
  polyline: string;
  total_distance_km: number;
  total_eta_minutes: number;
}

export interface RoutingDecisionCore {
  required_reroute: boolean;
  before_eta: string;
  after_eta: string;
  eta_delta_minutes: number;
  improvement_ratio: number;
  suggested_route: SuggestedRoute;
}

export interface RoutingDecisionResult {
  vehicle_id: string;
  routing_decision: RoutingDecisionCore;
}

export interface TaskSequenceItem {
  task_id: string;
  order: number;
  eta: string;
}

export interface EventUpdatePayload {
  vehicle_id: string;
  event_type: EventType;
  severity: EventSeverity;
  routing_decision: RoutingDecisionCore & {
    task_sequence: TaskSequenceItem[];
  };
}

export interface EventUpdateMessage {
  type: 'EVENT_UPDATE';
  payload: EventUpdatePayload;
}

// WebSocket message for incident updates (matching new database schema)
export interface IncidentUpdateMessage {
  type: 'INCIDENT_UPDATE';
  payload: {
    incident: Incident;
    resequence_result?: ResequenceResult;
  };
}

export type AlertStatus = 'pending' | 'accepted' | 'dismissed';

// Database schema types
export interface Vehicle {
  vehicle_id: string;
  status: VehicleStatusCode;
  last_location: LatLon;
  last_update_at: string;
}

export interface DeliveryPlan {
  plan_id: string;
  vehicle_id: string;
  status: string;
  created_at: string;
}

export interface DeliveryStop {
  stop_id: string;
  plan_id: string;
  vehicle_id?: string;
  original_sequence: number;
  current_sequence: number;
  address: string;
  planned_time_start: string;
  planned_time_end: string;
  current_time_start: string | null;
  current_time_end: string | null;
  status: string;
  package_count?: number;
  // Legacy field for backward compatibility
  stop_order?: number;
  eta?: string;
  completed_at?: string;
  // GPS coordinates from backend
  location?: LatLon;
  gps?: LatLon;
  lat?: number;
  lon?: number;
}

export interface Incident {
  incident_id: string;
  vehicle_id: string;
  event_type: EventType;
  severity: EventSeverity;
  start_time: string;
  end_time: string | null;
  location: LatLon;
  gps_speed: number;
  engine_status: string;
  dtc_codes: string[];
  eta_impact_min: number;
  requires_reorder: boolean;
  decision?: 'accepted' | 'dismissed';
  decided_at?: string;
}

export interface ResequenceResult {
  result_id: string;
  incident_id: string;
  plan_id: string;
  old_sequence: Array<{
    stop_id: string;
    order: number;
  }>;
  new_sequence: Array<{
    stop_id: string;
    order: number;
  }>;
  total_eta_before: number;
  improvement_pct: number;
  created_at: string;
}
