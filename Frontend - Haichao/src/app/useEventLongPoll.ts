import { useEffect, useRef } from 'react';
import { devLogger } from './logger.ts';

const console = devLogger;

type Cursor = {
  after_time?: string;
  after_id?: string;
};

const CURSOR_STORAGE_KEY = 'fleet.long_poll.cursor.v1';
const CURSOR_CLOCK_SKEW_TOLERANCE_MS = 2 * 60 * 1000;
const CURSOR_RESET_LOOKBACK_MS = 5 * 60 * 1000;
const DEFAULT_AFTER_ID = '00000000-0000-0000-0000-000000000000';

function toBackendIsoSeconds(value?: string): string {
  const ms = parseIsoMs(value);
  const base = ms === null ? new Date() : new Date(ms);
  return base.toISOString().replace(/\.\d{3}Z$/, 'Z');
}
function createSafeDefaultCursor(): Cursor {
  return {
    after_time: new Date(Date.now() - CURSOR_RESET_LOOKBACK_MS).toISOString(),
    after_id: undefined,
  };
}

function parseIsoMs(value?: string): number | null {
  if (!value || typeof value !== 'string') return null;
  const ms = Date.parse(value);
  return Number.isNaN(ms) ? null : ms;
}

function isFutureCursor(afterTime?: string): boolean {
  const ms = parseIsoMs(afterTime);
  if (ms === null) return false;
  return ms > Date.now() + CURSOR_CLOCK_SKEW_TOLERANCE_MS;
}

function normalizeCursor(input: unknown): Cursor {
  const candidate = (input ?? {}) as Record<string, unknown>;

  const after_time_raw =
    (typeof candidate.after_time === 'string' ? candidate.after_time : undefined) ??
    (typeof candidate.afterTime === 'string' ? candidate.afterTime : undefined);
  const after_id_raw =
    (typeof candidate.after_id === 'string' ? candidate.after_id : undefined) ??
    (typeof candidate.afterId === 'string' ? candidate.afterId : undefined);

  const parsedMs = parseIsoMs(after_time_raw);
  if (parsedMs === null) {
    return createSafeDefaultCursor();
  }

  const normalized: Cursor = {
    after_time: new Date(parsedMs).toISOString(),
    after_id: after_id_raw || undefined,
  };

  if (isFutureCursor(normalized.after_time)) {
    return createSafeDefaultCursor();
  }

  return normalized;
}

function loadCursorFromStorage(): Cursor {
  if (typeof window === 'undefined') {
    return createSafeDefaultCursor();
  }

  try {
    const raw = window.localStorage.getItem(CURSOR_STORAGE_KEY);
    if (!raw) return createSafeDefaultCursor();
    return normalizeCursor(JSON.parse(raw));
  } catch {
    return createSafeDefaultCursor();
  }
}

function persistCursorToStorage(cursor: Cursor) {
  if (typeof window === 'undefined') return;
  const normalized = normalizeCursor(cursor);
  try {
    window.localStorage.setItem(CURSOR_STORAGE_KEY, JSON.stringify(normalized));
  } catch {
    // Ignore storage failures.
  }
}

function pickServerTimestamp(item: any): string | undefined {
  if (!item) return undefined;

  const candidates = [
    item?.incident?.timestamp,
    item?.incident?.updated_at,
    item?.incident?.last_update_at,
    item?.event?.timestamp,
    item?.event?.updated_at,
    item?.event?.last_update_at,
    item?.vehicle_status?.timestamp,
    item?.vehicle_status?.updated_at,
    item?.vehicle_status?.last_update_at,
  ];

  return candidates.find((value) => typeof value === 'string');
}

function pickServerEventId(item: any): string | undefined {
  if (!item) return undefined;
  const candidates = [
    item?.incident?.incident_id,
    item?.incident?.event_id,
    item?.incident?.id,
    item?.event?.event_id,
    item?.event?.id,
    item?.vehicle_status?.event_id,
    item?.vehicle_status?.id,
  ];
  const value = candidates.find((entry) => typeof entry === 'string');
  return typeof value === 'string' ? value : undefined;
}

function pickResponseCursor(data: any): Cursor | undefined {
  if (!data || typeof data !== 'object') return undefined;

  const topAfterTime = typeof data.next_after_time === 'string' ? data.next_after_time : undefined;
  const topAfterId = typeof data.next_after_id === 'string' ? data.next_after_id : undefined;

  const cursorObj = (data as any).cursor;
  const cursorAfterTime =
    (typeof cursorObj?.after_time === 'string' ? cursorObj.after_time : undefined) ??
    (typeof cursorObj?.next_after_time === 'string' ? cursorObj.next_after_time : undefined) ??
    (typeof cursorObj?.afterTime === 'string' ? cursorObj.afterTime : undefined);
  const cursorAfterId =
    (typeof cursorObj?.after_id === 'string' ? cursorObj.after_id : undefined) ??
    (typeof cursorObj?.next_after_id === 'string' ? cursorObj.next_after_id : undefined) ??
    (typeof cursorObj?.afterId === 'string' ? cursorObj.afterId : undefined);

  const lastId = typeof (data as any).last_id === 'string' ? (data as any).last_id : undefined;

  const after_time = topAfterTime ?? cursorAfterTime;
  const after_id = topAfterId ?? cursorAfterId ?? lastId;

  if (!after_time && !after_id) return undefined;
  return { after_time, after_id };
}

export interface EventItem {
  [key: string]: unknown;
  incident_id?: string;
  vehicle_id?: string;
  event_type?: string;
  severity?: string;
  timestamp?: string;
  gps_speed?: number;
  engine_status?: string;
  eta_impact_min?: number;
  requires_reorder?: boolean;
  location?: { lat: number; lon: number };
  reason?: string;
  description?: string;
}

interface BackendTask {
  task_id: string;
  order: number;
  address?: string;
  planned_time_start?: string;
  planned_time_end?: string;
  status?: string;
  package_count?: number;
}

interface EventResponse {
  item: {
    incident: EventItem | null;
    vehicle_status?: unknown;
    original_task_sequence?: BackendTask[];
    stops?: BackendTask[];
  } | null;
  next_after_time: string | null;
  next_after_id: string | null;
}

interface EventPayload {
  incident?: EventItem;
  vehicle_status?: unknown;
  original_task_sequence?: BackendTask[];
  stops?: BackendTask[];
  __traceId?: string;
}

interface UseEventLongPollOptions {
  vehicleId: string;
  onEvent: (payload: EventPayload) => void;
  waitS?: number;
}

export function useEventLongPoll({
  vehicleId,
  onEvent,
  waitS = 20,
}: UseEventLongPollOptions) {
  const DEV_RESET_CURSOR_NOW_ON_LOAD = true;
  const abortControllerRef = useRef<AbortController | null>(null);
  const cursorRef = useRef<Cursor>(loadCursorFromStorage());
  const isPollingRef = useRef(true);
  const pollStartedRef = useRef(false); // Prevent duplicate startups during StrictMode
  const hasLoggedPayloadStructureRef = useRef(false); // One-time debug log flag
  const hasWarnedFutureCursorResetRef = useRef(false);
  const hasWarnedBadRequestResetRef = useRef(false);

  // Store onEvent callback in a ref so polling effect is NOT affected by onEvent changes
  const onEventRef = useRef(onEvent);

  // Isolated effect: only updates ref and does not restart polling
  useEffect(() => {
    console.log('[LONG_POLL] onEvent ref updated (this does not restart polling)');
    onEventRef.current = onEvent;
  }, [onEvent]); // This effect is isolated from polling effect

  useEffect(() => {
    if (import.meta.env.DEV) {
      console.log('[LONG_POLL] Effect started', { vehicleId, waitS });
    }

    const setCursor = (nextCursor: Cursor) => {
      const normalized = normalizeCursor(nextCursor);
      cursorRef.current = normalized;
      persistCursorToStorage(normalized);
    };

    const resetCursorToNow = () => {
      const nowCursor: Cursor = {
        after_time: new Date().toISOString(),
        after_id: undefined,
      };
      cursorRef.current = nowCursor;
      if (typeof window !== 'undefined') {
        try {
          window.localStorage.removeItem(CURSOR_STORAGE_KEY);
        } catch {
          // Ignore storage failures.
        }
      }
      if (import.meta.env.DEV) {
        console.log('[CURSOR_RESET_NOW]', nowCursor);
      }
    };

    // Re-validate persisted cursor on hook start (handles hard refresh and old format migration).
    setCursor(cursorRef.current);

    if (import.meta.env.DEV && DEV_RESET_CURSOR_NOW_ON_LOAD) {
      resetCursorToNow();
    }

    // StrictMode fix: prevent double startup during mount/cleanup/mount
    if (pollStartedRef.current) {
      console.log('[LONG_POLL] Poll already started in this mount cycle (StrictMode re-mount?), skipping');
      return () => {
        console.log('[LONG_POLL] Cleanup from skipped effect');
      };
    }

    pollStartedRef.current = true;
    isPollingRef.current = true;
    abortControllerRef.current = new AbortController();

    // Sequential polling loop - only one request in-flight at a time
    const pollLoop = async () => {
      while (isPollingRef.current) {
        try {
          // Hard guard: if cursor drifts into the future, reset cursor to a safe server-catchup window.
          if (isFutureCursor(cursorRef.current.after_time)) {
            if (!hasWarnedFutureCursorResetRef.current) {
              console.warn('[LONG_POLL] Cursor after_time in the future, resetting cursor', {
                old_after_time: cursorRef.current.after_time,
              });
              hasWarnedFutureCursorResetRef.current = true;
            }
            setCursor(createSafeDefaultCursor());
          }

          const requestAfterTime =
            toBackendIsoSeconds(
              cursorRef.current.after_time ?? new Date(Date.now() - CURSOR_RESET_LOOKBACK_MS).toISOString()
            );
          const requestAfterId =
            typeof cursorRef.current.after_id === 'string' && cursorRef.current.after_id.trim().length > 0
              ? cursorRef.current.after_id
              : DEFAULT_AFTER_ID;

          if (import.meta.env.DEV) {
            console.log('[CURSOR_SENT]', {
              after_time: requestAfterTime,
              after_id: requestAfterId,
            });
            console.log('[LONG_POLL] Request params', {
              vehicle_id: vehicleId,
              after_time: requestAfterTime,
              after_id: requestAfterId,
              wait_s: waitS,
            });
          }

          const params = new URLSearchParams({
            vehicle_id: vehicleId,
            after_time: requestAfterTime,
            after_id: requestAfterId,
            wait_s: String(waitS),
          });

          const url = `https://15eqsal673.execute-api.us-west-2.amazonaws.com/event?${params}`;

          // AWAIT the fetch - this blocks until response is received
          const response = await fetch(url, {
            signal: abortControllerRef.current!.signal,
          });

          // Log response status
          if (!response.ok) {
            if (response.status === 400) {
              if (!hasWarnedBadRequestResetRef.current) {
                console.warn('[LONG_POLL] HTTP 400 detected, resetting cursor to safe default');
                hasWarnedBadRequestResetRef.current = true;
              }
              setCursor(createSafeDefaultCursor());
            }
            console.warn(`[LONG_POLL] HTTP ${response.status}, waiting 500ms before retry`);
            if (isPollingRef.current) {
              await new Promise((resolve) => setTimeout(resolve, 500));
            }
            continue; // Continue to next iteration after delay
          }

          // AWAIT json parsing - parse before updating cursor
          const data: EventResponse = await response.json();

          // Log response structure AFTER successful parse
          const hasIncident = data.item !== null && data.item?.incident !== null;
          
          // Diagnostic: inspect vehicle_status for plan/stops fields
          const vehicleStatus = (data.item?.vehicle_status ?? (data.item as any)?.vehicleStatus ?? null) as any;
          const vehicleStatusKeys = vehicleStatus ? Object.keys(vehicleStatus) : [];
          const hasPlanFields = vehicleStatusKeys.some(k => 
            k.includes('plan') || k.includes('stop') || k.includes('task') || k.includes('active')
          );
          
          const serverTimestampUsed = pickServerTimestamp(data.item);
          const serverEventIdUsed = pickServerEventId(data.item);
          const responseCursor = pickResponseCursor(data as any);

          if (import.meta.env.DEV) {
            console.log('[LONG_POLL] Response received', {
              hasIncident,
              hasVehicleStatus: !!vehicleStatus,
              vehicleStatusKeys: vehicleStatusKeys.slice(0, 10),
              hasPlanFields,
              severity: data.item?.incident ? (data.item.incident as any).severity : null,
              serverTimestampUsed,
              serverEventIdUsed,
            });
          }

          // Check for event data (incident, tasks/stops, or vehicle status) and call callback
          const hasTasks = data.item?.original_task_sequence && Array.isArray(data.item.original_task_sequence) && data.item.original_task_sequence.length > 0;
          const hasStops = data.item?.stops && Array.isArray(data.item.stops) && data.item.stops.length > 0;
          const hasVehicleStatus = !!vehicleStatus;
          const shouldCallCallback = hasIncident || hasTasks || hasStops || hasVehicleStatus;

          if (shouldCallCallback) {
            const traceId = `lp-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
            // One-time debug: inspect payload structure for GPS coordinates
            if (!hasLoggedPayloadStructureRef.current && data.item) {
              hasLoggedPayloadStructureRef.current = true;
              
              console.log('🔍 [GPS_DEBUG] ========== ONE-TIME PAYLOAD STRUCTURE INSPECTION ==========');
              console.log('[GPS_DEBUG] item keys:', Object.keys(data.item));
              
              // Check original_task_sequence
              if (data.item.original_task_sequence && Array.isArray(data.item.original_task_sequence)) {
                const tasks = data.item.original_task_sequence;
                console.log(`[GPS_DEBUG] original_task_sequence: ${tasks.length} tasks found`);
                
                tasks.slice(0, 2).forEach((task: any, idx: number) => {
                  console.log(`[GPS_DEBUG] task[${idx}] keys:`, Object.keys(task));
                  console.log(`[GPS_DEBUG] task[${idx}] coordinate candidates:`, {
                    'task.location?.lat': task.location?.lat,
                    'task.location?.lon': task.location?.lon,
                    'task.gps?.lat': task.gps?.lat,
                    'task.gps?.lon': task.gps?.lon,
                    'task.lat': task.lat,
                    'task.lon': task.lon,
                    'task.latitude': task.latitude,
                    'task.longitude': task.longitude,
                    'task.lng': task.lng,
                    'task.coords?.lat': task.coords?.lat,
                    'task.coords?.lon': task.coords?.lon,
                    'task.position?.lat': task.position?.lat,
                    'task.position?.lon': task.position?.lon,
                    'task.stop_location?.lat': task.stop_location?.lat,
                    'task.stop_location?.lon': task.stop_location?.lon,
                  });
                });
              }
              
              // Check stops (alternative field name)
              if (data.item.stops && Array.isArray(data.item.stops)) {
                const stops = data.item.stops;
                console.log(`[GPS_DEBUG] stops: ${stops.length} stops found`);
                
                stops.slice(0, 2).forEach((stop: any, idx: number) => {
                  console.log(`[GPS_DEBUG] stop[${idx}] keys:`, Object.keys(stop));
                  console.log(`[GPS_DEBUG] stop[${idx}] coordinate candidates:`, {
                    'stop.location?.lat': stop.location?.lat,
                    'stop.location?.lon': stop.location?.lon,
                    'stop.gps?.lat': stop.gps?.lat,
                    'stop.gps?.lon': stop.gps?.lon,
                    'stop.lat': stop.lat,
                    'stop.lon': stop.lon,
                    'stop.latitude': stop.latitude,
                    'stop.longitude': stop.longitude,
                    'stop.lng': stop.lng,
                    'stop.coords?.lat': stop.coords?.lat,
                    'stop.coords?.lon': stop.coords?.lon,
                    'stop.position?.lat': stop.position?.lat,
                    'stop.position?.lon': stop.position?.lon,
                    'stop.stop_location?.lat': stop.stop_location?.lat,
                    'stop.stop_location?.lon': stop.stop_location?.lon,
                  });
                });
              }
              
              // Summary verdict
              const hasTaskCoords = data.item.original_task_sequence?.some((t: any) => 
                t.location?.lat || t.gps?.lat || t.lat || t.latitude || t.coords?.lat || t.position?.lat || t.stop_location?.lat
              );
              const hasStopCoords = data.item.stops?.some((s: any) => 
                s.location?.lat || s.gps?.lat || s.lat || s.latitude || s.coords?.lat || s.position?.lat || s.stop_location?.lat
              );
              
              if (hasTaskCoords || hasStopCoords) {
                console.log('✅ [GPS_DEBUG] VERDICT: Backend payload INCLUDES GPS coordinates');
              } else {
                console.log('❌ [GPS_DEBUG] VERDICT: Backend payload DOES NOT include GPS coordinates');
              }
              console.log('[GPS_DEBUG] ========== END PAYLOAD INSPECTION ==========');
            }
            
            const payload: EventPayload = {
              incident: data.item?.incident || undefined,
              vehicle_status: vehicleStatus || undefined,
              original_task_sequence: data.item?.original_task_sequence || (data.item as any)?.tasks,
              stops: data.item?.stops,
              __traceId: traceId,
            };
            if (import.meta.env.DEV) {
              const incident = payload.incident as any;
              const incidentTs = incident?.timestamp ?? incident?.start_time ?? null;
              console.log('[LONG_POLL] Event received (hasIncident: ' + hasIncident + ', hasTasks: ' + hasTasks + ', hasStops: ' + hasStops + ', hasVehicleStatus: ' + hasVehicleStatus + '), calling onEvent', {
                traceId,
                count: hasIncident ? 1 : 0,
                incident_id: incident?.incident_id ?? incident?.event_id ?? incident?.id ?? null,
                severity: incident?.severity ?? null,
                timestamp: incidentTs,
                source: 'live',
                behavior: null,
                isAlertPaused: null,
                dedupeHit: null,
                bufferLen: null,
                queueLen: null,
                hasVehicleStatus: !!payload.vehicle_status,
                hasOriginalTaskSequence: !!payload.original_task_sequence,
                hasStops: !!payload.stops,
                taskCount: payload.original_task_sequence?.length || payload.stops?.length || 0,
              });
            }
            onEventRef.current(payload);
          } else {
            // Empty timeout: keep last known good cursor unchanged.
            if (import.meta.env.DEV) {
              console.log('[LONG_POLL] Poll timeout (no incident, no tasks, no stops)');
            }
          }

          const fallbackCursorFromIncident: Cursor | undefined = serverTimestampUsed
            ? { after_time: serverTimestampUsed, after_id: serverEventIdUsed ?? undefined }
            : undefined;

          const cursorCandidate = responseCursor ?? fallbackCursorFromIncident;
          const cursorReason = responseCursor ? 'response_cursor' : fallbackCursorFromIncident ? 'incident_fallback' : null;

          if (cursorCandidate && cursorReason) {
            const currentAfterTime = cursorRef.current.after_time;
            const currentAfterId = cursorRef.current.after_id;
            const currentMs = parseIsoMs(currentAfterTime) ?? -Infinity;
            const candidateMs = parseIsoMs(cursorCandidate.after_time);

            if (candidateMs !== null) {
              if (candidateMs <= Date.now() + CURSOR_CLOCK_SKEW_TOLERANCE_MS && candidateMs >= currentMs) {
                const nextCursor: Cursor = {
                  after_time: new Date(candidateMs).toISOString(),
                  after_id: cursorCandidate.after_id ?? currentAfterId,
                };
                setCursor(nextCursor);
                if (import.meta.env.DEV) {
                  console.log('[CURSOR_UPDATED]', {
                    reason: cursorReason,
                    prev_after_time: currentAfterTime,
                    prev_after_id: currentAfterId,
                    next_after_time: nextCursor.after_time,
                    next_after_id: nextCursor.after_id,
                  });
                }
              }
            } else if (cursorCandidate.after_id && currentAfterTime) {
              const nextCursor: Cursor = {
                after_time: currentAfterTime,
                after_id: cursorCandidate.after_id,
              };
              setCursor(nextCursor);
              if (import.meta.env.DEV) {
                console.log('[CURSOR_UPDATED]', {
                  reason: `${cursorReason}_id_only`,
                  prev_after_time: currentAfterTime,
                  prev_after_id: currentAfterId,
                  next_after_time: nextCursor.after_time,
                  next_after_id: nextCursor.after_id,
                });
              }
            }
          }

          // Loop continues naturally to next iteration (no explicit recursion)
        } catch (err) {
          // Critical fix: handle AbortError (cleanup only - do not restart)
          // AbortError can be thrown by fetch() or response.json()
          if (err instanceof Error && (err.name === 'AbortError' || err.message === 'The operation was aborted')) {
            if (import.meta.env.DEV) {
              console.log('[LONG_POLL] AbortError caught - cleanup or dependency change');
            }
            break; // Exit the loop gracefully, do NOT retry or restart
          }

          // Log all other errors
          const errorMsg = err instanceof Error ? err.message : String(err);
          const errorName = err instanceof Error ? err.name : 'Unknown';
          console.error('[LONG_POLL] Fetch/parse error', {
            name: errorName,
            message: errorMsg,
          });

          // Wait before retry
          if (isPollingRef.current) {
            await new Promise((resolve) => setTimeout(resolve, 500));
          }
          // Continue to next iteration (retry)
        }
      }
      if (import.meta.env.DEV) {
        console.log('[LONG_POLL] Poll loop exited');
      }
    };

    // Start the sequential polling loop
    pollLoop().catch((err) => {
      // Critical fix: swallow AbortError during cleanup because it is expected
      if (err instanceof Error && (err.name === 'AbortError' || err.message === 'The operation was aborted')) {
        if (import.meta.env.DEV) {
          console.log('[LONG_POLL] AbortError swallowed in catch handler - this is expected during cleanup');
        }
        return; // Silently ignore AbortError
      }
      
      // Log any other unexpected errors
      console.error('[LONG_POLL] Unhandled error in pollLoop:', err);
      // Do NOT re-throw - we want to gracefully handle all errors without crashing the provider
    });

    return () => {
      if (import.meta.env.DEV) {
        console.log('[LONG_POLL] Cleanup triggered - stopping poll');
      }
      isPollingRef.current = false;
      
      // Critical fix: wrap abort() in try-catch to prevent throw during cleanup
      // In rare cases, abort() or pending requests might throw AbortError
      try {
        abortControllerRef.current?.abort();
      } catch (err) {
        // Silently ignore any errors during abort - cleanup should never throw
        if (import.meta.env.DEV) {
          console.log('[LONG_POLL] Silent cleanup: abort() threw, but this is safe during cleanup');
        }
      }
      
      // Reset pollStartedRef so next mount cycle can start fresh
      pollStartedRef.current = false;
    };
  }, [vehicleId, waitS]); // Only vehicleId and waitS trigger restarts
}
