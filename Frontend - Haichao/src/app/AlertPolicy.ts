import type { Incident, IncidentEvent } from './fleetTypes';

/**
 * Alert behavior type
 */
export type AlertBehavior = 'modal' | 'card' | 'toast';

/**
 * Alert policy: decide whether to show an alert based on incident severity
 */
export class AlertPolicy {
  /**
   * Decide whether an incident should trigger an alert
   * LOW: ignore and return null
   * MEDIUM: return AlertBehavior
   * HIGH: return AlertBehavior
   */
  static getAlertBehavior(incident: Incident, source: 'live' | 'demo' = 'live'): AlertBehavior | null {
    switch (incident.severity) {
      case 'LOW':
        return null; // No popup alert
      case 'MEDIUM':
        return source === 'demo' ? 'card' : 'modal';
      case 'HIGH':
        return 'modal';
      default:
        return null;
    }
  }

  /**
   * Build IncidentEvent for UI display
   */
  static buildIncidentEvent(incident: Incident): IncidentEvent {
    return {
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
        affected_tasks: incident.requires_reorder ? 17 : 0,
      },
    };
  }

  /**
   * Get auto-generated message content for HIGH severity
   */
  static generateHighSeverityMessage(incident: Incident): {
    title: string;
    content: string;
    thread_id: string;
  } {
    const timestamp = new Date().toLocaleString();
    return {
      title: `🚨 HIGH PRIORITY: ${incident.event_type.replace(/_/g, ' ')}`,
      content: `
**Incident Detected at ${timestamp}**

**Type:** ${incident.event_type.replace(/_/g, ' ')}
**Severity:** HIGH
**ETA Impact:** ${incident.eta_impact_min} minutes
**Location:** Lat ${incident.location.lat.toFixed(4)}, Lon ${incident.location.lon.toFixed(4)}
**DTC Codes:** ${incident.dtc_codes.join(', ')}
**Engine Status:** ${incident.engine_status}
**Vehicle Speed:** ${incident.gps_speed.toFixed(1)} km/h

**Action:** HQ/Dispatch has been notified. Check Messages for updates.
`.trim(),
      thread_id: `HIGH_${incident.incident_id}`,
    };
  }
}
