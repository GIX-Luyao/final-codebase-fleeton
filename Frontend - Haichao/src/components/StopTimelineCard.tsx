import type { DeliveryStop } from '../app/fleetTypes';

interface StopTimelineCardProps {
  stop: DeliveryStop & { isCompleted: boolean; isCurrent: boolean };
  isFirst: boolean;
  isLast?: boolean;
  onCardClick?: () => void;
}

export default function StopTimelineCard({ stop, isFirst, onCardClick }: StopTimelineCardProps) {
  const { isCompleted, isCurrent } = stop;
  const seq = stop.current_sequence ?? stop.stop_order ?? 0;
  

  // Helper: Extract GPS coordinates from stop (supported fields only)
  const getStopLatLon = (stop: DeliveryStop): { lat: number; lon: number } | null => {
    if (stop.location?.lat != null && stop.location?.lon != null) {
      return { lat: stop.location.lat, lon: stop.location.lon };
    }
    if (stop.gps?.lat != null && stop.gps?.lon != null) {
      return { lat: stop.gps.lat, lon: stop.gps.lon };
    }
    if (stop.lat != null && stop.lon != null) {
      return { lat: stop.lat, lon: stop.lon };
    }
    return null;
  };

  // Format address - use address or default
  const formatStopName = (stop: DeliveryStop) => {
    return stop.address || `Stop ${seq}`;
  };

  // Format GPS coordinates for subtitle (SAFE - only calls toFixed on numbers)
  const formatGpsCoordinates = (stop: DeliveryStop): string => {
    const coords = getStopLatLon(stop);
    if (coords && typeof coords.lat === 'number' && typeof coords.lon === 'number') {
      return `GPS: ${coords.lat.toFixed(4)}, ${coords.lon.toFixed(4)}`;
    }
    return 'GPS: —';
  };

  // Format time window
  const formatTimeWindow = (stop: DeliveryStop) => {
    const startStr = stop.planned_time_start ?? stop.eta;
    const endStr = stop.planned_time_end ?? '';

    if (startStr && endStr) {
      const s = new Date(startStr);
      const e = new Date(endStr);
      if (!Number.isNaN(s.getTime()) && !Number.isNaN(e.getTime())) {
        return `ETA: ${s.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} - ${e.toLocaleTimeString([], {
          hour: '2-digit',
          minute: '2-digit',
        })}`;
      }
    }

    if (startStr) {
      const d = new Date(startStr);
      if (!Number.isNaN(d.getTime())) {
        const s = new Date(d.getTime() - 15 * 60000);
        const e = new Date(d.getTime() + 15 * 60000);
        return `ETA: ${s.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} - ${e.toLocaleTimeString([], {
          hour: '2-digit',
          minute: '2-digit',
        })}`;
      }
    }

    return '';
  };

  // Determine pill text and colors
  const getPillInfo = () => {
    if (isCompleted) {
      return {
        text: 'Completed',
        bgColor: '#e5e7eb',
        textColor: '#9ca3af',
      };
    }
    if (isCurrent) {
      return {
        text: 'In Processing',
        bgColor: '#E8FCF0',
        textColor: '#20B457',
      };
    }
    return {
      text: 'Next Stop',
      bgColor: '#E8FCF0',
      textColor: '#668870',
    };
  };

  const pill = getPillInfo();

  // Determine dot and card styles
  let cardBgColor: string;
  let cardBorder: string = 'none';
  let cardBoxShadow: string;

  if (isCompleted) {
    cardBgColor = '#f9fafb';
    cardBoxShadow = '0 1px 2px rgba(0, 0, 0, 0.05)';
  } else if (isCurrent) {
    cardBgColor = '#ffffff';
    cardBorder = '2px solid #20B457';
    cardBoxShadow = '0 4px 12px rgba(32, 180, 87, 0.15)';
  } else {
    cardBgColor = '#ffffff';
    cardBoxShadow = '0 2px 8px rgba(0, 0, 0, 0.08)';
  }

  // Timeline styles handled inline where used

  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'flex-start',
        // NOTE: disabled for phone viewport layout (keep for rollback)
        // width: '390px',
        width: '100%',
        maxWidth: '100%',
        gap: '12px',
        boxSizing: 'border-box',
        background: 'transparent',
        marginBottom: 0,
        cursor: onCardClick ? 'pointer' : 'default',
        opacity: isCompleted ? 0.7 : 1,
        transition: 'box-shadow 0.2s',
      }}
      onClick={onCardClick}
    >
      {/* Pin centered with 18px margins on both sides */}
      <div style={{
        // NOTE: disabled for phone viewport layout (keep for rollback)
        // marginLeft: '18px',
        // NOTE: disabled for phone viewport layout (keep for rollback)
        // marginRight: '18px',
        width: '30px',
        flexShrink: 0,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        position: 'relative',
      }}>
        <div style={{
          width: '30px',
          height: '30px',
          marginTop: isFirst ? '18px' : '0',
          marginBottom: '0',
          zIndex: 2,
        }}>
          {/* Pin icon: switch SVG/image by status */}
          {isCompleted ? (
            <img src="/task-status-completed.svg" alt="completed" style={{ width: '100%', height: '100%' }} />
          ) : isCurrent ? (
            <img src="/task-status-current.svg" alt="current" style={{ width: '100%', height: '100%' }} />
          ) : (
            <img src="/task-status-next.svg" alt="next" style={{ width: '100%', height: '100%' }} />
          )}
        </div>
      </div>
      

      

      {/* Right Card Column - card width 306px + right margin 18px */}
      <div style={{
        // NOTE: disabled for phone viewport layout (keep for rollback)
        // width: '306px',
        // NOTE: disabled for phone viewport layout (keep for rollback)
        // marginRight: '18px',
        flex: 1,
        width: 'auto',
        maxWidth: '100%',
        flexShrink: 1,
        minWidth: 0,
      }}>
        <div
          onClick={onCardClick}
          style={{
            width: '100%',
            boxSizing: 'border-box',
            backgroundColor: cardBgColor,
            borderRadius: '12px',
            padding: '16px 16px 16px 16px',
            border: cardBorder,
            boxShadow: cardBoxShadow,
            cursor: onCardClick ? 'pointer' : 'default',
            transition: 'all 0.2s ease',
            position: 'relative',
            overflow: 'hidden',
          }}
          onMouseEnter={(e) => {
            if (onCardClick) {
              (e.currentTarget as HTMLDivElement).style.transform = 'translateY(-2px)';
            }
          }}
          onMouseLeave={(e) => {
            if (onCardClick) {
              (e.currentTarget as HTMLDivElement).style.transform = 'translateY(0)';
            }
          }}
        >
          {/* Status Pill */}
          <div
            style={{
              display: 'inline-block',
              backgroundColor: pill.bgColor,
              color: pill.textColor,
              padding: '4px 10px',
              borderRadius: '6px',
              fontSize: '13px',
              fontWeight: 600,
              marginBottom: '10px',
            }}
          >
            {pill.text}
          </div>

          {/* Title/Name */}
          <div
            style={{
              fontSize: '20px',
              fontWeight: 800,
              color: isCurrent ? '#334156' : '#5B6B7F',
              marginBottom: '6px',
              wordBreak: 'break-word',
              overflowWrap: 'break-word',
            }}
          >
            {formatStopName(stop)}
          </div>

          {/* GPS Coordinates Subtitle */}
          <div
            style={{
              fontSize: '12px',
              color: '#5B6B7F',
              fontWeight: 400,
              marginBottom: '10px',
              maxWidth: '100%',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {formatGpsCoordinates(stop)}
          </div>

          {/* Bottom info: ETA and Packages */}
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
            }}
          >
            <div
              style={{
                fontSize: '12px',
                color: '#5B6B7F',
                fontWeight: 400,
              }}
            >
              {formatTimeWindow(stop)}
            </div>

            <div
              style={{
                fontSize: '12px',
                fontWeight: 400,
                color: '#5B6B7F',
              }}
            >
              {stop.package_count ?? 7} Pkgs
            </div>
          </div>

          {/* Chevron removed */}
        </div>
      </div>
    </div>
  );
}
