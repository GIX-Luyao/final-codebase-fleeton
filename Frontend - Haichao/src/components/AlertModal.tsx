import { useFleet } from '../app/FleetProvider';
import { devLogger } from '../app/logger.ts';
import { getDemoStops } from '../demo/demoStops';

const console = devLogger;

export default function AlertModal() {
  const { activeAlert } = useFleet();

  // Render strictly from activeAlert (single source of truth)
  if (!activeAlert) {
    if (import.meta.env.DEV) {
      console.log('[ALERT_MODAL][EARLY_RETURN] missing activeAlert');
    }
    return null;
  }

  const alertBehavior = activeAlert.behavior;

  if (alertBehavior !== 'modal') {
    if (import.meta.env.DEV) {
      console.log('[ALERT_MODAL][EARLY_RETURN] behavior mismatch', {
        behavior: alertBehavior,
        source: activeAlert.source,
        severity: activeAlert.severity,
        incident_id: activeAlert.incident_id,
      });
    }
    return null;
  }

  if (import.meta.env.DEV) {
    console.log('[ALERT_MODAL] Rendering popup:', {
      source: activeAlert.source,
      severity: activeAlert.severity,
      behavior: alertBehavior,
      incident_id: activeAlert.incident_id,
    });
  }

  if (activeAlert.severity === 'HIGH') {
    return <HighAlertCard />;
  }

  if (activeAlert.severity === 'MEDIUM') {
    return <MediumAlertCard />;
  }

  if (import.meta.env.DEV) {
    console.log('[ALERT_MODAL][EARLY_RETURN] source mismatch or unsupported modal payload', {
      behavior: activeAlert.behavior,
      source: activeAlert.source,
      severity: activeAlert.severity,
      incident_id: activeAlert.incident_id,
    });
  }

  return null;
}

function HighAlertCard() {
  const { currentEvent, dismissRouting, setCurrentPage, triggerAutoSendMessage, scheduleAutoReply } = useFleet();

  if (!currentEvent) return null;

  const handlePrimary = () => {
    triggerAutoSendMessage();
    scheduleAutoReply();
    setCurrentPage('messages');
    dismissRouting();
  };

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(0,0,0,0.6)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 9999,
        padding: 20,
      }}
      onClick={() => dismissRouting()}
    >
      <div
        style={{
          backgroundColor: '#ffffff',
          borderRadius: 24,
          padding: 24,
          width: 354,
          height: 263,
          boxShadow: '0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04)',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Orange Info Icon */}
        <div
          style={{
            width: 80,
            height: 80,
            backgroundColor: '#E65100',
            borderRadius: '50%',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: 48,
            color: '#ffffff',
            fontWeight: 700,
          }}
        >
          i
        </div>

        {/* Title */}
        <div style={{ fontSize: 24, fontWeight: 700, color: '#111827', textAlign: 'center', marginTop: 16 }}>
          Action Required
        </div>

        {/* Subtitle */}
        <div style={{ fontSize: 13, color: '#6b7280', textAlign: 'center', lineHeight: 1.6, marginTop: 12, flex: 1, display: 'flex', alignItems: 'center' }}>
          Rerouting due to <strong>[Reason]</strong> to stay on schedule.
        </div>

        {/* Auto-send Alert Button */}
        <button
          type="button"
          onClick={handlePrimary}
          style={{
            width: '100%',
            padding: 14,
            backgroundColor: '#E65100',
            color: '#ffffff',
            border: 'none',
            borderRadius: 12,
            fontSize: 15,
            fontWeight: 700,
            cursor: 'pointer',
            transition: 'background-color 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.target as HTMLButtonElement).style.backgroundColor = '#CC4400';
          }}
          onMouseLeave={(e) => {
            (e.target as HTMLButtonElement).style.backgroundColor = '#E65100';
          }}
        >
          Auto-send Alert
        </button>
      </div>
    </div>
  );
}

function MediumAlertCard() {
  const { acceptRouting, dismissRouting, activeAlert, routeVariant } = useFleet();

  if (!activeAlert) return null;

  // NOTE: disabled for deterministic demo (keep for rollback)
  // const stopAddress = currentStop?.address || '888 116th Ave NE, Bellevue';
  const updatedStops = getDemoStops(routeVariant === 'A' ? 'B' : routeVariant);
  const newCurrentStop = updatedStops?.[0] ?? null;
  const newCurrentStopValue =
    newCurrentStop?.address ?? newCurrentStop?.stop_id ?? 'N/A';

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(0,0,0,0.6)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 9999,
        padding: 20,
      }}
      onClick={() => dismissRouting()}
    >
      <div
        style={{
          backgroundColor: '#ffffff',
          borderRadius: 24,
          padding: 24,
          width: 354,
          minHeight: 420,
          boxShadow: '0 20px 25px -5px rgba(0,0,0,0.1), 0 10px 10px -5px rgba(0,0,0,0.04)',
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'space-between',
          gap: 18,
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Title */}
        <div style={{ fontSize: 24, fontWeight: 800, color: '#111827', textAlign: 'center', lineHeight: 1.2, whiteSpace: 'nowrap' }}>
           New Route Available
        </div>

        {/* New current stop */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            gap: 14,
            marginTop: 6,
          }}
        >
          <div
            style={{
              width: 30,
              height: 30,
              backgroundColor: '#22c55e',
              borderRadius: '50% 50% 50% 0',
              transform: 'rotate(-45deg)',
              position: 'relative',
              flexShrink: 0,
            }}
          >
            <div
              style={{
                width: 10,
                height: 10,
                backgroundColor: '#ffffff',
                borderRadius: '50%',
                position: 'absolute',
                top: 10,
                left: 10,
              }}
            />
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div style={{ fontSize: 17, fontWeight: 700, color: '#22c55e', lineHeight: 1.2 }}>
                Updated Current Stop:
            </div>
            <div style={{ fontSize: 20, fontWeight: 700, color: '#374151', lineHeight: 1.3 }}>
              {newCurrentStopValue}
            </div>
          </div>
        </div>

        {/* Description */}
        <div style={{ fontSize: 16, color: '#64748b', textAlign: 'left', lineHeight: 1.45, marginTop: 8 }}>
          Tap 'Update Route' to apply.
        </div>

        {/* Update Route Button */}
        <button
          type="button"
          onClick={() => {
            if (activeAlert.source === 'demo' && import.meta.env.DEV) {
              console.log('DEMO_MODAL_DISMISSED');
            }
            // NOTE: disabled for deterministic demo (keep for rollback)
            // if (activeAlert.source === 'demo') {
            //   dismissRouting();
            //   return;
            // }
            acceptRouting();
          }}
          title="Apply backend route update"
          style={{
            width: '100%',
            height: 60,
            backgroundColor: '#2563eb',
            color: '#ffffff',
            border: 'none',
            borderRadius: 12,
            fontSize: 16,
            fontWeight: 700,
            cursor: 'pointer',
            transition: 'background-color 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.target as HTMLButtonElement).style.backgroundColor = '#1d4ed8';
          }}
          onMouseLeave={(e) => {
            (e.target as HTMLButtonElement).style.backgroundColor = '#2563eb';
          }}
        >
          Update Route
        </button>

        {/* No thanks button */}
        <button
          type="button"
          onClick={() => {
            if (activeAlert.source === 'demo' && import.meta.env.DEV) {
              console.log('DEMO_MODAL_DISMISSED');
            }
            dismissRouting();
          }}
          style={{
            width: '100%',
            height: 60,
            backgroundColor: '#e5e7eb',
            color: '#374151',
            border: 'none',
            fontSize: 16,
            fontWeight: 700,
            cursor: 'pointer',
            textAlign: 'center',
            borderRadius: 12,
          }}
        >
          No thanks, keep current route
        </button>
      </div>
    </div>
  );
}
