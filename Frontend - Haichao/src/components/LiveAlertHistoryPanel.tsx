import { useState } from 'react';
import { useFleet } from '../app/FleetProvider';

interface LiveAlertHistoryPanelProps {
  isOpen: boolean;
  onClose: () => void;
}

export default function LiveAlertHistoryPanel({ isOpen, onClose }: LiveAlertHistoryPanelProps) {
  const { liveAlertHistory, clearLiveAlertHistory } = useFleet();
  const [expandedId, setExpandedId] = useState<string | null>(null);

  if (!isOpen) {
    return null;
  }

  const handleClear = () => {
    if (window.confirm('Clear all live alert history?')) {
      clearLiveAlertHistory();
    }
  };

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        backgroundColor: 'rgba(0, 0, 0, 0.4)',
        display: 'flex',
        alignItems: 'flex-end',
        zIndex: 1000,
      }}
      onClick={onClose}
    >
      <div
        style={{
          backgroundColor: '#ffffff',
          borderTopLeftRadius: 24,
          borderTopRightRadius: 24,
          width: '100%',
          maxHeight: '80vh',
          display: 'flex',
          flexDirection: 'column',
          boxShadow: '0 -4px 12px rgba(0, 0, 0, 0.1)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          style={{
            padding: '20px 24px',
            borderBottom: '1px solid #e5e7eb',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}
        >
          <div style={{ fontSize: 18, fontWeight: 800, color: '#0f172a' }}>
            Live Alert History
          </div>
          <button
            onClick={onClose}
            style={{
              backgroundColor: 'transparent',
              border: 'none',
              fontSize: 24,
              cursor: 'pointer',
              color: '#6b7280',
              padding: '0 8px',
            }}
            aria-label="Close"
          >
            ✕
          </button>
        </div>

        {/* Content */}
        <div
          style={{
            flex: 1,
            overflowY: 'auto',
            padding: '12px 0',
          }}
        >
          {liveAlertHistory.length === 0 ? (
            <div
              style={{
                padding: '40px 24px',
                textAlign: 'center',
                color: '#9ca3af',
              }}
            >
              <div style={{ fontSize: 14 }}>No live alerts received yet.</div>
            </div>
          ) : (
            <div>
              {liveAlertHistory.map((item) => {
                const isExpanded = expandedId === item.incident_id;
                const severityColor = item.severity === 'HIGH' ? '#dc2626' : '#f59e0b';
                const severityBg = item.severity === 'HIGH' ? '#fee2e2' : '#fef3c7';

                return (
                  <div key={item.incident_id}>
                    {/* List Item */}
                    <button
                      onClick={() => setExpandedId(isExpanded ? null : item.incident_id)}
                      style={{
                        width: '100%',
                        padding: '12px 16px',
                        backgroundColor: 'transparent',
                        border: 'none',
                        borderBottom: '1px solid #f3f4f6',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '12px',
                        textAlign: 'left',
                        transition: 'background-color 0.2s',
                      }}
                      onMouseEnter={(e) => {
                        (e.currentTarget as HTMLButtonElement).style.backgroundColor = '#f9fafc';
                      }}
                      onMouseLeave={(e) => {
                        (e.currentTarget as HTMLButtonElement).style.backgroundColor = 'transparent';
                      }}
                    >
                      {/* Severity Badge */}
                      <div
                        style={{
                          width: 28,
                          height: 28,
                          borderRadius: '50%',
                          backgroundColor: severityBg,
                          color: severityColor,
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'center',
                          fontSize: 12,
                          fontWeight: 700,
                          flexShrink: 0,
                        }}
                      >
                        {item.severity.charAt(0)}
                      </div>

                      {/* Item Info */}
                      <div style={{ flex: 1, minWidth: 0 }}>
                        <div
                          style={{
                            fontSize: 14,
                            fontWeight: 600,
                            color: '#1f2937',
                            marginBottom: 4,
                          }}
                        >
                          {item.title || item.event_type.replace(/_/g, ' ')}
                        </div>
                        <div
                          style={{
                            fontSize: 12,
                            color: '#6b7280',
                            display: 'flex',
                            gap: 8,
                            justifyContent: 'space-between',
                          }}
                        >
                          <span>{item.event_type}</span>
                          <span>{new Date(item.timestamp).toLocaleTimeString()}</span>
                        </div>
                      </div>

                      {/* Expand Arrow */}
                      <div
                        style={{
                          color: '#9ca3af',
                          fontSize: 16,
                          transition: 'transform 0.2s',
                          transform: isExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                        }}
                      >
                        ▼
                      </div>
                    </button>

                    {/* Expanded Detail */}
                    {isExpanded && (
                      <div
                        style={{
                          padding: '16px 24px',
                          backgroundColor: '#f9fafc',
                          borderBottom: '1px solid #e5e7eb',
                        }}
                      >
                        <div style={{ fontSize: 12, color: '#4b5563' }}>
                          {item.description && (
                            <div style={{ marginBottom: 8 }}>
                              <span style={{ fontWeight: 600 }}>Description:</span> {item.description}
                            </div>
                          )}

                          <div style={{ marginBottom: 8 }}>
                            <span style={{ fontWeight: 600 }}>Incident ID:</span> {item.incident_id}
                          </div>

                          {item.vehicle_id && (
                            <div style={{ marginBottom: 8 }}>
                              <span style={{ fontWeight: 600 }}>Vehicle:</span> {item.vehicle_id}
                            </div>
                          )}

                          <div style={{ marginBottom: 8 }}>
                            <span style={{ fontWeight: 600 }}>Timestamp:</span> {new Date(item.timestamp).toLocaleString()}
                          </div>

                          {item.gps_speed !== undefined && (
                            <div style={{ marginBottom: 8 }}>
                              <span style={{ fontWeight: 600 }}>GPS Speed:</span> {item.gps_speed} km/h
                            </div>
                          )}

                          {item.engine_status && (
                            <div style={{ marginBottom: 8 }}>
                              <span style={{ fontWeight: 600 }}>Engine Status:</span> {item.engine_status}
                            </div>
                          )}

                          {item.eta_impact_min !== undefined && (
                            <div style={{ marginBottom: 8 }}>
                              <span style={{ fontWeight: 600 }}>ETA Impact:</span> +{item.eta_impact_min} min
                            </div>
                          )}

                          {item.dtc_codes && item.dtc_codes.length > 0 && (
                            <div>
                              <span style={{ fontWeight: 600 }}>DTC Codes:</span> {item.dtc_codes.join(', ')}
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Footer */}
        {liveAlertHistory.length > 0 && (
          <div
            style={{
              padding: '12px 16px',
              borderTop: '1px solid #e5e7eb',
              display: 'flex',
              gap: 12,
            }}
          >
            <button
              onClick={onClose}
              style={{
                flex: 1,
                padding: '12px 16px',
                backgroundColor: 'transparent',
                color: '#6b7280',
                border: '1px solid #d1d5db',
                borderRadius: 10,
                fontSize: 14,
                fontWeight: 600,
                cursor: 'pointer',
              }}
            >
              Close
            </button>
            <button
              onClick={handleClear}
              style={{
                flex: 1,
                padding: '12px 16px',
                backgroundColor: '#ef4444',
                color: '#ffffff',
                border: 'none',
                borderRadius: 10,
                fontSize: 14,
                fontWeight: 600,
                cursor: 'pointer',
              }}
            >
              Clear
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
