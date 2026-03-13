import { useEffect, useMemo } from 'react';
import { useFleet } from '../app/FleetProvider';
import identificationBadgeIcon from '../assets/icons/identification-badge.png';
import driverAvatarIcon from '../assets/icons/driver-avatar.png';
import deliveryTruckSpeedIcon from '../assets/icons/delivery-truck-speed.svg';
import packageIcon from '../assets/icons/package.svg';
import scanIcon from '../assets/icons/scan.svg';
import shareEtaIcon from '../assets/icons/share-eta.svg';

interface DriverHomeProps {
  onNavigateToMap: () => void;
}

export default function DriverHome({ onNavigateToMap }: DriverHomeProps) {
  const { currentStop, nextStop, stops, setCurrentPage, vehicle } = useFleet();

  // Set current page for global logic
  useEffect(() => {
    setCurrentPage('home');
  }, [setCurrentPage]);

  const styles = useMemo(
    () => ({
      container: {
        padding: '18px 0px 18px 0px',
        display: 'flex',
        flexDirection: 'column' as const,
        gap: '18px',
        backgroundColor: '#f9fafc',
        minHeight: '100%',
      },
      profileButton: {
        width: '48px',
        height: '48px',
        borderRadius: '50%',
        backgroundColor: 'transparent',
        border: 'none',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        position: 'relative' as const,
        transition: 'box-shadow 0.2s, transform 0.2s',
        boxShadow: '0 2px 8px rgba(0, 0, 0, 0.08)',
        padding: 0,
        overflow: 'hidden' as const,
      },
      profileDot: {
        position: 'absolute' as const,
        bottom: '4px',
        right: '4px',
        width: '14px',
        height: '14px',
        borderRadius: '50%',
        backgroundColor: '#10b981',
        border: '2px solid #ffffff',
      },
      // Driver Profile Card
      profileTopRow: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        gap: '16px',
      } as const,

      profileTopLeft: {
        display: 'flex',
        flexDirection: 'column' as const,
        gap: '12px',
      } as const,

      profileNameBig: {
        fontSize: '24px',   
        fontWeight: 800,
        color: '#0f172a',
        letterSpacing: '-0.5px',
        lineHeight: 1.05,
      } as const,

      verifiedRow: {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
      } as const,

      verifiedIcon: {
        width: '18px',
        height: '18px',
        borderRadius: '100px',
        backgroundColor: '#2563eb',
        color: '#fff',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontWeight: 800,
        fontSize: '10px',
      } as const,

      verifiedText: {
        fontSize: '12px',
        fontWeight: 700,
        color: '#64748b',
      } as const,

      profileAvatarLarge: {
        width: '80px',
        height: '80px',
        borderRadius: '50%',
        overflow: 'hidden',
        border: '1px solid rgba(15, 23, 42, 0.15)',
        boxSizing: 'border-box' as const,
        flexShrink: 0,
      } as const,

      profileAvatarImg: {
        width: '100%',
        height: '100%',
        objectFit: 'cover' as const,
        display: 'block',
      } as const,

      profileDivider: {
        height: '1px',
        backgroundColor: '#e5e7eb',
        margin: '18px 0',
      } as const,

      profileBottomRow: {
        display: 'flex',
        alignItems: 'center',
        gap: '16px',
      } as const,

      vehicleIconBox: {
        width: '44px',
        height: '44px',
        borderRadius: '16px',
        backgroundColor: '#eef2ff',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: '20px',
        flexShrink: 0,
      } as const,

      vehicleTextBlock: {
        display: 'flex',
        flexDirection: 'column' as const,
        gap: '10px',
        flex: 1,
      } as const,

      vehicleLine: {
        fontSize: '12px',
        lineHeight: 1.2,
      } as const,

      vehicleLabel: {
        fontWeight: 800,
        color: '#475569',
      } as const,

      vehicleValue: {
        fontWeight: 500,
        color: '#64748b',
      } as const,

      chevron: {
        fontSize: '40px',
        color: '#94a3b8',
        lineHeight: 1,
        marginLeft: '8px',
      } as const,

      // Driver Profile Card
      profileCard: {
        backgroundColor: '#ffffff',
        borderRadius: '16px',
        padding: '12px',
        boxShadow: '0 1px 3px rgba(0, 0, 0, 0.1)',
        width: '100%',
        boxSizing: 'border-box' as const,
      } as const,

      // Current Task Card
      card: {
        backgroundColor: '#ffffff',
        borderRadius: '16px',
        padding: '12px',
        boxShadow: '0 1px 3px rgba(0, 0, 0, 0.1)',
        width: '100%',
        boxSizing: 'border-box' as const,
      },
      currentTaskCard: {
        backgroundColor: '#ffffff',
        borderRadius: '16px',
        padding: '12px',
        boxShadow: '0 1px 3px rgba(0, 0, 0, 0.1)',
        border: 'none',
        position: 'relative' as const,
        width: '100%',
        boxSizing: 'border-box' as const,
      },
      cardLabel: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
        fontSize: '14px',
        fontWeight: 600,
        padding: '0',
        borderRadius: '0',
        marginBottom: '16px',
        textTransform: 'none' as const,
        letterSpacing: '0',
      },
      currentLabel: {
        backgroundColor: 'transparent',
        color: '#10b981',
      },
      nextLabel: {
        backgroundColor: 'transparent',
        color: '#2663eb',
      },
      addressText: {
        fontSize: '20px',
        fontWeight: 700,
        color: '#000000',
        marginBottom: '16px',
        lineHeight: '1.4',
      },
      infoPills: {
        display: 'grid',
        gridTemplateColumns: '1fr 1fr', 
        gap: '12px',
        marginTop: '12px',
        marginBottom: '16px',
      } as const,

      infoTile: {
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        backgroundColor: '#eff6ff',
        borderRadius: '12px',
        padding: '12px 8px',
      } as const,

      infoTileFull: {
        gridColumn: '1 / -1',
      } as const,

      tileIconBox: {
        width: '44px',
        height: '44px',
        borderRadius: '10px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        flexShrink: 0,
        fontSize: '22px',
      } as const,

      tileText: {
        display: 'flex',
        flexDirection: 'column' as const,
        gap: '4px',
        minWidth: 0,
      } as const,

      tileLabel: {
        fontSize: '12px',
        fontWeight: 600,
        color: '#94a3b8',
        textTransform: 'uppercase' as const,
        letterSpacing: '0.4px',
        lineHeight: 1,
      } as const,

      tileValue: {
        fontSize: '16px',
        fontWeight: 800,
        color: '#0f172a',
        lineHeight: 1.1,
        whiteSpace: 'nowrap' as const,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
      } as const,

      navigationButton: {
        backgroundColor: '#2663EB',
        color: '#ffffff',
        border: 'none',
        borderRadius: '12px',
        padding: '16px 20px',
        fontSize: '18px',
        fontWeight: 600,
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '8px',
        width: '100%',
        transition: 'background-color 0.2s, box-shadow 0.2s',
        boxShadow: '0 2px 8px rgba(38, 99, 235, 0.2)',
        height: '56px',
      },
      
      nextTaskChevron: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        marginBottom: '14px',
      },
      
      taskCardHeader: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        marginBottom: '10px',
      } as const,

      nextTaskTitle: {
        fontSize: '14px',
        fontWeight: 800,
        color: '#2563eb', 
      } as const,

      chevronSmall: {
        fontSize: '28px',
        color: '#94a3b8',
        lineHeight: 1,
      } as const,

      nextTaskAddress: {
        fontSize: '20px',
        fontWeight: 800,
        color: '#0f172a',
        lineHeight: 1.2,
        marginBottom: '14px',
      } as const,

      infoPillsSingle: {
        display: 'grid',
        gridTemplateColumns: '1fr',
        gap: '12px',
      } as const,


      mapPlaceholder: {
        width: '100%',
        height: '180px',
        backgroundColor: '#e5e7eb',
        borderRadius: '10px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: '#9ca3af',
        fontSize: '14px',
        cursor: 'pointer',
        transition: 'background-color 0.2s',
      },
    }),
    [stops]
  );

  const formatAddress = (stop: typeof currentStop) => {
    if (!stop) return '';
    return stop.address || '';
  };

  const formatTimeWindow = (stop: typeof currentStop) => {
    if (!stop) return '';

    if (stop.planned_time_start && stop.planned_time_end) {
      const start = new Date(stop.planned_time_start);
      const end = new Date(stop.planned_time_end);
      if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return '';
      return `${start.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} - ${end.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
    }

    const etaRaw = (stop as any).eta as string | undefined;
    if (etaRaw) {
      const date = new Date(etaRaw);
      if (Number.isNaN(date.getTime())) return '';
      const start = new Date(date.getTime() - 15 * 60000);
      const end = new Date(date.getTime() + 15 * 60000);
      return `${start.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })} - ${end.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
    }

    return '';
  };

  return (
    <div style={styles.container}>
      {/* Header is managed by AppShell */}

      {/* Driver Profile Card */}
      <div style={styles.profileCard}>
        {/* Top Row */}
        <div style={styles.profileTopRow}>
          <div style={styles.profileTopLeft}>
            <div style={styles.profileNameBig}>Michael Chen</div>

            <div style={styles.verifiedRow}>
              {/* Verified icon */}
              <img src={identificationBadgeIcon} alt="Verified" style={{width: '18px', height: '18px'}} />
              <div style={styles.verifiedText}>Verified Driver</div>
            </div>
          </div>

          {/* Avatar */}
          <div style={styles.profileAvatarLarge}>
            <img
              src={driverAvatarIcon}
              alt="Driver avatar"
              style={styles.profileAvatarImg}
            />
          </div>
        </div>

        {/* Divider */}
        <div style={styles.profileDivider} />

        {/* Bottom Row */}
        <div style={styles.profileBottomRow}>
          <div style={styles.vehicleIconBox}>
            <img src={deliveryTruckSpeedIcon} alt="truck" style={{ width: '24px', height: '24px' }} />
          </div>

          <div style={styles.vehicleTextBlock}>
            <div style={styles.vehicleLine}>
              <span style={styles.vehicleLabel}>Vehicle ID:</span>{' '}
              <span style={styles.vehicleValue}>{vehicle.vehicle_id}</span>
            </div>

            <div style={styles.vehicleLine}>
              <span style={styles.vehicleLabel}>Status:</span>{' '}
              <span style={styles.vehicleValue}>{vehicle.status || 'ON_ROUTE'}</span>
            </div>

            <div style={styles.vehicleLine}>
              <span style={styles.vehicleLabel}>Updated:</span>{' '}
              <span style={styles.vehicleValue}>
                {new Date(vehicle.last_update_at).toLocaleTimeString()}
              </span>
            </div>

            {vehicle.last_location && (
              <div style={styles.vehicleLine}>
                <span style={styles.vehicleLabel}>Location:</span>{' '}
                <span style={styles.vehicleValue}>
                  {vehicle.last_location.lat.toFixed(4)}, {vehicle.last_location.lon.toFixed(4)}
                </span>
              </div>
            )}
          </div>

          <div style={styles.chevron}>›</div>
        </div>
      </div>


      {/* Current Task Card */}
      {currentStop && (
        <div style={styles.currentTaskCard}>
          <div style={{ ...styles.cardLabel, ...styles.currentLabel }}>
            Current Task
          </div>

          <div style={styles.addressText}>
            {formatAddress(currentStop)}
          </div>

          {/* Info Tiles */}
          <div style={styles.infoPills}>
            {/* Load Tile */}
            {currentStop?.package_count !== undefined && (
              <div style={styles.infoTile}>
                <div style={styles.tileIconBox}>
                  <img src={packageIcon} alt="package" style={{ width: '24px', height: '24px' }} />
                </div>
                <div style={styles.tileText}>
                  <div style={styles.tileLabel}>Load</div>
                  <div style={styles.tileValue}>
                    {currentStop.package_count}
                    {typeof currentStop.package_count === 'number' ? '' : ''}
                  </div>
                </div>
              </div>
            )}

            {/* Scan Tile */}
            {currentStop?.package_count !== undefined && (
              <div style={styles.infoTile}>
                <div style={styles.tileIconBox}>
                  <img src={scanIcon} alt="package" style={{ width: '24px', height: '24px' }} />
                </div>
                <div style={styles.tileText}>
                  <div style={styles.tileLabel}>Scan</div>
                  <div style={styles.tileValue}>
                    {currentStop.package_count}
                    {typeof currentStop.package_count === 'number' ? '' : ''}
                  </div>
                </div>
              </div>
            )}

            {/* Window Tile */}
            {formatTimeWindow(currentStop) && (
              <div style={{ ...styles.infoTile, ...styles.infoTileFull }}>
                <div style={styles.tileIconBox}>
                  <img src={shareEtaIcon} alt="window" style={{ width: '24px', height: '24px' }} />
                </div>
                <div style={styles.tileText}>
                  <div style={styles.tileLabel}>Window</div>
                  <div style={styles.tileValue}>{formatTimeWindow(currentStop)}</div>
                </div>
              </div>
            )}
          </div>


          {/* Navigation Button */}
          <button
            style={styles.navigationButton}
            onClick={onNavigateToMap}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = '#1e4fa8';
              e.currentTarget.style.boxShadow = '0 4px 16px rgba(38, 99, 235, 0.4)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.backgroundColor = '#2663EB';
              e.currentTarget.style.boxShadow = '0 2px 8px rgba(38, 99, 235, 0.2)';
            }}
          >
            Navigation
          </button>
        </div>
      )}

      {/* Next Task Preview Card */}
      {nextStop && (
        <div style={styles.card}>
          {/* Header */}
          <div style={styles.taskCardHeader}>
            <div style={styles.nextTaskTitle}>Next Task</div>
            <div style={styles.chevronSmall}>›</div>
          </div>

          {/* Address */}
          <div style={styles.nextTaskAddress}>
            {formatAddress(nextStop)}
          </div>

          {/* Window Tile */}
          <div style={styles.infoPillsSingle}>
            {formatTimeWindow(nextStop) && (
              <div style={styles.infoTile}>
                <div style={styles.tileIconBox}>
                  <img src={shareEtaIcon} alt="window" style={{ width: '24px', height: '24px' }} />
                </div>
                <div style={styles.tileText}>
                  <div style={styles.tileLabel}>Window</div>
                  <div style={styles.tileValue}>{formatTimeWindow(nextStop)}</div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

    </div>
  );
}
