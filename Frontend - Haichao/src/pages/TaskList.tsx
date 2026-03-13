import { useMemo } from 'react';
import { useFleet } from '../app/FleetProvider';
import StopTimelineCard from '../components/StopTimelineCard';

export default function TaskList() {
  const { stops, currentStop } = useFleet();

  // Get all stops with their status
  const allStops = useMemo(() => {
    if (stops.length === 0) return [];

    // Render stops in array order (no re-sorting)
    // Backend new_sequence is already applied to array order by acceptRouting
    const result = stops.map((stop) => {
      const isCompleted = String(stop.status).toLowerCase() === 'completed';
      // NOTE: disabled for deterministic demo (keep for rollback)
      // const isCurrent = !currentStop && stop.stop_id === currentStop.stop_id;
      const isCurrent = !!currentStop && stop.stop_id === currentStop.stop_id;

      return {
        ...stop,
        isCompleted,
        isCurrent,
      };
    });

    return result;
  }, [stops, currentStop]);

  return (
    <div style={{
      padding: '0px',
      backgroundColor: '#F9FAFC',
      minHeight: '100%',
      width: '100%',
      // NOTE: disabled for phone viewport layout (keep for rollback)
      // maxWidth: '390px',
      maxWidth: '100%',
      overflowX: 'hidden',
      boxSizing: 'border-box',
      margin: '0 auto',
      display: 'flex',
      flexDirection: 'column',
      alignItems: 'center',
    }}>
      {/* Stop List with Timeline Layout */}
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
        paddingTop: '16px',
        paddingBottom: '24px',
        paddingLeft: '12px',
        paddingRight: '12px',
        width: '100%',
        alignItems: 'stretch',
        boxSizing: 'border-box',
      }}>
        {allStops.map((stop, index) => (
          <StopTimelineCard
            key={stop.stop_id}
            stop={stop}
            isFirst={index === 0}
            isLast={index === allStops.length - 1}
            onCardClick={() => {
              // Keep existing click behavior if any
              //
            }}
          />
        ))}
      </div>
    </div>
  );
}
