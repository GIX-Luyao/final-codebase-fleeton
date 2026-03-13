import { useMemo } from 'react';
import type { DeliveryStop } from '../app/fleetTypes';
import { getDemoStops, type DemoRouteVariant } from '../demo/demoStops';

export type RouteVariant = DemoRouteVariant;

export function useDemoStops(routeVariant: RouteVariant): {
  stops: DeliveryStop[];
  sortedStops: DeliveryStop[];
  currentStop: DeliveryStop | null;
  nextStop: DeliveryStop | null;
} {
  const stops = useMemo(() => getDemoStops(routeVariant), [routeVariant]);

  const sortedStops = useMemo(
    () =>
      stops
        .slice()
        .sort((a, b) => (a.current_sequence ?? 999) - (b.current_sequence ?? 999)),
    [stops]
  );

  const currentStop = useMemo(() => {
    if (sortedStops.length === 0) return null;
    return (
      sortedStops.find((stop) => String(stop.status).toLowerCase() !== 'completed') ??
      sortedStops[0] ??
      null
    );
  }, [sortedStops]);

  const nextStop = useMemo(() => {
    if (!currentStop) return null;
    const currentIndex = sortedStops.findIndex((stop) => stop.stop_id === currentStop.stop_id);
    if (currentIndex < 0 || currentIndex >= sortedStops.length - 1) return null;
    return sortedStops[currentIndex + 1];
  }, [sortedStops, currentStop]);

  return {
    stops,
    sortedStops,
    currentStop,
    nextStop,
  };
}
