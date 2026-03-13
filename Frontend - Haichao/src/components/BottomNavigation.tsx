import { Home, List, Map, MessageSquare } from 'lucide-react';

type TabKey = 'home' | 'list' | 'map' | 'message';

interface BottomNavigationProps {
  activeTab: TabKey;
  onTabChange: (tab: TabKey) => void;
}

export default function BottomNavigation({ activeTab, onTabChange }: BottomNavigationProps) {
  // Removed useMemo: static style objects do not need memoization
  const styles = {
    container: {
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      padding: '24px 18px 32px 18px',
      gap: '12px',
      width: '100%',
      boxSizing: 'border-box',
    } as const,
    tabContainer: {
      display: 'flex',
      flexDirection: 'column' as const,
      alignItems: 'center',
      justifyContent: 'center',
      gap: '6px',
      cursor: 'pointer',
      transition: 'all 0.2s ease',
      padding: '12px 16px',
      borderRadius: '8px',
      position: 'relative' as const,
      width: '80px',
      minHeight: '50px',
    } as const,
    tabActive: {
      backgroundColor: '#2663EB',
    } as const,
    tabInactive: {
      backgroundColor: '#f9fafc',
    } as const,
    iconContainer: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      width: '28px',
      height: '28px',
      position: 'relative' as const,
    } as const,
    label: {
      fontSize: '12px',
      letterSpacing: '0.2px',
    } as const,
    labelActive: {
      color: '#ffffff',
      fontWeight: 700,
    } as const,
    labelInactive: {
      color: '#63748a',
      fontWeight: 500,
    } as const,
    iconActive: {
      color: '#ffffff',
      strokeWidth: 2,
    } as const,
    iconInactive: {
      color: '#63748a',
      strokeWidth: 2,
    } as const,
  };

  const tabs = [
    { key: 'home' as TabKey, label: 'Home', icon: Home },
    { key: 'list' as TabKey, label: 'List', icon: List },
    { key: 'map' as TabKey, label: 'Map', icon: Map },
    { key: 'message' as TabKey, label: 'Message', icon: MessageSquare },
  ];

  return (
    <div style={styles.container}>
      {tabs.map((tab) => {
        const isActive = activeTab === tab.key;
        const Icon = tab.icon;

        return (
          <div
            key={tab.key}
            style={{
              ...styles.tabContainer,
              ...(isActive ? styles.tabActive : styles.tabInactive),
            }}
            onClick={() => onTabChange(tab.key)}
            title={`Go to ${tab.label}`}
          >
            <div style={styles.iconContainer}>
              <Icon
                size={40}
                style={isActive ? styles.iconActive : styles.iconInactive}
              />
            </div>
            <div
              style={{
                ...styles.label,
                ...(isActive ? styles.labelActive : styles.labelInactive),
              }}
            >
              {tab.label}
            </div>
          </div>
        );
      })}
    </div>
  );
}
