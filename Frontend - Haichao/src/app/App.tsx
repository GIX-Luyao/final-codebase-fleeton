import { useEffect, useState } from 'react';
import DriverHome from '../pages/DriverHome';
import TaskList from '../pages/TaskList';
import MapPage from '../pages/MapPage';
import Profile from '../pages/Profile';
import Messages from '../pages/Messages';
import AlertModal from '../components/AlertModal';
import LiveAlertHistoryPanel from '../components/LiveAlertHistoryPanel';
import BottomNavigation from '../components/BottomNavigation';
import { FleetProvider, useFleet } from './FleetProvider';
import DriverHeader from '../components/DriverHeader';


type TabKey = 'home' | 'list' | 'map' | 'messages';
type RouteKey = { type: 'tab'; tab: TabKey } | { type: 'profile' };

interface AppShellProps {
  setHistoryPanelOpen: (open: boolean) => void;
}

function AppShell({ setHistoryPanelOpen }: AppShellProps) {
  const { setCurrentPage, currentPage, triggerHighSeverityDemoAlert, triggerMediumSeverityDemoAlert, liveAlertHistory, isAlertPaused, toggleAlertPause } = useFleet();
  const [route, setRoute] = useState<RouteKey>({ type: 'tab', tab: 'home' });

  // Sync current page semantics to FleetProvider for popup behavior control
  useEffect(() => {
    // Keep FleetProvider page semantics in sync with route
    const pageMap: Record<TabKey, 'home' | 'manifest' | 'map' | 'messages'> = {
      home: 'home',
      list: 'manifest',
      map: 'map',
      messages: 'messages',
    };

    const pageKey = route.type === 'profile' ? 'profile' : pageMap[route.tab];
    setCurrentPage(pageKey);
  }, [route, setCurrentPage]);

  // Listen to currentPage changes and sync route
  useEffect(() => {
    if (currentPage === 'messages') {
      setRoute({ type: 'tab', tab: 'messages' });
    }
  }, [currentPage]);

  const activeTab: TabKey = route.type === 'tab' ? route.tab : 'home';

  // Simplified tab-to-route mapping
  const handleTabChange = (tab: 'home' | 'list' | 'map' | 'message') => {
    const routeTab = tab === 'message' ? 'messages' : tab;
    setRoute({ type: 'tab', tab: routeTab as TabKey });
  };

  // activeTab used by BottomNavigation ('messages' -> 'message')
  const navActiveTab: 'home' | 'list' | 'map' | 'message' = 
    activeTab === 'messages' ? 'message' : activeTab;

  // ========== Header Logic ==========
  // Compute header title from active tab
  const titleMap: Record<TabKey, string> = {
    home: 'Home',
    list: 'List',
    map: 'Map',
    messages: 'Message',
  };

  // Get time-based greeting
  const getGreeting = () => {
    const hour = new Date().getHours();
    if (hour < 12) return 'Good Morning';
    if (hour < 18) return 'Good Afternoon';
    return 'Good Evening';
  };

  // Get formatted date (for example, Fri. Jan. 16)
  const getFormattedDate = () => {
    const now = new Date();
    const days = ['Sun.', 'Mon.', 'Tue.', 'Wed.', 'Thu.', 'Fri.', 'Sat.'];
    const months = ['Jan.', 'Feb.', 'Mar.', 'Apr.', 'May', 'Jun.', 'Jul.', 'Aug.', 'Sep.', 'Oct.', 'Nov.', 'Dec.'];
    const dayOfWeek = days[now.getDay()];
    const month = months[now.getMonth()];
    const date = now.getDate();
    return `${dayOfWeek} ${month} ${date}`;
  };

  // Derive headerTitle and headerSubtitle from route
  const headerTitle = route.type === 'tab' ? titleMap[route.tab] : '';
  const headerSubtitle =
    route.type === 'tab'
      ? route.tab === 'home'
        ? getGreeting()
        : `${getFormattedDate()} | # 18802546`
      : '';

  const showHeader = route.type === 'tab'; // Hide header on Profile page
  const showTabBar = route.type === 'tab'; // Hide tab bar on Profile page
  const isMessagesTab = route.type === 'tab' && route.tab === 'messages';
  const isListTab = route.type === 'tab' && route.tab === 'list';
  const isMapTab = route.type === 'tab' && route.tab === 'map';

  const renderContent = () => {
    if (route.type === 'profile') {
      return <Profile />;
    }

    switch (route.tab) {
      case 'home':
        return (
          <DriverHome
            onNavigateToMap={() => setRoute({ type: 'tab', tab: 'map' })}
          />
        );
      case 'list':
        return <TaskList />;
      case 'map':
        return <MapPage />;
      case 'messages':
        return <Messages />;
      default:
        return null;
    }
  };

  return (
    <div
      style={{
        // NOTE: disabled for phone viewport layout (keep for rollback)
        // minHeight: '100vh',
        height: '100vh',
        backgroundColor: 'transparent',
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'flex-start',
        padding: '12px 0',
        overflow: 'hidden',
      }}
    >
      <div
        style={{
          // NOTE: disabled for phone viewport layout (keep for rollback)
          // width: '100%',
          // maxWidth: 390,
          // margin: '0 auto',
          width: '390px',
          maxWidth: '100%',
          height: 'min(100vh, 844px)',
          backgroundColor: '#f9fafc',
          display: 'flex',
          flexDirection: 'column',
          borderRadius: 18,
          overflow: 'hidden',
          boxShadow: '0 10px 30px rgba(0,0,0,0.10)',
          // NOTE: disabled for phone viewport layout (keep for rollback)
          // minHeight: '100%',
          position: 'relative' as const,
        }}
      >
        {/* Profile standalone page: top back bar */}
        {route.type === 'profile' && (
          <div
            style={{
              flexShrink: 0,
              padding: '12px 16px',
              borderBottom: '1px solid #e5e7eb',
              display: 'flex',
              alignItems: 'center',
              gap: 12,
            }}
          >
            <button
              type="button"
              onClick={() => setRoute({ type: 'tab', tab: 'home' })}
              style={{
                width: 32,
                height: 32,
                borderRadius: '50%',
                border: 'none',
                background: 'transparent',
                cursor: 'pointer',
                color: '#111',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                fontSize: 24,
                lineHeight: 1,
                padding: 0,
              }}
              aria-label="Back"
              title="Back"
            >
              ←
            </button>

            <div style={{ fontWeight: 600 }}>Profile</div>
          </div>
        )}

        {/* Shared DriverHeader: only shown on tab pages */}
        {showHeader && (
          <div style={{ flexShrink: 0 }}>
            <DriverHeader
              title={headerTitle}
              subtitle={headerSubtitle}
              onAvatarClick={() => setRoute({ type: 'profile' })}
              // avatarUrl="/avatar.png" // Put static image in public and uncomment if needed
            />
          </div>
        )}

        {/* Page content area */}
        <div
          className="hideScrollbar"
          style={{
          flex: 1,
          // NOTE: disabled for phone viewport layout (keep for rollback)
          // overflowY: 'auto',
          overflowY: isMessagesTab || isMapTab ? 'hidden' : 'auto',
          overflowX: 'hidden',
          WebkitOverflowScrolling: 'touch',
          backgroundColor: '#f9fafc',
          // NOTE: disabled for phone viewport layout (keep for rollback)
          // paddingRight: route.type === 'profile' || isMessagesTab ? '0px' : '18px',
          // NOTE: disabled for phone viewport layout (keep for rollback)
          // paddingLeft: route.type === 'profile' || isMessagesTab ? '0px' : '18px',
          paddingRight: route.type === 'profile' || isMessagesTab || isListTab || isMapTab ? '0px' : '18px',
          paddingLeft: route.type === 'profile' || isMessagesTab || isListTab || isMapTab ? '0px' : '18px'
        }}>
          {renderContent()}
        </div>


        {showTabBar && (
          <div
            style={{
              backgroundColor: '#f9fafc',
              borderTop: '1px solid #e5e7eb',
              width: '100%',
              flexShrink: 0,
            }}
          >
            <BottomNavigation
              activeTab={navActiveTab}
              onTabChange={handleTabChange}
            />
          </div>
        )}

        {/* Floating action buttons (stacked vertically) */}
        
        {/* Play/Pause Alert Button (Top-Most) */}
        <button
          onClick={toggleAlertPause}
          style={{
            // NOTE: disabled for phone viewport layout (keep for rollback)
            // position: 'fixed',
            position: 'absolute',
            bottom: 272,
            right: 16,
            width: 56,
            height: 56,
            borderRadius: '50%',
            backgroundColor: isAlertPaused ? '#10b981' : '#ef4444',
            color: 'white',
            border: 'none',
            cursor: 'pointer',
            fontSize: 24,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: isAlertPaused ? '0 2px 8px rgba(16, 185, 129, 0.3)' : '0 2px 8px rgba(239, 68, 68, 0.3)',
            zIndex: 999,
            fontWeight: 'bold',
            transition: 'transform 0.2s, box-shadow 0.2s, background-color 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1.1)';
            const color = isAlertPaused ? 'rgba(16, 185, 129, 0.4)' : 'rgba(239, 68, 68, 0.4)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = `0 4px 12px ${color}`;
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1)';
            const color = isAlertPaused ? 'rgba(16, 185, 129, 0.3)' : 'rgba(239, 68, 68, 0.3)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = `0 2px 8px ${color}`;
          }}
          title={isAlertPaused ? 'Play - Start showing alerts' : 'Pause - Stop showing alerts'}
          aria-label={isAlertPaused ? 'Play Alerts' : 'Pause Alerts'}
        >
          {isAlertPaused ? '▶️' : '⏸️'}
        </button>

        {/* Medium-Severity Alert Demo Button (Bottom-Most - Blue) */}
        <button
          onClick={triggerMediumSeverityDemoAlert}
          style={{
            // NOTE: disabled for phone viewport layout (keep for rollback)
            // position: 'fixed',
            position: 'absolute',
            bottom: 208,
            right: 16,
            width: 56,
            height: 56,
            borderRadius: '50%',
            backgroundColor: '#2563eb',
            color: 'white',
            border: 'none',
            cursor: 'pointer',
            fontSize: 24,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 2px 8px rgba(37, 99, 235, 0.3)',
            zIndex: 999,
            fontWeight: 'bold',
            transition: 'transform 0.2s, box-shadow 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1.1)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 4px 12px rgba(37, 99, 235, 0.4)';
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 2px 8px rgba(37, 99, 235, 0.3)';
          }}
          title="Trigger Medium-Severity Alert Demo"
          aria-label="Medium-Severity Demo"
        >
          🔵
        </button>

        {/* High-Severity Alert Demo Button (Red) */}
        <button
          onClick={triggerHighSeverityDemoAlert}
          style={{
            // NOTE: disabled for phone viewport layout (keep for rollback)
            // position: 'fixed',
            position: 'absolute',
            bottom: 144,
            right: 16,
            width: 56,
            height: 56,
            borderRadius: '50%',
            backgroundColor: '#ef4444',
            color: 'white',
            border: 'none',
            cursor: 'pointer',
            fontSize: 24,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 2px 8px rgba(239, 68, 68, 0.3)',
            zIndex: 999,
            fontWeight: 'bold',
            transition: 'transform 0.2s, box-shadow 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1.1)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 4px 12px rgba(239, 68, 68, 0.4)';
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 2px 8px rgba(239, 68, 68, 0.3)';
          }}
          title="Trigger High-Severity Alert Demo"
          aria-label="High-Severity Demo"
        >
          🔴
        </button>

        {/* Live Alert History Button (Bottom - Green) */}
        <button
          onClick={() => setHistoryPanelOpen(true)}
          style={{
            // NOTE: disabled for phone viewport layout (keep for rollback)
            // position: 'fixed',
            position: 'absolute',
            bottom: 80,
            right: 16,
            width: 56,
            height: 56,
            borderRadius: '50%',
            backgroundColor: liveAlertHistory.length > 0 ? '#10b981' : '#d1d5db',
            color: 'white',
            border: 'none',
            cursor: 'pointer',
            fontSize: 24,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            boxShadow: '0 2px 8px rgba(16, 185, 129, 0.3)',
            zIndex: 999,
            fontWeight: 'bold',
            transition: 'transform 0.2s, box-shadow 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1.1)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 4px 12px rgba(16, 185, 129, 0.4)';
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.transform = 'scale(1)';
            (e.currentTarget as HTMLButtonElement).style.boxShadow = '0 2px 8px rgba(16, 185, 129, 0.3)';
          }}
          title={`Live Alert History (${liveAlertHistory.length})`}
          aria-label="Live Alert History"
        >
          📋
        </button>
      </div>
    </div>
  );
}

function App() {
  const [historyPanelOpen, setHistoryPanelOpen] = useState(false);

  return (
    <FleetProvider>
      <AppShell setHistoryPanelOpen={setHistoryPanelOpen} />
      {/* Global modal controlled by FleetProvider (show/hide + accept/dismiss) */}
      <AlertModal />
      
      {/* Live alert history panel */}
      <LiveAlertHistoryPanel 
        isOpen={historyPanelOpen}
        onClose={() => setHistoryPanelOpen(false)}
      />
    </FleetProvider>
  );
}

export default App;