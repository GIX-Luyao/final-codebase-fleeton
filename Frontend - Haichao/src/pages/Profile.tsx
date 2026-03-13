import notificationsIcon from '../assets/icons/notifications.svg';
import settingsIcon from '../assets/icons/settings.svg';
import darkModeIcon from '../assets/icons/dark-mode.svg';
import logoutIcon from '../assets/icons/logout.svg';
import profileAvatarIcon from '../assets/icons/profile-avatar.png';

export default function Profile() {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        gap: '24px',
        paddingTop: '24px',
        paddingBottom: '32px',
        paddingLeft: '18px',
        paddingRight: '18px',
        backgroundColor: '#f9fafc',
        width: '100%',
        boxSizing: 'border-box',
      }}
    >
      {/* Avatar, Name, Vehicle */}
      <div
        style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          gap: '12px',
        }}
      >
        <div
          style={{
            width: '120px',
            height: '120px',
            borderRadius: '50%',
            backgroundColor: '#f9fafc',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            overflow: 'hidden',
          }}
        >
          <img
            src={profileAvatarIcon}
            alt="Driver avatar"
            style={{ width: '100%', height: '100%', objectFit: 'cover', borderRadius: '50%' }}
          />
        </div>

        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: '20px', fontWeight: '600', color: '#111' }}>
            Michael Chen
          </div>
          <div style={{ fontSize: '14px', color: '#666', marginTop: '4px' }}>
            Assigned vehicle: #123123
          </div>
        </div>
      </div>

      {/* Stats Card */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr 1fr',
          gap: '12px',
          backgroundColor: '#fff',
          padding: '16px',
          borderRadius: '8px',
          borderWidth: '1px',
          borderStyle: 'solid',
          borderColor: '#e5e7eb',
        }}
      >
        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: '12px', color: '#666', marginBottom: '4px' }}>
            Rating
          </div>
          <div style={{ fontSize: '18px', fontWeight: '600', color: '#111' }}>
            4.9
          </div>
        </div>

        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: '12px', color: '#666', marginBottom: '4px' }}>
            Deliveries
          </div>
          <div style={{ fontSize: '18px', fontWeight: '600', color: '#111' }}>
            1,220
          </div>
        </div>

        <div style={{ textAlign: 'center' }}>
          <div style={{ fontSize: '12px', color: '#666', marginBottom: '4px' }}>
            On time
          </div>
          <div style={{ fontSize: '18px', fontWeight: '600', color: '#111' }}>
            98%
          </div>
        </div>
      </div>

      {/* Menu Items */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '0px' }}>
        {/* Alert History */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '16px 0',
            borderBottom: '1px solid #e5e7eb',
            cursor: 'pointer',
            transition: 'background-color 0.2s',
          }}
          onMouseEnter={(e) =>
            (e.currentTarget.style.backgroundColor = '#f3f4f6')
          }
          onMouseLeave={(e) =>
            (e.currentTarget.style.backgroundColor = 'transparent')
          }
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <img src={notificationsIcon} alt="alert" style={{ width: '24px', height: '24px' }} />
            <span style={{ fontSize: '14px', color: '#111', fontWeight: '500' }}>
              Alert History
            </span>
          </div>
          <span style={{ fontSize: '16px', color: '#999' }}>›</span>
        </div>

        {/* Preference */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '16px 0',
            borderBottom: '1px solid #e5e7eb',
            cursor: 'pointer',
            transition: 'background-color 0.2s',
          }}
          onMouseEnter={(e) =>
            (e.currentTarget.style.backgroundColor = '#f3f4f6')
          }
          onMouseLeave={(e) =>
            (e.currentTarget.style.backgroundColor = 'transparent')
          }
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <img src={settingsIcon} alt="settings" style={{ width: '24px', height: '24px' }} />
            <span style={{ fontSize: '14px', color: '#111', fontWeight: '500' }}>
              Preference
            </span>
          </div>
          <span style={{ fontSize: '16px', color: '#999' }}>›</span>
        </div>

        {/* Dark Mode */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '16px 0',
            borderBottom: '1px solid #e5e7eb',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <img src={darkModeIcon} alt="dark mode" style={{ width: '24px', height: '24px' }} />
            <span style={{ fontSize: '14px', color: '#111', fontWeight: '500' }}>
              Dark Mode
            </span>
          </div>
          <div
            style={{
              width: '44px',
              height: '24px',
              backgroundColor: '#ccc',
              borderRadius: '12px',
              cursor: 'pointer',
              position: 'relative',
              transition: 'background-color 0.3s',
            }}
          >
            <div
              style={{
                width: '20px',
                height: '20px',
                backgroundColor: '#fff',
                borderRadius: '50%',
                position: 'absolute',
                top: '2px',
                left: '2px',
                transition: 'left 0.3s',
              }}
            />
          </div>
        </div>

        {/* Log Out */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            padding: '16px 0',
            cursor: 'pointer',
            transition: 'background-color 0.2s',
          }}
          onMouseEnter={(e) =>
            (e.currentTarget.style.backgroundColor = '#f3f4f6')
          }
          onMouseLeave={(e) =>
            (e.currentTarget.style.backgroundColor = 'transparent')
          }
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <img src={logoutIcon} alt="logout" style={{ width: '24px', height: '24px' }} />
            <span style={{ fontSize: '14px', color: '#111', fontWeight: '500' }}>
              Log Out
            </span>
          </div>
          <span style={{ fontSize: '16px', color: '#999' }}>›</span>
        </div>
      </div>
    </div>
  );
}
