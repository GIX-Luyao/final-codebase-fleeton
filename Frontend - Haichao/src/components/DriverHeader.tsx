// components/DriverHeader.tsx
import profileAvatarIcon from '../assets/icons/profile-avatar.png';

type DriverHeaderProps = {
  title: string;
  subtitle: string;
  onAvatarClick?: () => void;
  avatarUrl?: string; // Optional: static image
};

export default function DriverHeader({ title, subtitle, onAvatarClick, avatarUrl }: DriverHeaderProps) {
  return (
    <div
      style={{
        padding: '24px 18px 12px 18px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'flex-start',
        gap: 16,
        backgroundColor: '#f9fafc',
      }}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
        <div style={{ fontSize: 28, fontWeight: 700, lineHeight: 1, color: '#000' }}>
          {title}
        </div>
        <div style={{ fontSize: 12, fontWeight: 600, color: '#6b7280' }}>
          {subtitle}
        </div>
      </div>

      {/* Avatar + dot wrapper (defines dot positioning area) */}
      <div style={{ position: 'relative', width: 60, height: 60, flexShrink: 0 }}>
        <button
          type="button"
          onClick={onAvatarClick}
          style={{
            width: 60,
            height: 60,
            borderRadius: '50%',
            border: 'none',
            padding: 0,
            cursor: 'pointer',
            background: 'transparent',
            overflow: 'hidden',
            display: 'block',
          }}
          title="View Profile"
          aria-label="View Profile"
        >
          {avatarUrl ? (
            <img
              src={avatarUrl}
              alt="Driver avatar"
              style={{ width: '100%', height: '100%', objectFit: 'cover', borderRadius: '50%' }}
            />
          ) : (
            <img
              src={profileAvatarIcon}
              alt="Driver avatar"
              style={{ width: '100%', height: '100%', objectFit: 'cover', borderRadius: '50%' }}
            />
          )}
        </button>

        {/* Online dot: positioned relative to wrapper */}
        <div
          style={{
            position: 'absolute',
            bottom: -2, // Use a negative value to place it farther outside; use 0 to keep inside
            right: -2,
            width: 16,
            height: 16,
            borderRadius: '50%',
            background: '#10b981',
            border: '2px solid #f9fafc',
            pointerEvents: 'none',
          }}
        />
      </div>
    </div>
  );
}
