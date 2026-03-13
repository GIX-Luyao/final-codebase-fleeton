import { useEffect, useRef, useState } from 'react';
import { useFleet } from '../app/FleetProvider';
import { devLogger } from '../app/logger.ts';

const console = devLogger;

type Sender = 'system' | 'other' | 'me';

type Severity = 'low' | 'medium' | 'high';
type MessageSource = 'manual' | 'auto-send' | 'reply';

type ChatMessage = {
  id: string;
  sender: Sender;
  text: string;
  timestamp?: string; // Optional timestamp for outgoing messages
  // Track message origin and severity for proper styling
  source?: MessageSource; // Where the message came from
  severity?: Severity; // Severity level (for auto-send alerts)
};

// ─────────────────────────────────────────────────────────────────
// Message Variant Classifier
// Determines visual rendering style based on message metadata
// ─────────────────────────────────────────────────────────────────
type MessageVariant = 'outgoing' | 'incoming' | 'system';

function getMessageVariant(msg: ChatMessage): MessageVariant {
  // Rule 1: Manual outgoing messages (sender: 'me')
  if (msg.sender === 'me') {
    return 'outgoing';
  }

  // Rule 2: Auto-send messages with HIGH severity → render as blue outgoing
  // (Dispatcher-style) so they stand out as urgent/important
  if (msg.source === 'auto-send' && msg.severity === 'high') {
    return 'outgoing';
  }

  // Rule 3: Incoming messages from dispatcher (sender: 'other')
  if (msg.sender === 'other') {
    return 'incoming';
  }

  // Rule 4: Default to system (gray, centered) for everything else
  // - System messages (sender: 'system')
  // - Auto-send with low/medium severity
  // - Any other edge cases
  return 'system';
}

// ─────────────────────────────────────────────────────────────────
// Message Component: System Messages (neutral gray, centered)
// ─────────────────────────────────────────────────────────────────
function SystemMessage({ text }: { text: string }) {
  return (
    <div
      style={{
        display: 'flex',
        justifyContent: 'center',
        marginBottom: 8,
      }}
    >
      <div
        style={{
          maxWidth: '85%',
          backgroundColor: '#f3f4f6',
          color: '#374151',
          padding: '10px 14px',
          borderRadius: 12,
          fontSize: 14,
          lineHeight: 1.4,
          wordBreak: 'break-word',
          overflowWrap: 'anywhere',
          textAlign: 'center',
        }}
      >
        {text}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// Message Component: Outgoing Messages (blue #007AFF, right-aligned)
// ─────────────────────────────────────────────────────────────────
function OutgoingMessage({ text, timestamp }: { text: string; timestamp?: string }) {
  return (
    <div
      style={{
        display: 'flex',
        justifyContent: 'flex-end',
        marginBottom: 4,
      }}
    >
      <div
        style={{
          maxWidth: '75%',
          backgroundColor: '#007AFF',
          color: '#ffffff',
          padding: '10px 14px',
          borderRadius: 18,
          fontSize: 15,
          lineHeight: 1.4,
          wordBreak: 'break-word',
          overflowWrap: 'anywhere',
        }}
      >
        {text}
      </div>
      {timestamp && (
        <div
          style={{
            alignSelf: 'flex-end',
            marginLeft: 8,
            fontSize: 12,
            color: '#9ca3af',
            minWidth: 40,
            textAlign: 'right',
          }}
        >
          {timestamp}
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// Message Component: Incoming Messages (light gray, left-aligned)
// ─────────────────────────────────────────────────────────────────
function IncomingMessage({ text }: { text: string }) {
  return (
    <div
      style={{
        display: 'flex',
        justifyContent: 'flex-start',
        marginBottom: 8,
      }}
    >
      <div
        style={{
          maxWidth: '75%',
          backgroundColor: '#e5e7eb',
          color: '#111827',
          padding: '10px 14px',
          borderRadius: 18,
          fontSize: 15,
          lineHeight: 1.4,
          wordBreak: 'break-word',
          overflowWrap: 'anywhere',
        }}
      >
        {text}
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// Dispatch Center Header (sticky at top)
// ─────────────────────────────────────────────────────────────────
function DispatchCenterHeader() {
  const handleCall = () => {
    // Placeholder: trigger call action
    //
    // In production, this would trigger a phone/calling feature
  };

  return (
    <div
      style={{
        position: 'sticky',
        top: 0,
        zIndex: 10,
        backgroundColor: '#ffffff',
        borderBottom: '1px solid #e5e7eb',
        padding: '14px 16px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}
    >
      <div
        style={{
          fontSize: 18,
          fontWeight: 600,
          color: '#111827',
        }}
      >
        Dispatch Center
      </div>
      <button
        type="button"
        onClick={handleCall}
        aria-label="Call Dispatch Center"
        style={{
          width: 36,
          height: 36,
          borderRadius: '50%',
          border: 'none',
          padding: 0,
          backgroundColor: '#f3f4f6',
          color: '#111827',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          cursor: 'pointer',
          flex: '0 0 auto',
          transition: 'background-color 0.2s',
        }}
        onMouseEnter={(e) => {
          (e.currentTarget as HTMLButtonElement).style.backgroundColor = '#e5e7eb';
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLButtonElement).style.backgroundColor = '#f3f4f6';
        }}
      >
        {/* Phone Icon */}
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path
            d="M22 16.92v3a2 2 0 0 1-2.18 2 19.79 19.79 0 0 1-8.63-3.07 19.5 19.5 0 0 1-6-6 19.79 19.79 0 0 1-3.07-8.67A2 2 0 0 1 4.11 2h3a2 2 0 0 1 2 1.72 12.84 12.84 0 0 0 .7 2.81 2 2 0 0 1-.45 2.11L8.09 9.91a16 16 0 0 0 6 6l1.27-1.27a2 2 0 0 1 2.11-.45 12.84 12.84 0 0 0 2.81.7A2 2 0 0 1 22 16.92z"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </button>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// Persistent Info Banner (sticky below header)
// ─────────────────────────────────────────────────────────────────
function InfoBanner() {
  return (
    <div
      style={{
        position: 'sticky',
        top: 62, // Height of dispatch header
        zIndex: 9,
        backgroundColor: '#f3f4f6',
        padding: '12px 16px',
        borderRadius: 8,
        margin: '8px 16px 0 16px',
        display: 'flex',
        flexDirection: 'column',
        gap: 4,
      }}
    >
      <div
        style={{
          fontSize: 13,
          color: '#374151',
          lineHeight: 1.4,
        }}
      >
        Messages and calls are for emergency use only (e.g., vehicle, road, or driver issues)
      </div>
      <button
        type="button"
        style={{
          alignSelf: 'flex-start',
          padding: 0,
          border: 'none',
          backgroundColor: 'transparent',
          fontSize: 13,
          color: '#2563eb',
          cursor: 'pointer',
          textDecoration: 'underline',
          fontWeight: 500,
        }}
        onClick={() => {
          // Placeholder: open help/learn more dialog
          //
        }}
      >
        Learn more
      </button>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// Format timestamp from Date (HH:MM)
// ─────────────────────────────────────────────────────────────────
function formatTime(date: Date): string {
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  return `${hours}:${minutes}`;
}

export default function Messages() {
  const { autoSendMessage, clearAutoSendMessage, pendingAutoReply } = useFleet();
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([]);
  const autoSendTriggeredRef = useRef(false);
  const processedReplyIds = useRef<Set<string>>(new Set());

  const [draft, setDraft] = useState('');
  const listRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  // Monitor auto-send alert messages
  useEffect(() => {
    if (autoSendMessage && !autoSendTriggeredRef.current) {
      autoSendTriggeredRef.current = true;
      clearAutoSendMessage();

      // Add auto-send high-priority alert message
      // Mark with source='auto-send' and severity='high' for outgoing-style rendering
      const now = new Date();
      setChatMessages((prev) => [
        ...prev,
        {
          id: `auto_system_${Date.now()}`,
          sender: 'me', // Treat as "our" message (outgoing)
          text: '[AUTO-SEND] High-priority issue detected. Immediate action required.',
          source: 'auto-send',
          severity: 'high',
          timestamp: formatTime(now), // Include timestamp like manual outgoing messages
        },
      ]);
    }
    // Reset flag for next trigger
    if (!autoSendMessage) {
      autoSendTriggeredRef.current = false;
    }
  }, [autoSendMessage, clearAutoSendMessage]);

  // Monitor provider auto-replies
  useEffect(() => {
    if (pendingAutoReply && !processedReplyIds.current.has(pendingAutoReply.id)) {
      processedReplyIds.current.add(pendingAutoReply.id);
      setChatMessages((prev) => [
        ...prev,
        {
          id: pendingAutoReply.id,
          sender: 'other',
          text: pendingAutoReply.text,
        },
      ]);
    }
  }, [pendingAutoReply]);

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    requestAnimationFrame(() => {
      if (!listRef.current) return;
      listRef.current.scrollTop = listRef.current.scrollHeight;
    });
  }, [chatMessages]);

  const send = () => {
    const text = draft.trim();
    if (!text) return;

    const now = new Date();
    setChatMessages((prev) => [
      ...prev,
      {
        id: `m_${Date.now()}`,
        sender: 'me',
        text,
        timestamp: formatTime(now),
      },
    ]);
    setDraft('');
    requestAnimationFrame(() => inputRef.current?.focus());

    // Auto-reply after delay
    setTimeout(() => {
      setChatMessages((prev) => [
        ...prev,
        {
          id: `reply_${Date.now()}`,
          sender: 'other',
          text: 'Copy that. We will update you shortly.',
        },
      ]);
    }, 1200);
  };

  return (
    <div
      style={{
        height: '100%',
        display: 'flex',
        flexDirection: 'column',
        backgroundColor: '#ffffff',
        overflowX: 'hidden',
      }}
    >
      {/* Header: Dispatch Center (sticky) */}
      <DispatchCenterHeader />

      {/* Info Banner: Emergency Use Notice (sticky below header) */}
      <InfoBanner />

      {/* Messages List: Scrollable content area */}
      <div
        ref={listRef}
        style={{
          flex: 1,
          overflowY: 'auto',
          overflowX: 'hidden',
          padding: '12px 16px',
          display: 'flex',
          flexDirection: 'column',
          gap: 0,
        }}
      >
        {chatMessages.map((m) => {
          const variant = getMessageVariant(m);
          if (import.meta.env.DEV) {
            // Debug: log message structure and variant
            console.log({
              id: m.id,
              sender: m.sender,
              source: m.source,
              severity: m.severity,
              variant,
              text: m.text.substring(0, 50) + '...',
            });
          }
          if (variant === 'outgoing') {
            return <OutgoingMessage key={m.id} text={m.text} timestamp={m.timestamp} />;
          }
          if (variant === 'incoming') {
            return <IncomingMessage key={m.id} text={m.text} />;
          }
          return <SystemMessage key={m.id} text={m.text} />;
        })}
      </div>

{/* Input Area: Fixed at bottom */}
      <div
        style={{
          borderTop: '1px solid #e5e7eb',
          padding: '12px 16px',
          backgroundColor: '#ffffff',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          flexShrink: 0,
        }}
      >
        {/* Plus/Add Button */}
        <button
          type="button"
          aria-label="Add"
          style={{
            width: 44,
            height: 44,
            borderRadius: '50%',
            border: 'none',
            padding: 0,
            backgroundColor: '#e5e7eb',
            color: '#111827',
            fontSize: 22,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            cursor: 'pointer',
            flex: '0 0 auto',
            transition: 'background-color 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.backgroundColor = '#d1d5db';
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.backgroundColor = '#e5e7eb';
          }}
        >
          <svg width="20" height="20" viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M10 2V18M2 10H18" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
          </svg>
        </button>

        {/* Message Input Field */}
        <input
          ref={inputRef}
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') send();
          }}
          placeholder="Type a message"
          style={{
            flex: 1,
            minWidth: 0,
            height: 40,
            padding: '8px 14px',
            borderRadius: 10,
            border: '1px solid #e5e7eb',
            backgroundColor: '#ffffff',
            fontSize: 16,
            color: '#111827',
            outline: 'none',
            transition: 'border-color 0.2s',
          }}
          onFocus={(e) => {
            (e.target as HTMLInputElement).style.borderColor = '#d1d5db';
          }}
          onBlur={(e) => {
            (e.target as HTMLInputElement).style.borderColor = '#e5e7eb';
          }}
        />

        {/* Send Button (green) */}
        <button
          type="button"
          onClick={send}
          aria-label="Send"
          style={{
            width: 44,
            height: 44,
            borderRadius: '50%',
            border: 'none',
            padding: 0,
            backgroundColor: '#22c55e',
            color: '#ffffff',
            fontSize: 22,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            cursor: 'pointer',
            flex: '0 0 auto',
            transition: 'background-color 0.2s',
          }}
          onMouseEnter={(e) => {
            (e.currentTarget as HTMLButtonElement).style.backgroundColor = '#16a34a';
          }}
          onMouseLeave={(e) => {
            (e.currentTarget as HTMLButtonElement).style.backgroundColor = '#22c55e';
          }}
        >
          <svg width="20" height="20" viewBox="0 0 20 20" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M3 10L17 2L9 18L7 11L3 10Z" fill="currentColor"/>
          </svg>
        </button>
      </div>
    </div>
  );
}
