/**
 * Message store (global state)
 * Stores auto-generated messages for HIGH severity incidents
 */

export interface Message {
  id: string;
  thread_id: string;
  title: string;
  content: string;
  timestamp: string; // ISO
  is_draft: boolean;
  is_sent: boolean;
}

class MessageStore {
  private static messages: Message[] = [];
  private static listeners: Set<(messages: Message[]) => void> = new Set();

  /**
   * Add a new message
   */
  static addMessage(message: Message) {
    this.messages.push(message);
    this.notifyListeners();
  }

  /**
   * Get all messages
   */
  static getMessages(): Message[] {
    return [...this.messages];
  }

  /**
   * Get messages by thread_id
   */
  static getMessageThread(thread_id: string): Message[] {
    return this.messages.filter((m) => m.thread_id === thread_id);
  }

  /**
   * Subscribe to message updates
   */
  static subscribe(listener: (messages: Message[]) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Notify all listeners
   */
  private static notifyListeners() {
    for (const listener of this.listeners) {
      listener([...this.messages]);
    }
  }

  /**
   * Clear all messages (for testing)
   */
  static clear() {
    this.messages = [];
    this.notifyListeners();
  }
}

export default MessageStore;
