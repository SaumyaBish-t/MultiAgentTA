import { useState, useEffect, useRef } from 'react';

export function useSSE<T>(url: string, initialState: T | null = null) {
  const [data, setData] = useState<T | null>(initialState);
  const [error, setError] = useState<Event | null>(null);
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    // Prevent multiple connections
    if (eventSourceRef.current) {
      return;
    }

    try {
      const source = new EventSource(url);
      eventSourceRef.current = source;

      source.onopen = () => {
        setIsConnected(true);
        setError(null);
      };

      source.onmessage = (event) => {
        try {
          const parsedData = JSON.parse(event.data);
          // If the server sends { type: 'ping' }, ignore it
          if (parsedData.type !== 'ping') {
            setData(parsedData);
          }
        } catch (e) {
          // If not JSON, you can still return raw string or handle appropriately
          console.error("Error parsing SSE data", e);
        }
      };

      source.onerror = (err) => {
        setIsConnected(false);
        setError(err);
        source.close();
        eventSourceRef.current = null;

        // Reconnect after 5 seconds
        setTimeout(() => {
          setIsConnected(false); // trigger re-render
        }, 5000);
      };

    } catch (err) {
      console.error("Error connecting to SSE", err);
    }

    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close();
        eventSourceRef.current = null;
        setIsConnected(false);
      }
    };
  }, [url]);

  return { data, isConnected, error };
}
