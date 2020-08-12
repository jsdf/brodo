import React from 'react';
import debounce from 'debounce';
const {useState, useEffect, useRef, useMemo, useCallback} = React;

function useWindowSize(debounceInterval) {
  const [windowSize, setWindowSize] = useState({
    width: window.innerWidth,
    height: window.innerHeight,
  });

  useEffect(() => {
    const handleResizeDebounced = debounce(function handleResize() {
      if (
        windowSize.width !== window.innerWidth &&
        windowSize.height !== window.innerHeight
      ) {
        setWindowSize({width: window.innerWidth, height: window.innerHeight});
      }
    }, debounceInterval);
    window.addEventListener('resize', handleResizeDebounced);
    return () => window.removeEventListener('resize', handleResizeDebounced);
  }, []);

  return windowSize;
}

export default useWindowSize;
