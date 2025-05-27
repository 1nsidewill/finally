import React, { useRef, useEffect } from 'react'; 
import './Box.css';

export default function BottomSheet({ isOpen, onClose, children }) {
  const sheetRef = useRef();

  useEffect(() => {
    const handleClickOutside = (e) => {
      if (sheetRef.current && !sheetRef.current.contains(e.target)) {
        onClose();
      }
    };
    if (isOpen) document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [isOpen, onClose]);

  return (
    <div className={`bottom-overlay ${isOpen ? 'show' : ''}`}>
      <div className="bottom-sheet" ref={sheetRef}>
        <div className="drag-handle" />
        {children}
      </div>
    </div>
  );
}