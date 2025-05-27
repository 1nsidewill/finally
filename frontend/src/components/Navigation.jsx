import React, { useState, useRef, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import './Navigation.css';

export default function GNB() {
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);
  const navigate = useNavigate();

  useEffect(() => {
    function handleClickOutside(event) {
      if (menuOpen && menuRef.current && !menuRef.current.contains(event.target)) {
        setMenuOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [menuOpen]);

  return (
    <header className="gnb">
      <img src="/logo-bl.svg" alt="logo" className="logo" />
      <button onClick={() => setMenuOpen(!menuOpen)} className="menu-button">
        <span className="material-symbols-outlined">
          {menuOpen ? 'close' : 'menu'}
        </span>
      </button>

      {menuOpen && (
        <div className="mobile-menu-wrapper">
          <div className="mobile-menu" ref={menuRef}>
            <button className="close-button" onClick={() => setMenuOpen(false)}>
              <span className="material-symbols-outlined">close</span>
            </button>
            <button className="menu-item" onClick={() => navigate('/login')}>로그인</button>
            <button className="menu-item">Menu 1</button>
            <button className="menu-item">Menu 2</button>
            <button className="menu-item">Menu 3</button>
          </div>
        </div>
      )}
    </header>
  );
}
