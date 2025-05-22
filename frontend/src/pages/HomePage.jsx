// src/pages/HomePage.jsx
import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';

export default function HomePage() {
  const [inputValue, setInputValue] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);
  const navigate = useNavigate();

  const handleSubmit = () => {
    if (inputValue.trim()) {
      navigate('/result', { state: { question: inputValue } });
    }
  };

  useEffect(() => {
    function handleClickOutside(event) {
      if (menuOpen && menuRef.current && !menuRef.current.contains(event.target)) {
        if (window.innerWidth >= 768) {
          setMenuOpen(false);
        }
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [menuOpen]);

  return (
    <div className="App">
      <header className="gnb">
        <div className="logo">finally</div>
        <button onClick={() => setMenuOpen(!menuOpen)} className="menu-button">
          <span className="material-symbols-outlined">
            {menuOpen ? 'close' : 'menu'}
          </span>
        </button>
      </header>

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

      <h4 className="main-text">finally you've got it</h4>

      <div className="input-container">
        <input
          type="text"
          placeholder="Ask me anything..."
          className="input"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
        />
        <button
          className={`send-button ${inputValue.trim() ? 'active' : ''}`}
          disabled={!inputValue.trim()}
          onClick={handleSubmit}
        >
          <span className="material-symbols-outlined">arrow_upward</span>
        </button>
      </div>
    </div>
  );
}
