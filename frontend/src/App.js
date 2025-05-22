/* eslint-disabled */
import './App.css';
import { useState, useEffect, useRef } from 'react';
import { BrowserRouter as Router, Routes, Route, useNavigate } from 'react-router-dom';
import LoginPage from './pages/LoginPage';


function App() {
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
      {/* GNB */}
      <header className="gnb">
        <div className="logo">finally</div>
        <button onClick={() => setMenuOpen(!menuOpen)} className="menu-button">
          <span className="material-symbols-outlined">
            {menuOpen ? 'close' : 'menu'}
          </span>
        </button>
      </header>

      {/* 모바일 메뉴 */}
      {menuOpen && (
        <div className="mobile-menu-wrapper">
          <div className="mobile-menu" ref={menuRef}>
            <button className="close-button" onClick={() => setMenuOpen(false)}>
              <span className="material-symbols-outlined">close</span>
            </button>
            <button className="menu-item" onClick={() => navigate('/login')}>로그인</button> {/* 로그인 버튼 */}
            <button className="menu-item">Menu 1</button>
            <button className="menu-item">Menu 2</button>
            <button className="menu-item">Menu 3</button>
            
          </div>
        </div>
      )}

      {/* 본문 */}
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

export default App;
