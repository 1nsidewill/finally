import React, { useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import './Form.css';
import './Button.css';

// ✅ Named export
export function Input({ inputValue, setInputValue, handleSubmit }) {
  return (
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
  );
}

export default function SearchInput() {
  const [focused, setFocused] = useState(false);
  const [value, setValue] = useState('');
  const inputRef = useRef();
  const navigate = useNavigate();

  const handleSearch = () => {
    if (value.trim()) {
      navigate('/result', { state: { question: value } });
    }
  };
return (
  <div className="bottom-sheet">
    <input
      ref={inputRef}
      type="text"
      className="search-input"
      value={value}
      onChange={(e) => setValue(e.target.value)}
      placeholder="test 당신이 원하는 매물을 찾을 때까지"
    />

    <div className="homebtn">
      <div className="bike-btn">
        <span className="material-symbols-outlined">moped</span>
      </div>
      <div className="search-btn" onClick={handleSearch}>
        <span className="material-symbols-outlined">search</span>
      </div>
      <div className="preference-btn">
        <span className="material-symbols-outlined">person_check</span>
      </div>
    </div>
  </div>
);

}
