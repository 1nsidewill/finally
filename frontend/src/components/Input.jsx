// src/components/Input.jsx
import '../style/App.css';

export default function Input({ inputValue, setInputValue, handleSubmit }) {
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
