import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import GNB from '../components/GNB';
import BG from '../components/BG';
import Input from '../components/Input';

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
      <BG />
      <GNB />
      <Input
        inputValue={inputValue}
        setInputValue={setInputValue}
        handleSubmit={handleSubmit}
      />
    </div>
  );
}
