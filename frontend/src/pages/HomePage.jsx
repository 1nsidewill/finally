import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import GNB from '../components/Navigation';
import BG from '../components/BG';
import SearchInput, { Input as ChatInput } from '../components/Form-search';
import BottomSheet from '../components/Box';
import { getRecommendation } from '../api/auth';


export default function HomePage() {
  const [inputValue, setInputValue] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef(null);
  const navigate = useNavigate();

  const handleSubmit = async () => {
  if (!inputValue.trim()) return;
  try {
    const result = await getRecommendation(inputValue);
    navigate('/result', {
      state: {
        question: inputValue,
        result, 
      },
    });
  } catch (error) {
    alert(error.message); 
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
      <SearchInput
        inputValue={inputValue}
        setInputValue={setInputValue}
        handleSubmit={handleSubmit}
      />
      <BottomSheet />

    </div>
  );
}
