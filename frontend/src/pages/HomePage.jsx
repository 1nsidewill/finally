import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import GNB from '../components/Navigation';
import BG from '../components/BG';
import SearchInput, { Input as ChatInput } from '../components/Form';
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
        result, // ✅ API 응답 결과 같이 넘기기
      },
    });
  } catch (error) {
    alert(error.message); // ❗ 또는 setError 상태로 UI에 표시
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
