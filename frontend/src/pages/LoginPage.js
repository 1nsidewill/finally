// src/pages/LoginPage.js
import { useState } from 'react';
import { login } from '../api/auth';

export default function LoginPage() {
  const [id, setId] = useState('');
  const [pw, setPw] = useState('');
  const [message, setMessage] = useState('');

  const handleLogin = async () => {
    try {
      const res = await login(id, pw);
      alert('로그인 성공!');
      setMessage('로그인 성공!');
    } catch (err) {
      alert('로그인 실패!');
      setMessage('헉!!!로그인 실패!');
    }
  };

  return (
    <div style={{ padding: '2rem' }}>
      <h2>로그인 테스트</h2>
      <input
        placeholder="아이디"
        value={id}
        onChange={(e) => setId(e.target.value)}
        style={{ display: 'block', marginBottom: '10px' }}
      />
      <input
        placeholder="비밀번호"
        type="password"
        value={pw}
        onChange={(e) => setPw(e.target.value)}
        style={{ display: 'block', marginBottom: '10px' }}
      />
      <button onClick={handleLogin}>로그인</button>
      <p>{message}</p>
    </div>
  );
}
