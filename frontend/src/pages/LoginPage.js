// src/pages/LoginPage.js
import { useState } from 'react';

export default function LoginPage() {
  const [id, setId] = useState('');
  const [pw, setPw] = useState('');
  const [message, setMessage] = useState('');

  const handleLogin = async () => {
    try {
      const res = await fetch('http://34.64.229.242/agent-service/auth/token', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: id, password: pw })
      });

      if (!res.ok) {
        throw new Error('로그인 실패');
      }

      const data = await res.json();
      localStorage.setItem('token', data.access_token);
      setMessage('✅ 로그인 성공!');
    } catch (err) {
      setMessage('❌ 로그인 실패: ' + err.message);
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
