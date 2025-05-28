import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { login } from '../api/auth';
import GNB from '../components/Navigation';

export default function LoginForm() {
  const navigate = useNavigate();

  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [message, setMessage] = useState('');

  // 자동 로그인 상태 유지
  useEffect(() => {
    const savedUsername = localStorage.getItem('savedUsername');
    if (savedUsername) setUsername(savedUsername);
  }, []);

  const handleLogin = async () => {
    try {
      const res = await login(username, password);
      localStorage.setItem('savedUsername', username);
      alert('로그인 성공!');
      setMessage('로그인 성공!');
    } catch (err) {
      alert('로그인 실패!');
      setMessage('헉!!! 로그인 실패!');
    }
  };

  return (
    <div>
      <GNB />
      <div className='loginform'>
        {/* 아이디 */}
        <div className='idInput'>
          <input
            type="text"
            value={username}
            placeholder="아이디"
            onChange={(e) => setUsername(e.target.value)}
            style={{ width: '100%', padding: '1rem', borderRadius: '10px', border: '1px solid #999' }}
          />
        </div>

        {/* 비밀번호 */}
        <div style={{ position: 'relative', marginBottom: '1rem' }}>
          <input
            type={showPassword ? 'text' : 'password'}
            value={password}
            placeholder="비밀번호"
            onChange={(e) => setPassword(e.target.value)}
            style={{ width: '100%', padding: '1rem', borderRadius: '10px', border: '1px solid #999' }}
          />
          <span
            style={{ position: 'absolute', right: '10px', top: '50%', transform: 'translateY(-50%)', cursor: 'pointer' }}
            onClick={() => setShowPassword(!showPassword)}
          >{showPassword ? '🙈' : '👁️'}</span>
        </div>

        {/* 로그인 상태 유지 */}
        <div style={{ display: 'flex', alignItems: 'center', marginBottom: '1rem' }}>
          <input type="checkbox" checked readOnly style={{ marginRight: '0.5rem' }} />
          <label>로그인 상태 유지</label>
        </div>

        {/* 로그인 버튼 */}
        <button className='btn round Primary size-l ui-button ui-corner-all ui-widget'
          onClick={handleLogin}
          style={{ width: '100%', padding: '1rem', background: '#ff5a2c', color: '#fff', borderRadius: '12px', border: 'none', fontWeight: 'bold' }}
        >
          로그인
        </button>

        {/* 로그인 결과 메시지 */}
        <p style={{ textAlign: 'center', marginTop: '1rem' }}>{message}</p>

        {/* 하단 링크 */}
        <div style={{ marginTop: '1rem', textAlign: 'center', fontSize: '0.9rem', color: '#ccc' }}>
          <span style={{ cursor: 'pointer', marginRight: '0.5rem' }} onClick={() => navigate('/find-password')}>비밀번호 찾기</span>
          |
          <span style={{ cursor: 'pointer', margin: '0 0.5rem' }} onClick={() => navigate('/find-id')}>아이디 찾기</span>
          |
          <span style={{ cursor: 'pointer', marginLeft: '0.5rem' }} onClick={() => navigate('/signup')}>회원가입</span>
        </div>
      </div>
      
    </div>
  );
}
