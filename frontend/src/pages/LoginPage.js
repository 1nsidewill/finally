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
  const [isError, setIsError] = useState(false);

  // 자동 로그인 상태 유지
  const [isAutoLogin, setIsAutoLogin] = useState(false);

  // 자동 로그인 상태 유지
  useEffect(() => {
    const savedUsername = localStorage.getItem('savedUsername');
    const savedPassword = localStorage.getItem('savedPassword');
    const autoLogin = localStorage.getItem('savedAutoLogin') === 'true';

    if (savedUsername && savedPassword && autoLogin) {
      setUsername(savedUsername);
      setPassword(savedPassword);
      setIsAutoLogin(true);
    }
  }, []);

  const handleLogin = async () => {
    try {
      const res = await login(username, password);

      if (isAutoLogin) {
        localStorage.setItem('savedUsername', username);
        localStorage.setItem('savedPassword', password);
        localStorage.setItem('savedAutoLogin', 'true');
      } else {
        localStorage.removeItem('savedUsername');
        localStorage.removeItem('savedPassword');
        localStorage.removeItem('savedAutoLogin');
      }

      setIsError(false);
      navigate('/');
    } catch (err) {
      setMessage(err.message || '로그인 실패');
      setIsError(true);
    }
  };


  return (
    <div>
      <GNB />
      <div className='loginform'>
        {/* 아이디 */}
        <div className='input-container'>
          <input className='input'
            type="text"
            value={username}
            placeholder="아이디"
            onChange={(e) => setUsername(e.target.value)}
          />
        </div>

        {/* 비밀번호 */}
        <div className='input-container'>
          <input className='input'
            type={showPassword ? 'text' : 'password'}
            value={password}
            placeholder="비밀번호"
            onChange={(e) => setPassword(e.target.value)}
          />
          <button
            type="button"
            className="password-toggle"
            onClick={() => setShowPassword(!showPassword)}
          >
            <img
              className="eye-icon"
              src={showPassword ? '/ico-eye-off.svg' : '/ico-eye.svg'}
              alt={showPassword ? 'Hide password' : 'Show password'}
            />
          </button>
        </div>

        {/* 로그인 상태 유지 */}
        <div className='login-checkbox'>
          <label class="custom-checkbox">
            <input type="checkbox" 
              checked={isAutoLogin}
              onChange={(e) => setIsAutoLogin(e.target.checked)}/>
            <span class="checkmark"></span>
            로그인 상태 유지
          </label>
        </div>


        {/* 로그인 결과 메시지 */}
        {message && (
          <p className={`validation ${isError ? 'txtwarning' : ''}`}>{message}</p>
        )}

        {/* 로그인 버튼 */}
        <button className='btn Primary'
          onClick={handleLogin}
        >
          로그인
        </button>

        {/* 하단 링크 */}
        <div className='txtbtn-wrap'>
          <span className='txtbtn txtgray' onClick={() => navigate('/find-password')}>비밀번호 찾기</span>
          |
          <span className='txtbtn txtgray' onClick={() => navigate('/find-id')}>아이디 찾기</span>
          |
          <span className='txtbtn txtgray' onClick={() => navigate('/signup')}>회원가입</span>
        </div>
      </div>
      
    </div>
  );
}
