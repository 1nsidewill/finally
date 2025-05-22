// src/components/GNB.jsx
import { Link } from 'react-router-dom';

export default function GNB() {
  return (
    <nav style={{ padding: '10px', borderBottom: '1px solid #ccc' }}>
      <span style={{ marginRight: '20px' }}>🌐 GNB</span>
      <Link to="/login">로그인</Link> {/* 로그인 버튼 */}
    </nav>
  );
}
