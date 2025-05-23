import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { getRecommendation } from '../api/query';

export default function ResultPage() {
  const location = useLocation();
  const question = location.state?.question || '';
  const [results, setResults] = useState([]);
  const [error, setError] = useState('');

  useEffect(() => {
    if (question) {
      getRecommendation(question)
        .then(setResults)
        .catch(err => setError(err.message));
    }
  }, [question]);

  return (
    <div style={{ padding: '2rem' }}>
      <h2>🔍 추천 결과</h2>
      <p><strong>질문:</strong> {question}</p>

      {error && <p style={{ color: 'red' }}>{error}</p>}

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '20px' }}>
        {Array.isArray(results) ? results.map((item, i) => (
          <div key={i} style={{
            border: '1px solid #ccc',
            borderRadius: '8px',
            padding: '1rem',
            width: '300px',
            backgroundColor: '#fff',
            boxShadow: '0 2px 8px rgba(0,0,0,0.1)'
          }}>
            {/* 🔽 랭크 표시 */}
            <div style={{ fontWeight: 'bold', fontSize: '1rem', marginBottom: '0.5rem', color: '#007bff' }}>
              🏆 {item.rank}위
            </div>
            
            <img
              src={item.img_url}
              alt={item.title}
              style={{ width: '100%', height: '180px', objectFit: 'cover', borderRadius: '4px' }}
            />
            <h3>{item.title}</h3>
            <p><strong>가격:</strong> {item.price.toLocaleString()}원</p>
            <p style={{ fontSize: '0.9rem', color: '#555' }}>
              <strong>요약:</strong><br />
              {item.match_summary.split('\n').map((line, idx) => (
                <span key={idx}>{line}<br /></span>
              ))}
            </p>
            <a href={item.url} target="_blank" rel="noopener noreferrer">
              상세 페이지 →
            </a>
          </div>
        )) : (
          <p>추천 결과가 없습니다.</p>
        )}
      </div>
    </div>
  );
}
