import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { getRecommendation } from '../api/query';

export default function ResultPage() {
  const location = useLocation();
  const question = location.state?.question;
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
      <h2>추천 결과</h2>
      <p><strong>질문:</strong> {question}</p>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: '20px' }}>
        {results.map((item, i) => (
          <div key={i} style={{ border: '1px solid #ccc', padding: '1rem', width: '300px' }}>
            <img src={item.image} alt={item.name} style={{ width: '100%' }} />
            <h3>{item.name}</h3>
            <p><strong>가격:</strong> {item.price.toLocaleString()}원</p>
            <p>{item.reason}</p>
            <a href={item.url} target="_blank" rel="noopener noreferrer">
              시세 보기 →
            </a>
          </div>
        ))}
      </div>
    </div>
  );
}
