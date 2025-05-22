// src/pages/ResultPage.js
import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { getRecommendation } from '../api/query';

export default function ResultPage() {
  const location = useLocation();
  const question = location.state?.question || '';
  const [result, setResult] = useState('');
  const [error, setError] = useState('');

  useEffect(() => {
    if (question) {
      getRecommendation(question)
        .then(setResult)
        .catch(err => setError(err.message));
    }
  }, [question]);

  return (
    <div style={{ padding: '2rem' }}>
      <h2>추천 결과</h2>
      <p><strong>질문:</strong> {question}</p>
      {result && <pre>{result}</pre>}
      {error && <p style={{ color: 'red' }}>❌ {error}</p>}
    </div>
  );
}
