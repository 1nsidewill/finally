import { useEffect, useState } from 'react';
import { useLocation } from 'react-router-dom';
import { getRecommendation } from '../api/auth';
import GNB from '../components/Navigation';
import '../components/Content.css';

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
    <div>
      <GNB />
      <p className='item-header'>
        당신이 찾던 <strong className='txtPrimary'>{ question}</strong>와 관련된 오토바이를 추천드려요.
      </p>

      {error && 
        <div className="custom-alert">{error}</div>
      }

      <div className='item-wrap'>
        {Array.isArray(results) ? results.map((item, i) => (
          <div className='item' key={i} >

            <div className='flag-rank'>
              TOP {item.rank}
            </div>
            
            <div className="img-gradient" />
            <img className='item-img'
              src={item.img_url}
            />

            <div className='item-txt-container'>
              <h3 className='item-tit'>{item.title}</h3>
              <p className='item-summary' >
                {item.match_summary.split('\n').map((line, idx) => (
                  <span key={idx}>{line}<br /></span>
                ))}
              </p>
              <div className='item-bottom'>
                <p className='item-price'> {item.price.toLocaleString()}원</p>
                <a className='txtbtn txtPrimary underline' href={item.url} target="_blank" rel="noopener noreferrer">
                  구매하러 가기
                </a>
              </div>
            </div>


          </div>
        )) : (
          <p>추천 결과가 없습니다.</p>
        )}
      </div>
    </div>
  );
}
