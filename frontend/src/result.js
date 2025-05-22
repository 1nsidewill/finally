import './result.css';

const bikes = [
  {
    title: '야마하 XMAX 300 ABS 2025년식',
    year: '25년식',
    location: '서울',
    price: '1,300만원',
    tag: '20% 이상 고가',
    monthly: '월 59만원',
    image: '/images/bike1.jpg',
  },
  {
    title: 'BMW C 400 GT 2020년식',
    year: '20년식 · 40,000km · 인천',
    price: '369만원',
    monthly: '월 16만원',
    tag: '13% 저렴',
    image: '/images/bike2.jpg',
  },
  // ...
];

export default function MainPage() {
  return (
    <div className="page-wrap">
      <header className="header">
        <div className="logo">Finally</div>
        <div className="menu-icon">☰</div>
      </header>

      <div className="main-content">
        {/* 왼쪽 필터 */}
        <aside className="sidebar">
          <h4>Keywords</h4>
          <div className="tags">
            <span>Spring ✕</span>
            <span>Smart ✕</span>
          </div>

          <h5>Label</h5>
          <div>
            <label><input type="checkbox" /> Label</label>
            <label><input type="checkbox" /> Label</label>
            <label><input type="checkbox" /> Label</label>
          </div>

          <h5>Label Range</h5>
          <input type="range" min="0" max="100" />
        </aside>

        {/* 오른쪽 상품 영역 */}
        <section className="bikes-section">
          <div className="search-sort">
            <input type="text" placeholder="Search" />
            <div className="sort-buttons">
              <button className="active">New</button>
              <button>Price ascending</button>
              <button>Price descending</button>
              <button>Rating</button>
            </div>
          </div>

          <div className="bike-grid">
            {bikes.map((bike, i) => (
              <div key={i} className="bike-card">
                <img src={bike.image} alt={bike.title} />
                <div className="bike-info">
                  <h4>{bike.title}</h4>
                  <p>{bike.year}</p>
                  <p>{bike.location}</p>
                  <div className="price">{bike.price} <span>{bike.monthly}</span></div>
                  <div className="tag">{bike.tag}</div>
                  <button className="detail-btn">시세 확인하기</button>
                </div>
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  );
}
