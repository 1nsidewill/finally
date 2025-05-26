import { useNavigate } from 'react-router-dom';
import '../style/App.css';

export default function BG() {
  return (
    <div className="bg">
      <img src="/logo-wh.svg" alt="logo" className="logo-wh" />
      <div className="floating-blobs">
        <div className="blob blue" style={{ top: '20%', left: '30%', animationDelay: '0s' }} />
        <div className="blob green" style={{ top: '40%', left: '70%', animationDelay: '5s' }} />
        <div className="blob gray" style={{ top: '60%', left: '20%', animationDelay: '10s' }} />
      </div>
    </div>
  );
}