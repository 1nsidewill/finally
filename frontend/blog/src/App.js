import logo from './logo.svg';
import './App.css';

function App() {

  let posts = '야마하';
  function 함수(){
    return 100
  }
  let style = { color : 'red', fontSize : '30px' };

  return (
    <div className="App">
      <div className='gnb'>
        <div>Finally</div>
      </div>
      <h4>finally you've got it</h4>
      <h4> { posts } </h4>
      <h4> { 함수() } </h4>
      <div className={ posts }>test</div>
      <div style={ { color : 'blue', fontSize : '30px' } }>인라인에 스타일 넣고싶을 때</div>
      <div style={ style }>변수를 넣어도 가능</div>
      <img src={logo}/>
    </div>
  );
}


export default App;
