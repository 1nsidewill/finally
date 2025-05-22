import axios from 'axios';

export async function getRecommendation(question) {
  const token = localStorage.getItem('token'); // 저장된 토큰 불러오기

  const response = await axios.post(
    'http://34.64.229.242/agent-service/api/query',
    { question },
    {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`, // 헤더에 토큰 포함
      },
    }
  );
  return response.data.result;
}
