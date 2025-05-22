export async function getRecommendation(question) {
  const token = localStorage.getItem('token');
  if (!token) {
    throw new Error('❌ 토큰이 없습니다. 먼저 로그인 해주세요.');
  }

  const response = await fetch('http://34.47.76.15/agent-service/api/query', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ question }), // 질문 내용 포함
  });

  if (!response.ok) {
    let errorData;
    try {
      errorData = await response.json();
    } catch {
      errorData = { detail: '응답 파싱 실패' };
    }
    console.error('요청 실패:', errorData);
    throw new Error(errorData?.detail || '알 수 없는 오류');
  }

  const data = await response.json();
  return data.result;
}
