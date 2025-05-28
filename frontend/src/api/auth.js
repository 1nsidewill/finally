const BASE_URL = process.env.REACT_APP_API_BASE_URL;

export async function login(username, password) {
  const response = await fetch(`${BASE_URL}/auth/token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });

  if (!response.ok) throw new Error('로그인 실패');

  const data = await response.json();
  localStorage.setItem('token', data.access_token);
  return data;
}

export async function getRecommendation(question) {
  const token = localStorage.getItem('token');
  if (!token) {
    throw new Error('❌ 토큰이 없습니다. 먼저 로그인 해주세요.');
  }

  const response = await fetch(`${BASE_URL}/api/query`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ question }),
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
