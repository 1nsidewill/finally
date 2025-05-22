export async function login(username, password) {
  const response = await fetch('http://34.47.76.15/agent-service/auth/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
  });

  if (!response.ok) throw new Error('로그인 실패');

  const data = await response.json();
  localStorage.setItem('token', data.access_token); // 토큰 저장
  return data;
}
