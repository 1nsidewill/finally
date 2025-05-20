// .storybook/preview.js
import '../src/styles/global.css'; // 전역 스타일
import React from 'react';

export const parameters = {
  actions: { argTypesRegex: '^on[A-Z].*' },
  controls: {
    matchers: {
      color: /(background|color)$/i,
      date: /Date$/,
    },
  },
};

export const decorators = [
  (Story) => (
    <div style={{ padding: '2rem', background: '#111', color: '#fff' }}>
      <Story />
    </div>
  ),
];
