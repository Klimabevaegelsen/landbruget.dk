import { dirname } from 'path';
import { fileURLToPath } from 'url';
import { FlatCompat } from '@eslint/eslintrc';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const compat = new FlatCompat({
  baseDirectory: __dirname,
});

const eslintConfig = [
  ...compat.extends('next/core-web-vitals', 'next/typescript'),
  {
    rules: {
      // Remove trailing whitespace
      'no-trailing-spaces': 'error',
      // Remove multiple empty lines
      'no-multiple-empty-lines': ['error', { max: 1, maxEOF: 0, maxBOF: 0 }],
    },
  },
];

export default eslintConfig;
