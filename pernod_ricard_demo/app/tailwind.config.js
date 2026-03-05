/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./client/index.html",
    "./client/src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        navy: {
          50: '#f0f2f7',
          100: '#d9dde8',
          200: '#b3bbcf',
          300: '#8c99b5',
          400: '#66779c',
          500: '#405582',
          600: '#334468',
          700: '#26334e',
          800: '#1B2A4A',
          900: '#111b31',
        },
        gold: {
          50: '#fbf8ef',
          100: '#f5efd5',
          200: '#ebdfa9',
          300: '#dccf7e',
          400: '#C5A55A',
          500: '#b08f3e',
          600: '#8d7232',
          700: '#6a5625',
          800: '#473919',
          900: '#241d0c',
        },
        warmred: {
          400: '#d04848',
          500: '#B83232',
          600: '#962929',
        },
      },
    },
  },
  plugins: [],
};
