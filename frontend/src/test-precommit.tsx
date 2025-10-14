// Test file for pre-commit frontend formatting
import React from 'react';
import { useState,useEffect } from 'react';

const TestComponent = ( ) => {
  const [count,setCount]=useState(0);
  const [data,setData]=useState(null);

  useEffect(()=>{
    // This should trigger formatting
    // Component mounted
  },[]);

  const handleClick=()=>{
    setCount(count+1);
  };

  return (
    <div className="test-component">
      <h1>Test Component</h1>
      <p>Count: {count}</p>
      <button onClick={handleClick}>Increment</button>
    </div>
  );
};

export default TestComponent;
