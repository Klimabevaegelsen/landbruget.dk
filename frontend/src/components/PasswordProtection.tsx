'use client'

import { useEffect, useState } from 'react'

export default function PasswordProtection() {
  const [isVisible, setIsVisible] = useState(true)
  const [password, setPassword] = useState('')

  useEffect(() => {
    // Check if already authenticated
    if (typeof window !== 'undefined' && localStorage.getItem('authenticated') === 'true') {
      setIsVisible(false)
    }
  }, [])

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault()
    const correctPassword = process.env.NEXT_PUBLIC_SITE_PASSWORD

    if (correctPassword && password.includes(correctPassword)) {
      localStorage.setItem('authenticated', 'true')
      setIsVisible(false)
    } else {
      alert('Forkert adgangskode')
      setPassword('')
    }
  }

  if (!isVisible) {
    return null
  }

  return (
    <div style={{
      position: 'fixed',
      top: 0,
      left: 0,
      width: '100%',
      height: '100%',
      backgroundColor: 'white',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 9999,
      flexDirection: 'column',
      gap: '20px'
    }}>
      <h2 style={{ fontSize: '24px', fontWeight: 'bold', color: '#333' }}>
        Landbruget.dk
      </h2>
      <p style={{ color: '#666', marginBottom: '10px' }}>
        Indtast adgangskode:
      </p>
      <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '20px', alignItems: 'center' }}>
        <input
          type="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          placeholder="Adgangskode"
          style={{
            padding: '12px',
            border: '1px solid #ddd',
            borderRadius: '6px',
            fontSize: '16px',
            width: '250px'
          }}
          autoFocus
        />
        <button
          type="submit"
          style={{
            padding: '12px 24px',
            backgroundColor: '#3b82f6',
            color: 'white',
            border: 'none',
            borderRadius: '6px',
            fontSize: '16px',
            cursor: 'pointer'
          }}
        >
          Log ind
        </button>
      </form>
    </div>
  )
}
