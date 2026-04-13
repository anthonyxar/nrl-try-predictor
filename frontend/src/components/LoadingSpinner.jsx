import React from 'react'

export default function LoadingSpinner({ text = 'Loading...' }) {
  return (
    <div className="loading-spinner">
      <div className="rugby-ball-wrapper">
        <svg className="rugby-ball" viewBox="0 0 80 50" xmlns="http://www.w3.org/2000/svg">
          <ellipse cx="40" cy="25" rx="36" ry="20" fill="#8B4513" stroke="#5C2D0A" strokeWidth="2" />
          <ellipse cx="40" cy="25" rx="36" ry="20" fill="url(#ballShading)" />
          <line x1="40" y1="5" x2="40" y2="45" stroke="#F5E6C8" strokeWidth="1.5" />
          <path d="M 26 8 Q 33 25 26 42" fill="none" stroke="#F5E6C8" strokeWidth="1" />
          <path d="M 54 8 Q 47 25 54 42" fill="none" stroke="#F5E6C8" strokeWidth="1" />
          <line x1="36" y1="6" x2="44" y2="6" stroke="#F5E6C8" strokeWidth="1" />
          <line x1="36" y1="44" x2="44" y2="44" stroke="#F5E6C8" strokeWidth="1" />
          <defs>
            <radialGradient id="ballShading" cx="35%" cy="35%">
              <stop offset="0%" stopColor="rgba(255,255,255,0.15)" />
              <stop offset="100%" stopColor="rgba(0,0,0,0.2)" />
            </radialGradient>
          </defs>
        </svg>
      </div>
      <p className="loading-text">{text}</p>
    </div>
  )
}
