import React from 'react'

export default function LoadingSpinner({ text = 'Loading...' }) {
  return (
    <div className="loading-spinner">
      <div className="rugby-ball-wrapper">
        <svg className="rugby-ball" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
          {/* Rotated group to tilt the ball at 45 degrees like the FA icon */}
          <g transform="rotate(-45, 50, 50)">
            {/* Ball outline */}
            <ellipse cx="50" cy="50" rx="42" ry="22" fill="none" stroke="#888" strokeWidth="2" />
            {/* Centre seam line */}
            <line x1="50" y1="28" x2="50" y2="72" stroke="#888" strokeWidth="1.5" />
            {/* Cross seam curves */}
            <path d="M 36 30 Q 43 50 36 70" fill="none" stroke="#888" strokeWidth="1.5" />
            <path d="M 64 30 Q 57 50 64 70" fill="none" stroke="#888" strokeWidth="1.5" />
          </g>
        </svg>
      </div>
      <p className="loading-text">{text}</p>
    </div>
  )
}
