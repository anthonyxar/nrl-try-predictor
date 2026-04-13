import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'

export default function WeekSelector({ apiBase }) {
  const [rounds, setRounds] = useState({})
  const [loading, setLoading] = useState(true)
  const navigate = useNavigate()

  useEffect(() => {
    fetch(`${apiBase}/rounds`)
      .then(r => r.json())
      .then(data => { setRounds(data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [apiBase])

  if (loading) return (
    <div className="week-selector">
      <h2>Select a Round</h2>
      <div className="rounds-grid">
        {Array.from({length: 27}, (_, i) => (
          <div key={i} className="round-card skeleton-round">
            <div className="skeleton-line skeleton-short" />
            <div className="skeleton-line skeleton-short" />
          </div>
        ))}
      </div>
    </div>
  )

  return (
    <div className="week-selector">
      <h2>Select a Round</h2>
      <div className="rounds-grid">
        {Object.entries(rounds).map(([num, info]) => (
          <button
            key={num}
            className="round-card"
            onClick={() => navigate(`/round/${num}`)}
          >
            <span className="round-number">Round {num}</span>
            <span className="match-count">{info.match_count} matches</span>
          </button>
        ))}
      </div>
    </div>
  )
}
