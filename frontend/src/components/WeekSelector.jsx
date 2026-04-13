import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'

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
      <LoadingSpinner text="Loading rounds..." />
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
