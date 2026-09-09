import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'
import { fetchJson } from '../api'

export default function WeekSelector({ apiBase }) {
  const [rounds, setRounds] = useState({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryCount, setRetryCount] = useState(0)
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchJson(`${apiBase}/rounds`)
      .then(data => { if (!cancelled) { setRounds(data); setLoading(false) } })
      .catch(e => { if (!cancelled) { setError(e.message); setLoading(false) } })
    return () => { cancelled = true }
  }, [apiBase, retryCount])

  if (loading) return (
    <div className="week-selector">
      <h2>Select a Round</h2>
      <LoadingSpinner text="Loading rounds..." />
    </div>
  )

  if (error) return (
    <div className="week-selector">
      <h2>Select a Round</h2>
      <div className="error-message">{error}</div>
      <button className="nav-btn" onClick={() => setRetryCount(c => c + 1)}>Retry</button>
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
            <span className="round-number">{info.name || `Round ${num}`}</span>
          </button>
        ))}
      </div>
    </div>
  )
}
