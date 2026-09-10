import React, { useEffect, useMemo, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'
import { fetchJson } from '../api'

export default function PlayersList({ apiBase }) {
  const [players, setPlayers] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [query, setQuery] = useState('')
  const [position, setPosition] = useState('all')
  const [retryCount, setRetryCount] = useState(0)
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchJson(`${apiBase}/players`)
      .then(data => { if (!cancelled) { setPlayers(data); setLoading(false) } })
      .catch(e => { if (!cancelled) { setError(e.message); setLoading(false) } })
    return () => { cancelled = true }
  }, [apiBase, retryCount])

  const positions = useMemo(() => {
    const set = new Set(players.map(p => p.position).filter(Boolean))
    return Array.from(set).sort()
  }, [players])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    return players.filter(p => {
      if (position !== 'all' && p.position !== position) return false
      if (q && !p.name.toLowerCase().includes(q)) return false
      return true
    })
  }, [players, query, position])

  if (loading) return <LoadingSpinner text="Loading players..." />
  if (error) return (
    <div className="error-container">
      <div className="error-message">{error}</div>
      <button className="nav-btn" onClick={() => setRetryCount(c => c + 1)}>Retry</button>
    </div>
  )

  return (
    <div className="players-list">
      <div className="list-page-header">
        <h2>Player Stats</h2>
        <div className="players-filters">
          <input
            className="players-search-input"
            type="text"
            placeholder="Filter by name..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          <select className="season-select" value={position} onChange={(e) => setPosition(e.target.value)}>
            <option value="all">All Positions</option>
            {positions.map(p => <option key={p} value={p}>{p}</option>)}
          </select>
        </div>
      </div>

      <div className="players-table">
        <div className="roster-row roster-heading">
          <span className="roster-name">Player</span>
          <span className="roster-pos">Position</span>
          <span className="players-team">Team</span>
          <span className="roster-games">Games</span>
          <span className="roster-tries">Tries</span>
        </div>
        {filtered.map(p => (
          <div key={p.name} className="roster-row clickable"
            onClick={() => navigate(`/player?name=${encodeURIComponent(p.name)}`)}>
            <span className="roster-name">{p.name}</span>
            <span className="roster-pos">{p.position || '—'}</span>
            <span className="players-team">{p.team || '—'}</span>
            <span className="roster-games">{p.total_games}</span>
            <span className="roster-tries">{p.total_tries > 0 ? p.total_tries : '—'}</span>
          </div>
        ))}
      </div>

      {filtered.length === 0 && (
        <div className="no-games">No players match this filter</div>
      )}
    </div>
  )
}
