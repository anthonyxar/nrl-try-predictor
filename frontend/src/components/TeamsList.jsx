import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'
import { fetchJson } from '../api'

const SEASONS = [2026, 2025, 2024, 2023, 2022, 2021, 2020]

export default function TeamsList({ apiBase }) {
  const [teams, setTeams] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [season, setSeason] = useState(2026)
  const [retryCount, setRetryCount] = useState(0)
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchJson(`${apiBase}/teams`)
      .then(data => { if (!cancelled) { setTeams(data); setLoading(false) } })
      .catch(e => { if (!cancelled) { setError(e.message); setLoading(false) } })
    return () => { cancelled = true }
  }, [apiBase, retryCount])

  const goToTeam = (name) => navigate(`/team?name=${encodeURIComponent(name)}&season=${season}`)

  if (loading) return <LoadingSpinner text="Loading teams..." />
  if (error) return (
    <div className="error-container">
      <div className="error-message">{error}</div>
      <button className="nav-btn" onClick={() => setRetryCount(c => c + 1)}>Retry</button>
    </div>
  )

  return (
    <div className="teams-list">
      <div className="list-page-header">
        <h2>Team Stats</h2>
        <select className="season-select" value={season} onChange={(e) => setSeason(Number(e.target.value))}>
          {SEASONS.map(y => <option key={y} value={y}>{y}</option>)}
        </select>
      </div>

      <div className="teams-grid">
        {teams.map(t => (
          <button key={t.name} className="team-card" onClick={() => goToTeam(t.name)}>
            <div className="team-card-badge-wrap">
              <img
                className="team-card-badge"
                src={`https://www.nrl.com/.theme/${t.theme_key || 'nrl'}/badge.svg`}
                alt={t.name}
                onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
              />
              <div className="team-card-badge-fallback" style={{ backgroundColor: t.colour, display: 'none' }}>
                {t.name.substring(0, 3).toUpperCase()}
              </div>
            </div>
            <span className="team-card-name">{t.name}</span>
          </button>
        ))}
      </div>
    </div>
  )
}
