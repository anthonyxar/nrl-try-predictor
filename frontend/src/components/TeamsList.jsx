import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'
import TeamSelect from './TeamSelect'
import { fetchJson } from '../api'

const SEASONS = [2026, 2025, 2024, 2023, 2022, 2021, 2020]

export default function TeamsList({ apiBase }) {
  const [teams, setTeams] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [season, setSeason] = useState(2026)
  const [retryCount, setRetryCount] = useState(0)
  const [ladder, setLadder] = useState(null)
  const [ladderLoading, setLadderLoading] = useState(true)
  const [ladderError, setLadderError] = useState(null)
  const [ladderRetry, setLadderRetry] = useState(0)
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

  useEffect(() => {
    let cancelled = false
    setLadderLoading(true)
    setLadderError(null)
    fetchJson(`${apiBase}/ladder?season=${season}`)
      .then(data => { if (!cancelled) { setLadder(data.ladder); setLadderLoading(false) } })
      .catch(e => { if (!cancelled) { setLadderError(e.message); setLadderLoading(false) } })
    return () => { cancelled = true }
  }, [apiBase, season, ladderRetry])

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
        <div className="teams-filters">
          <TeamSelect teams={teams} value="" onChange={goToTeam} placeholder="Jump to team..." />
          <select className="season-select" value={season} onChange={(e) => setSeason(Number(e.target.value))}>
            {SEASONS.map(y => <option key={y} value={y}>{y}</option>)}
          </select>
        </div>
      </div>

      <div className="ladder-section">
        <h3 className="section-title">Ladder &mdash; {season}</h3>
        {ladderLoading ? (
          <LoadingSpinner text="Loading ladder..." />
        ) : ladderError ? (
          <div className="error-container">
            <div className="error-message">{ladderError}</div>
            <button className="nav-btn" onClick={() => setLadderRetry(c => c + 1)}>Retry</button>
          </div>
        ) : ladder && ladder.length > 0 ? (
          <div className="ladder-table-wrap">
            <div className="ladder-table">
              <div className="ladder-row ladder-heading">
                <span className="ld-pos">#</span>
                <span className="ld-team">Team</span>
                <span className="ld-num">P</span>
                <span className="ld-num">W</span>
                <span className="ld-num">D</span>
                <span className="ld-num">L</span>
                <span className="ld-num">PF</span>
                <span className="ld-num">PA</span>
                <span className="ld-num">PD</span>
                <span className="ld-num ld-pts">Pts</span>
                <span className="ld-streak">Streak</span>
              </div>
              {ladder.map(row => (
                <div
                  key={row.team}
                  className={`ladder-row clickable ${row.position === 8 ? 'finals-cutoff' : ''}`}
                  onClick={() => goToTeam(row.team)}
                >
                  <span className="ld-pos">
                    {row.position}
                    {row.movement === 'up' && <span className="ld-move up">&#9650;</span>}
                    {row.movement === 'down' && <span className="ld-move down">&#9660;</span>}
                  </span>
                  <span className="ld-team">
                    <img
                      className="ld-team-badge"
                      src={`https://www.nrl.com/.theme/${row.theme_key || 'nrl'}/badge.svg`}
                      alt=""
                      onError={(e) => { e.target.style.display = 'none' }}
                    />
                    {row.team}
                  </span>
                  <span className="ld-num">{row.played}</span>
                  <span className="ld-num">{row.wins}</span>
                  <span className="ld-num">{row.drawn}</span>
                  <span className="ld-num">{row.lost}</span>
                  <span className="ld-num">{row.points_for}</span>
                  <span className="ld-num">{row.points_against}</span>
                  <span className="ld-num">{row.points_diff > 0 ? `+${row.points_diff}` : row.points_diff}</span>
                  <span className="ld-num ld-pts">{row.comp_points}</span>
                  <span className={`ld-streak ${row.streak?.endsWith('W') ? 'win' : row.streak?.endsWith('L') ? 'loss' : ''}`}>
                    {row.streak}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ) : (
          <div className="error-message">No ladder data available for {season}.</div>
        )}
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
