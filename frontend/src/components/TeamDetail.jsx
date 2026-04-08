import React, { useEffect, useState } from 'react'
import { useSearchParams, Link, useNavigate } from 'react-router-dom'

export default function TeamDetail({ apiBase }) {
  const [searchParams] = useSearchParams()
  const teamName = searchParams.get('name')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [season, setSeason] = useState(2026)
  const navigate = useNavigate()

  useEffect(() => {
    if (!teamName) { setError('No team name provided'); setLoading(false); return }
    setLoading(true)
    fetch(`${apiBase}/team?name=${encodeURIComponent(teamName)}&season=${season}`)
      .then(r => {
        if (!r.ok) throw new Error('Could not load team data')
        return r.json()
      })
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }, [apiBase, teamName, season])

  if (loading) return <div className="loading">Loading team data...</div>
  if (error) return (
    <div className="error-container">
      <Link to="/" className="back-link">&larr; Back</Link>
      <div className="error-message">{error}</div>
    </div>
  )
  if (!data) return null

  const s = data.stats

  return (
    <div className="team-detail">
      <Link to="/" className="back-link">&larr; All Rounds</Link>

      <div className="team-detail-header">
        <div className="team-detail-badge-wrap">
          <img
            className="team-detail-badge"
            src={`https://www.nrl.com/.theme/${data.theme_key || 'nrl'}/badge.svg`}
            alt={data.name}
            onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
          />
          <div className="team-detail-badge-fallback" style={{ backgroundColor: data.colour, display: 'none' }}>
            {data.name.substring(0, 3).toUpperCase()}
          </div>
        </div>
        <div className="team-detail-info">
          <h2 style={{ color: data.colour }}>{data.name}</h2>
          <div className="team-form-badges">
            {data.form.map((r, i) => (
              <span key={i} className={`form-badge ${r === 'W' ? 'win' : r === 'D' ? 'draw' : 'loss'}`}>{r}</span>
            ))}
          </div>
        </div>
      </div>

      <div className="team-stats-grid">
        <div className="team-stat-card">
          <span className="team-stat-value">{s.avg_scored}</span>
          <span className="team-stat-label">Avg Scored</span>
        </div>
        <div className="team-stat-card">
          <span className="team-stat-value">{s.avg_conceded}</span>
          <span className="team-stat-label">Avg Conceded</span>
        </div>
        <div className="team-stat-card">
          <span className="team-stat-value">{s.wins}/{s.played}</span>
          <span className="team-stat-label">Wins (Last 10)</span>
        </div>
        <div className="team-stat-card">
          <span className="team-stat-value">{(s.home_win_rate * 100).toFixed(0)}%</span>
          <span className="team-stat-label">Home Win %</span>
        </div>
        <div className="team-stat-card">
          <span className="team-stat-value">{(s.away_win_rate * 100).toFixed(0)}%</span>
          <span className="team-stat-label">Away Win %</span>
        </div>
      </div>

      {/* Team Summary */}
      {data.summary && (
        <div className="team-summary-full">
          <div className="team-summary-section">
            <h4 className={`summary-heading attack-${data.summary.attack_rating}`}>Attack</h4>
            <div className="summary-points">
              {data.summary.attack.map((pt, i) => (
                <span key={i} className={`summary-point ${pt.type}`}>{pt.text}</span>
              ))}
            </div>
          </div>
          <div className="team-summary-section">
            <h4 className={`summary-heading defence-${data.summary.defence_rating}`}>Defence</h4>
            <div className="summary-points">
              {data.summary.defence.map((pt, i) => (
                <span key={i} className={`summary-point ${pt.type}`}>{pt.text}</span>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Edge Vulnerability */}
      {data.edge_vulnerability && Object.keys(data.edge_vulnerability).length > 0 && (
        <div className="edge-vuln-section">
          <h3 className="section-title">Defensive Edge Vulnerability</h3>
          <div className="edge-vuln-grid">
            {Object.entries(data.edge_vulnerability).map(([edge, info]) => (
              <div key={edge} className={`edge-vuln-card ${info.vulnerability >= 1.2 ? 'weak' : info.vulnerability <= 0.8 ? 'strong' : ''}`}>
                <span className="edge-name">{edge}</span>
                <span className="edge-rate">{info.rate_per_game} tries/game</span>
                <span className="edge-vuln-val">{info.vulnerability}x avg</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Results */}
      <div className="recent-results-section">
        <h3 className="section-title">Recent Results</h3>
        <div className="results-table">
          {data.recent_results.map((r, i) => (
            <div key={i} className={`result-row ${r.result === 'W' ? 'win-row' : 'loss-row'}`}>
              <span className="result-round">R{r.round}</span>
              <span className="result-vs">{r.is_home ? 'vs' : '@'}</span>
              <span className="result-opponent">{r.opponent}</span>
              <span className="result-score">{r.team_score}-{r.opp_score}</span>
              <span className={`result-badge ${r.result === 'W' ? 'win' : 'loss'}`}>{r.result}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Roster */}
      <div className="roster-section">
        <div className="roster-header">
          <h3 className="section-title">Squad</h3>
          <select className="season-select" value={season} onChange={(e) => setSeason(Number(e.target.value))}>
            {[2026, 2025, 2024, 2023, 2022, 2021, 2020].map(y => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
        <div className="roster-table">
          <div className="roster-row roster-heading">
            <span className="roster-num">#</span>
            <span className="roster-name">Player</span>
            <span className="roster-pos">Position</span>
            <span className="roster-games">Games</span>
            <span className="roster-tries">Tries</span>
          </div>
          {data.roster.map((p, i) => (
            <div key={i} className="roster-row clickable"
              onClick={() => navigate(`/player?name=${encodeURIComponent(p.name)}`)}>
              <span className="roster-num">{p.jersey_number}</span>
              <span className="roster-name">{p.name}</span>
              <span className="roster-pos">{p.position}</span>
              <span className="roster-games">{p.games}</span>
              <span className="roster-tries">{p.tries > 0 ? p.tries : '—'}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
