import React, { useEffect, useState } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'

const TEAM_COLOURS = {
  'broncos': '#6D2735',
  'raiders': '#56B947',
  'bulldogs': '#005DB5',
  'sharks': '#00A5DB',
  'titans': '#E8B825',
  'sea-eagles': '#6D2735',
  'storm': '#552D6D',
  'knights': '#005DB5',
  'cowboys': '#002B5C',
  'eels': '#005DB5',
  'panthers': '#2A2A2A',
  'rabbitohs': '#003B2F',
  'dragons': '#E2231A',
  'roosters': '#003B7B',
  'warriors': '#636466',
  'wests-tigers': '#F47920',
  'dolphins': '#C8102E',
}

export default function Draw({ apiBase }) {
  const { roundNumber } = useParams()
  const [roundData, setRoundData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [modelVersion, setModelVersion] = useState(2)
  const [versionLoading, setVersionLoading] = useState(false)
  const navigate = useNavigate()

  useEffect(() => {
    const isVersionSwitch = roundData !== null
    if (isVersionSwitch) setVersionLoading(true)
    else setLoading(true)
    const fetchData = () => {
      fetch(`${apiBase}/rounds/${roundNumber}?version=${modelVersion}`)
        .then(r => {
          if (r.status === 503) { setTimeout(fetchData, 3000); return null }
          return r.json()
        })
        .then(data => { if (data) { setRoundData(data); setLoading(false); setVersionLoading(false) } })
        .catch(() => { setLoading(false); setVersionLoading(false) })
    }
    fetchData()
  }, [apiBase, roundNumber, modelVersion])

  if (loading) return <div className="loading">Loading model data — this may take a moment on first visit...</div>
  if (!roundData) return <div className="error">Round not found</div>

  const formatDate = (dt) => {
    if (!dt) return ''
    const d = new Date(dt)
    return d.toLocaleDateString('en-AU', { weekday: 'short', day: 'numeric', month: 'short' })
  }

  const formatTime = (dt) => {
    if (!dt) return ''
    const d = new Date(dt)
    return d.toLocaleTimeString('en-AU', { hour: '2-digit', minute: '2-digit' })
  }

  const handleMatchClick = (match) => {
    if (!match.match_url) return
    navigate(`/match?url=${encodeURIComponent(match.match_url)}`)
  }

  const getMatchStatus = (match) => {
    const state = (match.match_state || '').toLowerCase()
    if (state === 'fulltime' || state === 'postmatch') return 'completed'
    if (state === 'halftime' || state === 'inprogress') return 'live'
    return 'upcoming'
  }

  return (
    <div className="draw">
      <div className="draw-header">
        <Link to="/" className="back-link">&larr; All Rounds</Link>
        <h2>{roundData.name}</h2>
      </div>

      <div className="version-selector">
        <div className="version-btn-wrap">
          <button className={`version-btn ${modelVersion === 1 ? 'active' : ''}`}
            onClick={(e) => { e.stopPropagation(); setModelVersion(1) }} disabled={versionLoading}>
            V1 <span className="version-desc">Baseline</span>
          </button>
          <div className="version-tooltip">
            <strong>Version 1 — Baseline Model</strong>
            <ul>
              <li>Player try factor based on full career history</li>
              <li>Team attack/defence from last 10 games only</li>
              <li>Win prediction: equal-weighted factors (form 30%, H2H 25%, venue 25%, season 20%)</li>
              <li>No edge vulnerability, weather, or venue-specific adjustments</li>
            </ul>
          </div>
        </div>
        <div className="version-btn-wrap">
          <button className={`version-btn ${modelVersion === 2 ? 'active' : ''}`}
            onClick={(e) => { e.stopPropagation(); setModelVersion(2) }} disabled={versionLoading}>
            V2 <span className="version-desc">Full Model</span>
          </button>
          <div className="version-tooltip">
            <strong>Version 2 — Full Model</strong>
            <ul>
              <li>Player try factor blends 60% last 5 games / 40% career</li>
              <li>Team attack/defence blends 60% last 5 / 40% last 10 games</li>
              <li>Edge vulnerability: adjusts probability based on opponent's left/right/middle/fullback defensive weaknesses</li>
              <li>Venue-specific home advantage based on team's record at that ground</li>
              <li>Weather &amp; ground conditions: wet weather reduces try-scoring rates (backs affected more than forwards)</li>
            </ul>
          </div>
        </div>
        {versionLoading && <span className="version-loading">Updating...</span>}
      </div>

      {roundData.byes && roundData.byes.length > 0 && (
        <div className="byes-banner">
          <strong>BYE:</strong> {roundData.byes.join(', ')}
        </div>
      )}

      {roundData.matches.length === 0 && (
        <div className="error-message">No match data available for this round yet.</div>
      )}

      <div className="matches-grid">
        {roundData.matches.map((match, idx) => {
          const status = getMatchStatus(match)
          const homeColour = TEAM_COLOURS[match.home_theme_key] || '#333'
          const awayColour = TEAM_COLOURS[match.away_theme_key] || '#333'

          const homePick = match.predicted_winner === match.home_team
          const awayPick = match.predicted_winner === match.away_team

          return (
            <div
              key={match.match_id || idx}
              className="match-card clickable"
              onClick={() => handleMatchClick(match)}
            >
              <div className="match-meta">
                <span className="match-date">{formatDate(match.kickoff)}</span>
                <span className="match-time">{formatTime(match.kickoff)}</span>
              </div>
              <div className="match-teams">
                <div className={`team home-team ${homePick ? 'predicted' : ''}`}>
                  <div className="team-badge-wrap" style={{ boxShadow: homePick ? '0 0 0 3px #22c55e' : 'none' }}>
                    <img
                      className="team-logo"
                      src={`https://www.nrl.com/.theme/${match.home_theme_key || 'nrl'}/badge.svg`}
                      alt={match.home_team}
                      onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
                    />
                    <div className="team-badge-fallback" style={{ backgroundColor: homeColour, display: 'none' }}>
                      {(match.home_team || '').substring(0, 3).toUpperCase()}
                    </div>
                  </div>
                  <span className="team-name">{match.home_team}</span>
                  {homePick && <span className="pick-tag">PICK {match.home_win_prob ? `${(match.home_win_prob * 100).toFixed(0)}%` : ''}</span>}
                  {match.odds_comparison && (
                    <span className="team-odds">
                      ${match.odds_comparison.home_decimal.toFixed(2)}
                      {match.odds_comparison.home_value && <span className="value-tag">VALUE</span>}
                    </span>
                  )}
                </div>
                <div className="vs-container">
                  {status === 'completed' && match.home_score != null ? (
                    <span className="score">{match.home_score} - {match.away_score}</span>
                  ) : status === 'live' ? (
                    <span className="score live">LIVE</span>
                  ) : (
                    <span className="vs">VS</span>
                  )}
                  {match.predicted_home_score != null && (
                    <span className="predicted-score">Predicted: {match.predicted_home_score} - {match.predicted_away_score}</span>
                  )}
                  {match.odds_comparison && (
                    <span className="odds-label-centre">Odds</span>
                  )}
                </div>
                <div className={`team away-team ${awayPick ? 'predicted' : ''}`}>
                  <div className="team-badge-wrap" style={{ boxShadow: awayPick ? '0 0 0 3px #22c55e' : 'none' }}>
                    <img
                      className="team-logo"
                      src={`https://www.nrl.com/.theme/${match.away_theme_key || 'nrl'}/badge.svg`}
                      alt={match.away_team}
                      onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
                    />
                    <div className="team-badge-fallback" style={{ backgroundColor: awayColour, display: 'none' }}>
                      {(match.away_team || '').substring(0, 3).toUpperCase()}
                    </div>
                  </div>
                  <span className="team-name">{match.away_team}</span>
                  {awayPick && <span className="pick-tag">PICK {match.away_win_prob ? `${(match.away_win_prob * 100).toFixed(0)}%` : ''}</span>}
                  {match.odds_comparison && (
                    <span className="team-odds">
                      ${match.odds_comparison.away_decimal.toFixed(2)}
                      {match.odds_comparison.away_value && <span className="value-tag">VALUE</span>}
                    </span>
                  )}
                </div>
              </div>
              <div className="match-venue">{match.venue}{match.venue_city ? `, ${match.venue_city}` : ''}</div>
              {status === 'completed' && match.prediction_correct !== undefined && (
                <div className={`prediction-tag ${match.prediction_correct ? 'correct' : 'wrong'}`}>
                  {match.prediction_correct ? 'Prediction correct' : `Predicted ${match.predicted_winner} — ${match.actual_winner} won`}
                </div>
              )}
              <div className="team-list-ready">
                {status === 'completed' ? 'Full Time — View predictions →' :
                 status === 'live' ? 'Match in progress — View predictions →' :
                 'View try predictions →'}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
