import React, { useEffect, useState } from 'react'
import { useSearchParams, Link } from 'react-router-dom'

export default function PlayerDetail({ apiBase }) {
  const [searchParams] = useSearchParams()
  const playerName = searchParams.get('name')
  const headshot = searchParams.get('headshot')
  const teamColor = searchParams.get('color') || '#666'
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [filterSeason, setFilterSeason] = useState('all')

  useEffect(() => {
    if (!playerName) { setError('No player name provided'); setLoading(false); return }
    fetch(`${apiBase}/player?name=${encodeURIComponent(playerName)}`)
      .then(r => {
        if (!r.ok) throw new Error('Could not load player data')
        return r.json()
      })
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(e.message); setLoading(false) })
  }, [apiBase, playerName])

  if (loading) return <div className="loading">Loading player history...</div>
  if (error) return (
    <div className="error-container">
      <button onClick={() => window.history.back()} className="back-link">&larr; Back</button>
      <div className="error-message">{error}</div>
    </div>
  )
  if (!data) return null

  const seasons = Object.keys(data.seasons_summary).sort((a, b) => b - a)
  const filteredGames = filterSeason === 'all'
    ? data.games
    : data.games.filter(g => String(g.season) === filterSeason)

  return (
    <div className="player-detail">
      <button onClick={() => window.history.back()} className="back-link">&larr; Back to Match</button>

      <div className="player-detail-header">
        {headshot ? (
          <img className="player-detail-headshot" src={headshot} alt={data.name}
            onError={(e) => { e.target.style.display = 'none' }} />
        ) : (
          <div className="player-detail-badge" style={{ backgroundColor: teamColor }}>
            {data.name.split(' ').map(n => n[0]).join('').substring(0, 2)}
          </div>
        )}
        <div className="player-detail-info">
          <h2>{data.name}</h2>
          <div className="player-detail-meta">
            <span>{data.positions.join(', ')}</span>
            <span>{data.teams.join(', ')}</span>
          </div>
        </div>
      </div>

      <div className="player-stats-grid">
        <div className="player-stat-card">
          <span className="player-stat-value">{data.total_games}</span>
          <span className="player-stat-label">Games</span>
        </div>
        <div className="player-stat-card">
          <span className="player-stat-value">{data.total_tries}</span>
          <span className="player-stat-label">Tries</span>
        </div>
        <div className="player-stat-card">
          <span className="player-stat-value">{(data.try_rate * 100).toFixed(1)}%</span>
          <span className="player-stat-label">Try Rate</span>
        </div>
        <div className="player-stat-card">
          <span className="player-stat-value">{(data.win_rate * 100).toFixed(0)}%</span>
          <span className="player-stat-label">Win Rate</span>
        </div>
      </div>

      <div className="player-season-summary">
        <h3 className="section-title">Tries by Season</h3>
        <div className="season-bars">
          {seasons.map(s => {
            const ss = data.seasons_summary[s]
            return (
              <div key={s} className="season-bar-row">
                <span className="season-label">{s}</span>
                <div className="season-bar-track">
                  <div className="season-bar-fill"
                    style={{ width: `${Math.min((ss.tries / Math.max(...Object.values(data.seasons_summary).map(x => x.tries), 1)) * 100, 100)}%` }} />
                </div>
                <span className="season-stat">{ss.tries} tries / {ss.games} games</span>
              </div>
            )
          })}
        </div>
      </div>

      <div className="player-game-log">
        <div className="game-log-header">
          <h3 className="section-title">Game Log</h3>
          <div className="season-filter">
            <select value={filterSeason} onChange={e => setFilterSeason(e.target.value)}>
              <option value="all">All Seasons</option>
              {seasons.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
        </div>

        <div className="game-log-table">
          <div className="game-log-row game-log-heading">
            <span className="gl-season">Season</span>
            <span className="gl-round">Rd</span>
            <span className="gl-team">Team</span>
            <span className="gl-vs">vs</span>
            <span className="gl-opponent">Opponent</span>
            <span className="gl-score">Score</span>
            <span className="gl-result">Result</span>
            <span className="gl-tries">Tries</span>
          </div>
          {filteredGames.map((g, i) => (
            <div key={i} className={`game-log-row ${g.try_count > 0 ? 'try-game' : ''}`}>
              <span className="gl-season">{g.season}</span>
              <span className="gl-round">{g.round}</span>
              <span className="gl-team">{g.team}</span>
              <span className="gl-vs">{g.is_home ? 'vs' : '@'}</span>
              <span className="gl-opponent">{g.opponent}</span>
              <span className="gl-score">{g.team_score}-{g.opp_score}</span>
              <span className={`gl-result ${g.won ? 'win' : 'loss'}`}>
                {g.won ? 'W' : 'L'}
              </span>
              <span className="gl-tries">
                {g.try_count > 0 ? (
                  <span className="gl-try-badge">
                    {g.try_count} {g.try_count === 1 ? 'try' : 'tries'}
                    {g.tries.length > 0 && <span className="gl-try-mins"> ({g.tries.join(', ')})</span>}
                  </span>
                ) : '—'}
              </span>
            </div>
          ))}
        </div>

        {filteredGames.length === 0 && (
          <div className="no-games">No games found for this filter</div>
        )}
      </div>
    </div>
  )
}
