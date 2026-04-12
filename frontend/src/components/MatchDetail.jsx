import React, { useEffect, useState } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import PlayerCard from './PlayerCard'

export default function MatchDetail({ apiBase }) {
  const [searchParams] = useSearchParams()
  const matchUrl = searchParams.get('url')
  const [match, setMatch] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [viewMode, setViewMode] = useState('ranked')
  const [activeTab, setActiveTab] = useState('home')
  const [modelVersion, setModelVersion] = useState(3)
  const [versionLoading, setVersionLoading] = useState(false)

  const roundMatch = matchUrl ? matchUrl.match(/round-(\d+)/) : null
  const roundNumber = roundMatch ? roundMatch[1] : null

  useEffect(() => {
    if (!matchUrl) { setError('No match URL provided'); setLoading(false); return }
    const isVersionSwitch = match !== null
    if (isVersionSwitch) setVersionLoading(true)
    else setLoading(true)
    fetch(`${apiBase}/match?url=${encodeURIComponent(matchUrl)}&version=${modelVersion}`)
      .then(r => {
        if (r.status === 403) throw new Error('Team lists have not been announced for this match yet')
        if (!r.ok) throw new Error('Could not load match data')
        return r.json()
      })
      .then(data => { setMatch(data); setLoading(false); setVersionLoading(false) })
      .catch(e => { setError(e.message); setLoading(false); setVersionLoading(false) })
  }, [apiBase, matchUrl, modelVersion])

  if (loading) return <div className="loading">Fetching team lists and calculating predictions...</div>
  if (error) return (
    <div className="error-container">
      <Link to={roundNumber ? `/round/${roundNumber}` : '/'} className="back-link">&larr; Back</Link>
      <div className="error-message">{error}</div>
    </div>
  )
  if (!match) return null

  const positionGroups = {
    'Backs': ['Fullback', 'Winger', 'Centre'],
    'Halves': ['Five-Eighth', 'Halfback'],
    'Forwards': ['Prop', 'Hooker', '2nd Row', 'Lock'],
    'Interchange': ['Interchange'],
  }

  const groupByPosition = (players) => {
    const groups = {}
    for (const [groupName, positions] of Object.entries(positionGroups)) {
      const grouped = players.filter(p => {
        const pos = p.position || ''
        if (groupName === 'Interchange') return p.is_interchange || p.number >= 14 || pos.toLowerCase().includes('interchange')
        return positions.some(pp => pos.toLowerCase().includes(pp.toLowerCase()))
      })
      if (grouped.length > 0) groups[groupName] = grouped.sort((a, b) => (a.number || 0) - (b.number || 0))
    }
    const allGrouped = Object.values(groups).flat()
    const ungrouped = players.filter(p => !allGrouped.includes(p))
    if (ungrouped.length > 0) groups['Other'] = ungrouped
    return groups
  }

  const players = activeTab === 'home' ? match.predictions.home : match.predictions.away
  const teamColour = activeTab === 'home' ? match.home_colour : match.away_colour
  const stats = activeTab === 'home' ? match.home_stats : match.away_stats
  const oppStats = activeTab === 'home' ? match.away_stats : match.home_stats
  const scoring = match.scoring
  const isCompleted = match.is_completed
  const win = match.win_prediction
  const multi = match.multi

  const actualTries = isCompleted && scoring
    ? (activeTab === 'home' ? scoring.home_tries : scoring.away_tries)
    : []

  // Compute best edge per player across all bookmakers (both teams)
  const computeBestEdge = (p) => {
    if (!p.model_odds || !p.bookmaker_odds || p.bookmaker_odds.length === 0) return -Infinity
    const modelProb = 1 / p.model_odds
    return Math.max(...p.bookmaker_odds.map(bk => modelProb - 1 / bk.decimal))
  }

  const allPlayers = [...(match.predictions.home || []), ...(match.predictions.away || [])]
  const edges = allPlayers
    .map(p => computeBestEdge(p))
    .filter(e => e > 0)
    .sort((a, b) => b - a)
  // Top edge threshold: top 3 players or top 10% of positive-edge players, whichever is larger
  const topN = Math.max(3, Math.ceil(edges.length * 0.1))
  const edgeThreshold = edges.length >= topN ? edges[topN - 1] : (edges.length > 0 ? edges[edges.length - 1] : Infinity)

  return (
    <div className="match-detail">
      <Link to={roundNumber ? `/round/${roundNumber}` : '/'} className="back-link">
        &larr; Back to {roundNumber ? `Round ${roundNumber}` : 'Rounds'}
      </Link>

      {/* Model version selector */}
      <div className="version-selector">
        <div className="version-btn-wrap">
          <button className={`version-btn ${modelVersion === 1 ? 'active' : ''}`}
            onClick={() => setModelVersion(1)} disabled={versionLoading}>
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
            onClick={() => setModelVersion(2)} disabled={versionLoading}>
            V2 <span className="version-desc">Enhanced</span>
          </button>
          <div className="version-tooltip">
            <strong>Version 2 — Enhanced Model</strong>
            <ul>
              <li>Player try factor blends 60% last 5 games / 40% career</li>
              <li>Team attack/defence blends 60% last 5 / 40% last 10 games</li>
              <li>Edge vulnerability: adjusts probability based on opponent's defensive weaknesses</li>
              <li>Venue-specific home advantage based on team's record at that ground</li>
              <li>Weather &amp; ground conditions impact</li>
            </ul>
          </div>
        </div>
        <div className="version-btn-wrap">
          <button className={`version-btn ${modelVersion === 3 ? 'active' : ''}`}
            onClick={() => setModelVersion(3)} disabled={versionLoading}>
            V3 <span className="version-desc">Full Model</span>
          </button>
          <div className="version-tooltip">
            <strong>Version 3 — Full Model</strong>
            <ul>
              <li>All V2 features plus:</li>
              <li>Margin-of-victory weighted form (quality of wins/losses)</li>
              <li>Rest days &amp; short turnaround penalties</li>
              <li>Bye-week freshness boost</li>
              <li>Opponent-quality adjusted try rates</li>
              <li>Interchange timing (actual bench minutes)</li>
              <li>Season progression (early-round discount)</li>
              <li>Probability calibration from historical accuracy</li>
            </ul>
          </div>
        </div>
        {versionLoading && <span className="version-loading">Updating...</span>}
      </div>

      {/* Combined match header + win prediction + team summaries */}
      <div className="match-overview">
        <div className="match-detail-header">
          <div className="detail-team home">
            <div className="detail-logo-wrap">
              <img
                className="detail-logo"
                src={`https://www.nrl.com/.theme/${match.home_theme_key || 'nrl'}/badge.svg`}
                alt={match.home_nickname}
                onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
              />
              <div className="detail-badge" style={{ backgroundColor: match.home_colour || '#333', display: 'none' }}>
                {(match.home_nickname || '').substring(0, 3).toUpperCase()}
              </div>
            </div>
            <h3>{match.home_team}</h3>
            {match.home_position && <span className="ladder-pos">{match.home_position}</span>}
          </div>
          <div className="detail-vs">
            {isCompleted && scoring ? (
              <span className="detail-score">{scoring.home_score} - {scoring.away_score}</span>
            ) : (
              <span className="vs-text">VS</span>
            )}
            <span className="detail-venue">{match.venue}</span>
            {(match.weather || match.ground_conditions) && (
              <span className="detail-conditions">
                {match.weather}{match.weather && match.ground_conditions ? ' — ' : ''}{match.ground_conditions}
              </span>
            )}
            {isCompleted && <span className="detail-ft">Full Time</span>}
          </div>
          <div className="detail-team away">
            <div className="detail-logo-wrap">
              <img
                className="detail-logo"
                src={`https://www.nrl.com/.theme/${match.away_theme_key || 'nrl'}/badge.svg`}
                alt={match.away_nickname}
                onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling.style.display = 'flex' }}
              />
              <div className="detail-badge" style={{ backgroundColor: match.away_colour || '#333', display: 'none' }}>
                {(match.away_nickname || '').substring(0, 3).toUpperCase()}
              </div>
            </div>
            <h3>{match.away_team}</h3>
            {match.away_position && <span className="ladder-pos">{match.away_position}</span>}
          </div>
        </div>

        {/* Win prediction bar */}
        {win && (
          <div className="win-prediction-inline">
            <div className="win-prediction-header">
              <span className="win-prediction-title">
                Win Prediction
                {isCompleted && win.correct !== undefined && (
                  <span className={`prediction-result ${win.correct ? 'correct' : 'wrong'}`}>
                    {win.correct ? 'CORRECT' : 'WRONG'}
                  </span>
                )}
              </span>
            </div>
            <div className="win-bar-container">
              <div className="win-bar-label home" style={{ color: match.home_colour }}>
                {match.home_nickname}
                <span className="win-pct">{(win.home_win_prob * 100).toFixed(0)}%</span>
              </div>
              <div className="win-bar">
                <div
                  className="win-bar-fill home"
                  style={{ width: `${win.home_win_prob * 100}%`, backgroundColor: match.home_colour }}
                />
                <div
                  className="win-bar-fill away"
                  style={{ width: `${win.away_win_prob * 100}%`, backgroundColor: match.away_colour }}
                />
              </div>
              <div className="win-bar-label away" style={{ color: match.away_colour }}>
                {match.away_nickname}
                <span className="win-pct">{(win.away_win_prob * 100).toFixed(0)}%</span>
              </div>
            </div>
            {win.predicted_home_score != null && (
              <div className="predicted-score-detail">
                Predicted score: <strong>{match.home_nickname} {win.predicted_home_score}</strong> — <strong>{match.away_nickname} {win.predicted_away_score}</strong>
                {win.weather_impact && <span className="weather-badge">Wet weather adjusted</span>}
              </div>
            )}
            <div className="win-factors">
              <span>H2H: {win.factors.h2h_home_wins}-{win.factors.h2h_away_wins} ({win.factors.h2h_played} games)</span>
              <span>Home form: {(win.factors.home_recent_form * 100).toFixed(0)}%</span>
              <span>Away form: {(win.factors.away_recent_form * 100).toFixed(0)}%</span>
            </div>
            {isCompleted && win.actual_winner && (
              <div className="actual-winner">
                Winner: <strong>{win.actual_winner}</strong>
              </div>
            )}
          </div>
        )}

        {/* Odds Comparison */}
        {match.odds_comparison && (
          <div className="odds-comparison-section">
            <h3 className="odds-comparison-title">Odds Comparison</h3>
            <div className="odds-comparison-grid">
              <div className={`odds-team-card ${match.odds_comparison.home_value ? 'value-bet' : ''}`}>
                <div className="odds-team-name" style={{ color: match.home_colour }}>{match.home_nickname}</div>
                <div className="odds-decimal">${match.odds_comparison.home_decimal.toFixed(2)}</div>
                <div className="odds-prob-row">
                  <div className="odds-prob-item">
                    <span className="odds-prob-label">Odds Implied</span>
                    <span className="odds-prob-value">{(match.odds_comparison.home_implied_prob * 100).toFixed(1)}%</span>
                  </div>
                  <div className="odds-prob-item">
                    <span className="odds-prob-label">Model</span>
                    <span className="odds-prob-value model">{(match.odds_comparison.home_model_prob * 100).toFixed(1)}%</span>
                  </div>
                </div>
                <div className={`odds-edge ${match.odds_comparison.home_edge > 0 ? 'positive' : 'negative'}`}>
                  Edge: {match.odds_comparison.home_edge > 0 ? '+' : ''}{(match.odds_comparison.home_edge * 100).toFixed(1)}%
                </div>
                {match.odds_comparison.home_value && <div className="odds-value-badge">VALUE BET</div>}
              </div>
              <div className={`odds-team-card ${match.odds_comparison.away_value ? 'value-bet' : ''}`}>
                <div className="odds-team-name" style={{ color: match.away_colour }}>{match.away_nickname}</div>
                <div className="odds-decimal">${match.odds_comparison.away_decimal.toFixed(2)}</div>
                <div className="odds-prob-row">
                  <div className="odds-prob-item">
                    <span className="odds-prob-label">Odds Implied</span>
                    <span className="odds-prob-value">{(match.odds_comparison.away_implied_prob * 100).toFixed(1)}%</span>
                  </div>
                  <div className="odds-prob-item">
                    <span className="odds-prob-label">Model</span>
                    <span className="odds-prob-value model">{(match.odds_comparison.away_model_prob * 100).toFixed(1)}%</span>
                  </div>
                </div>
                <div className={`odds-edge ${match.odds_comparison.away_edge > 0 ? 'positive' : 'negative'}`}>
                  Edge: {match.odds_comparison.away_edge > 0 ? '+' : ''}{(match.odds_comparison.away_edge * 100).toFixed(1)}%
                </div>
                {match.odds_comparison.away_value && <div className="odds-value-badge">VALUE BET</div>}
              </div>
            </div>
            <div className="odds-explainer">
              Value bet = model probability exceeds odds-implied probability (positive edge)
            </div>
          </div>
        )}

        {/* Team summaries */}
        {(match.home_summary || match.away_summary) && (
          <div className="team-summaries">
            {[
              { summary: match.home_summary, name: match.home_nickname, colour: match.home_colour },
              { summary: match.away_summary, name: match.away_nickname, colour: match.away_colour },
            ].map(({ summary, name, colour }) => summary && (
              <div key={name} className="team-summary-card">
                <h4 className="team-summary-name" style={{ color: colour }}>{name}</h4>
                <div className="summary-sections">
                  <div className="summary-section">
                    <span className={`summary-label attack-${summary.attack_rating}`}>Attack</span>
                    <div className="summary-points">
                      {summary.attack.map((pt, i) => (
                        <span key={i} className={`summary-point ${pt.type}`}>{pt.text}</span>
                      ))}
                    </div>
                  </div>
                  <div className="summary-section">
                    <span className={`summary-label defence-${summary.defence_rating}`}>Defence</span>
                    <div className="summary-points">
                      {summary.defence.map((pt, i) => (
                        <span key={i} className={`summary-point ${pt.type}`}>{pt.text}</span>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Scoring summary for completed games */}
      {isCompleted && scoring && (
        <div className="scoring-summary">
          <div className="scoring-team">
            <h4 style={{ color: match.home_colour }}>{match.home_nickname} Tries ({scoring.home_tries.length})</h4>
            <div className="try-list">
              {scoring.home_tries.length > 0 ? scoring.home_tries.map((t, i) => (
                <span key={i} className="try-entry">{t.player} <span className="try-min">{t.minute}</span></span>
              )) : <span className="no-tries">No tries</span>}
            </div>
          </div>
          <div className="scoring-divider" />
          <div className="scoring-team">
            <h4 style={{ color: match.away_colour }}>{match.away_nickname} Tries ({scoring.away_tries.length})</h4>
            <div className="try-list">
              {scoring.away_tries.length > 0 ? scoring.away_tries.map((t, i) => (
                <span key={i} className="try-entry">{t.player} <span className="try-min">{t.minute}</span></span>
              )) : <span className="no-tries">No tries</span>}
            </div>
          </div>
        </div>
      )}

      {/* Multi Suggestion */}
      {multi && (
        <div className="multi-section">
          <h3 className="section-title">
            Suggested Try Scorer Multi
            {isCompleted && multi.all_scored !== undefined && (
              <span className={`prediction-result ${multi.all_scored ? 'correct' : 'wrong'}`}>
                {multi.all_scored ? 'ALL SCORED' : `${multi.hits}/3 HIT`}
              </span>
            )}
          </h3>
          <div className="multi-card">
            {multi.picks.map((pick, i) => (
              <div key={i} className={`multi-pick ${isCompleted ? (pick.scored ? 'hit' : 'miss') : ''}`}>
                <span className="multi-pick-team" style={{
                  color: pick.team === match.home_nickname ? match.home_colour : match.away_colour
                }}>{pick.team}</span>
                <span className="multi-pick-name">{pick.name}</span>
                <span className="multi-pick-pos">{pick.position}</span>
                <span className="multi-pick-pct">{pick.try_percentage}%</span>
                {isCompleted && (
                  <span className={`pick-result ${pick.scored ? 'hit' : 'miss'}`}>
                    {pick.scored ? 'SCORED' : 'NO TRY'}
                  </span>
                )}
              </div>
            ))}
            <div className="multi-odds">
              Combined probability: <strong>{multi.multi_percentage}%</strong>
            </div>
          </div>
        </div>
      )}

      {/* Value Picks — edge-based when bookmaker odds available, otherwise heuristic fallback */}
      {(() => {
        const getEdgePicks = (players, teamName) => {
          if (!players) return []
          return players
            .filter(p => p.bookmaker_odds && p.bookmaker_odds.length > 0 && p.model_odds)
            .map(p => {
              const modelProb = 1 / p.model_odds
              const best = p.bookmaker_odds.reduce((best, bk) => {
                const edge = modelProb - 1 / bk.decimal
                return edge > best.edge ? { ...bk, edge } : best
              }, { edge: -Infinity })
              return { ...p, bestEdge: best.edge, bestBookmaker: best.bookmaker, bestDecimal: best.decimal, team: teamName }
            })
            .filter(p => p.bestEdge > 0)
            .sort((a, b) => b.bestEdge - a.bestEdge)
            .slice(0, 5)
        }

        const homeEdgePicks = getEdgePicks(match.predictions.home, match.home_nickname)
        const awayEdgePicks = getEdgePicks(match.predictions.away, match.away_nickname)
        const hasEdgePicks = homeEdgePicks.length > 0 || awayEdgePicks.length > 0

        // Fallback to old heuristic value picks if no bookmaker odds
        const hasOldPicks = !hasEdgePicks && (
          (match.value_picks_home && match.value_picks_home.length > 0) ||
          (match.value_picks_away && match.value_picks_away.length > 0)
        )

        if (!hasEdgePicks && !hasOldPicks) return null

        const actualHome = isCompleted && scoring ? new Set(scoring.home_try_scorers) : new Set()
        const actualAway = isCompleted && scoring ? new Set(scoring.away_try_scorers) : new Set()

        const renderEdgeColumn = (picks, colour, actualSet) => (
          picks.length > 0 && (
            <div className="value-picks-column">
              <h4 style={{ color: colour }}>{picks[0].team}</h4>
              {picks.map((vp, i) => {
                const scored = isCompleted && actualSet.has(vp.name)
                return (
                  <div key={i} className={`value-pick-card ${isCompleted ? (scored ? 'hit' : 'miss') : ''}`}>
                    <div className="value-pick-header">
                      <span className="value-pick-name">{vp.name}</span>
                      <span className="value-pick-pct">{vp.try_percentage}%</span>
                      {isCompleted && (
                        <span className={`pick-result ${scored ? 'hit' : 'miss'}`}>
                          {scored ? 'SCORED' : 'NO TRY'}
                        </span>
                      )}
                    </div>
                    <div className="value-pick-meta">
                      <span className="value-pick-pos">#{vp.number} {vp.position}</span>
                      <span className="value-pick-edge positive">+{(vp.bestEdge * 100).toFixed(1)}% edge</span>
                    </div>
                    <div className="value-pick-reasons">
                      <span className="value-reason">Model: ${vp.model_odds.toFixed(2)}</span>
                      <span className="value-reason">{vp.bestBookmaker}: ${vp.bestDecimal.toFixed(2)}</span>
                    </div>
                  </div>
                )
              })}
            </div>
          )
        )

        if (hasEdgePicks) {
          return (
            <div className="value-picks-section">
              <h3 className="section-title">Value Picks</h3>
              <p className="value-picks-subtitle">Players where model probability exceeds bookmaker odds — best edge first</p>
              <div className="value-picks-columns">
                {renderEdgeColumn(homeEdgePicks, match.home_colour, actualHome)}
                {renderEdgeColumn(awayEdgePicks, match.away_colour, actualAway)}
              </div>
            </div>
          )
        }

        // Fallback: old heuristic picks
        return (
          <div className="value-picks-section">
            <h3 className="section-title">Value Picks</h3>
            <p className="value-picks-subtitle">Lower-ranked players with strong form against vulnerable defences</p>
            <div className="value-picks-columns">
              {match.value_picks_home && match.value_picks_home.length > 0 && (
                <div className="value-picks-column">
                  <h4 style={{ color: match.home_colour }}>{match.home_nickname}</h4>
                  {match.value_picks_home.map((vp, i) => (
                    <div key={i} className={`value-pick-card ${isCompleted ? (vp.scored ? 'hit' : 'miss') : ''}`}>
                      <div className="value-pick-header">
                        <span className="value-pick-name">{vp.name}</span>
                        <span className="value-pick-pct">{vp.try_percentage}%</span>
                        {isCompleted && (
                          <span className={`pick-result ${vp.scored ? 'hit' : 'miss'}`}>
                            {vp.scored ? 'SCORED' : 'NO TRY'}
                          </span>
                        )}
                      </div>
                      <div className="value-pick-meta">
                        <span className="value-pick-pos">#{vp.number} {vp.position}</span>
                        <span className="value-pick-rank">Ranked #{vp.rank}</span>
                      </div>
                      <div className="value-pick-reasons">
                        {vp.reasons.map((reason, j) => (
                          <span key={j} className="value-reason">{reason}</span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
              {match.value_picks_away && match.value_picks_away.length > 0 && (
                <div className="value-picks-column">
                  <h4 style={{ color: match.away_colour }}>{match.away_nickname}</h4>
                  {match.value_picks_away.map((vp, i) => (
                    <div key={i} className={`value-pick-card ${isCompleted ? (vp.scored ? 'hit' : 'miss') : ''}`}>
                      <div className="value-pick-header">
                        <span className="value-pick-name">{vp.name}</span>
                        <span className="value-pick-pct">{vp.try_percentage}%</span>
                        {isCompleted && (
                          <span className={`pick-result ${vp.scored ? 'hit' : 'miss'}`}>
                            {vp.scored ? 'SCORED' : 'NO TRY'}
                          </span>
                        )}
                      </div>
                      <div className="value-pick-meta">
                        <span className="value-pick-pos">#{vp.number} {vp.position}</span>
                        <span className="value-pick-rank">Ranked #{vp.rank}</span>
                      </div>
                      <div className="value-pick-reasons">
                        {vp.reasons.map((reason, j) => (
                          <span key={j} className="value-reason">{reason}</span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )
      })()}

      {/* Top 3 per team */}
      <div className="top3-section">
        <h3 className="section-title">Top 3 Try Scorer Picks Per Team</h3>
        <div className="top3-columns">
          <div className="top3-column">
            <h4 style={{ color: match.home_colour }}>{match.home_nickname}</h4>
            {match.top3_home.map((pick, i) => (
              <div key={i} className={`top3-pick ${isCompleted ? (pick.scored ? 'hit' : 'miss') : ''}`}>
                <span className="pick-rank">{i + 1}.</span>
                <span className="pick-name">{pick.name}</span>
                <span className="pick-pos">{pick.position}</span>
                <span className="pick-pct">{pick.try_percentage}%</span>
                {isCompleted && (
                  <span className={`pick-result ${pick.scored ? 'hit' : 'miss'}`}>
                    {pick.scored ? 'SCORED' : 'NO TRY'}
                  </span>
                )}
              </div>
            ))}
          </div>
          <div className="top3-column">
            <h4 style={{ color: match.away_colour }}>{match.away_nickname}</h4>
            {match.top3_away.map((pick, i) => (
              <div key={i} className={`top3-pick ${isCompleted ? (pick.scored ? 'hit' : 'miss') : ''}`}>
                <span className="pick-rank">{i + 1}.</span>
                <span className="pick-name">{pick.name}</span>
                <span className="pick-pos">{pick.position}</span>
                <span className="pick-pct">{pick.try_percentage}%</span>
                {isCompleted && (
                  <span className={`pick-result ${pick.scored ? 'hit' : 'miss'}`}>
                    {pick.scored ? 'SCORED' : 'NO TRY'}
                  </span>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Full player list */}
      <div className="team-tabs">
        <button className={`tab ${activeTab === 'home' ? 'active' : ''}`}
          style={activeTab === 'home' ? { backgroundColor: match.home_colour || '#22c55e' } : {}}
          onClick={() => setActiveTab('home')}>{match.home_team}</button>
        <button className={`tab ${activeTab === 'away' ? 'active' : ''}`}
          style={activeTab === 'away' ? { backgroundColor: match.away_colour || '#22c55e' } : {}}
          onClick={() => setActiveTab('away')}>{match.away_team}</button>
      </div>

      <div className="model-info">
        <div className="model-stat">
          <span className="stat-label">Avg Pts Scored</span>
          <span className={`stat-value ${(stats.avg_points_scored || 0) >= 22 ? 'good' : 'poor'}`}>
            {stats.avg_points_scored || '—'}</span>
        </div>
        <div className="model-stat">
          <span className="stat-label">Opp Avg Conceded</span>
          <span className={`stat-value ${(oppStats.avg_points_conceded || 0) >= 22 ? 'good' : 'poor'}`}>
            {oppStats.avg_points_conceded || '—'}</span>
        </div>
        <div className="model-stat">
          <span className="stat-label">Completion %</span>
          <span className="stat-value">{stats.completion_rate || '—'}%</span>
        </div>
        <div className="model-stat">
          <span className="stat-label">Record</span>
          <span className="stat-value">{stats.wins || 0}W-{stats.losses || 0}L</span>
        </div>
      </div>

      <div className="view-toggle">
        <button className={viewMode === 'ranked' ? 'active' : ''} onClick={() => setViewMode('ranked')}>
          Ranked by Probability</button>
        <button className={viewMode === 'position' ? 'active' : ''} onClick={() => setViewMode('position')}>
          By Position</button>
      </div>

      {viewMode === 'ranked' ? (
        <div className="players-ranked">
          {players.map((player, idx) => (
            <PlayerCard key={player.number || idx} player={player} rank={idx + 1}
              teamColor={teamColour} actualTries={actualTries} isCompleted={isCompleted} edgeThreshold={edgeThreshold} />
          ))}
        </div>
      ) : (
        <div className="players-by-position">
          {Object.entries(groupByPosition(players)).map(([groupName, groupPlayers]) => (
            <div key={groupName} className="position-group">
              <h4 className="group-title">{groupName}</h4>
              <div className="group-players">
                {groupPlayers.map((player, idx) => (
                  <PlayerCard key={player.number || idx} player={player}
                    teamColor={teamColour} actualTries={actualTries} isCompleted={isCompleted} edgeThreshold={edgeThreshold} />
                ))}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* DB status */}
      {match.db_status && (
        <div className="db-status">
          Model trained on {match.db_status.matches} historical matches and {match.db_status.tries} tries (2020-2026)
        </div>
      )}
    </div>
  )
}
