import React, { useEffect, useState } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import PlayerCard from './PlayerCard'
import LoadingSpinner from './LoadingSpinner'
import { fetchJson } from '../api'

export default function MatchDetail({ apiBase }) {
  const [searchParams] = useSearchParams()
  const matchUrl = searchParams.get('url')
  const [match, setMatch] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryCount, setRetryCount] = useState(0)
  const [viewMode, setViewMode] = useState('ranked')
  const [activeTab, setActiveTab] = useState('home')
  const [roundMatches, setRoundMatches] = useState(null)

  // Finals weeks don't carry a "round-N" segment in their NRL match_url —
  // they're "finals-week-N" instead (e.g. ".../2026/finals-week-1/..."), so
  // the regular-season regex alone can't find the round for those matches.
  // Mirrors backend/main.py's _round_number_from_url. REGULAR_SEASON_ROUNDS
  // must stay in sync with TOTAL_ROUNDS (nrl_client.py) — see AGENTS.md.
  const REGULAR_SEASON_ROUNDS = 27
  const roundNumber = (() => {
    if (!matchUrl) return null
    const regular = matchUrl.match(/round-(\d+)/)
    if (regular) return regular[1]
    const finals = matchUrl.match(/finals-week-(\d+)/)
    if (finals) return String(REGULAR_SEASON_ROUNDS + parseInt(finals[1], 10))
    return null
  })()

  // The match's own season, so a past-season match's round nav doesn't land
  // on the current season's round N (mirrors backend/main.py's season_m regex).
  const matchSeason = (() => {
    if (!matchUrl) return null
    const m = matchUrl.match(/\/(\d{4})\//)
    return m ? m[1] : null
  })()
  const roundLink = roundNumber
    ? `/round/${roundNumber}${matchSeason ? `?season=${matchSeason}` : ''}`
    : '/predictions'

  // Fetch match detail
  useEffect(() => {
    if (!matchUrl) { setError('No match URL provided'); setLoading(false); return }

    setMatch(null)
    setError(null)
    setLoading(true)
    setActiveTab('home')
    window.scrollTo(0, 0)

    let cancelled = false
    fetchJson(`${apiBase}/match?url=${encodeURIComponent(matchUrl)}`)
      .then(data => { if (!cancelled) { setMatch(data); setLoading(false) } })
      .catch(e => {
        if (cancelled) return
        const message = e.status === 403
          ? 'Team lists have not been announced for this match yet'
          : e.message || 'Could not load match data'
        setError(message)
        setLoading(false)
      })

    return () => { cancelled = true }
  }, [apiBase, matchUrl, retryCount])

  // Fetch round matches for prev/next navigation
  useEffect(() => {
    if (!roundNumber) return
    const seasonQuery = matchSeason ? `?season=${matchSeason}` : ''
    fetchJson(`${apiBase}/rounds/${roundNumber}${seasonQuery}`)
      .then(data => { if (data?.matches) setRoundMatches(data.matches) })
      .catch(() => {})
  }, [apiBase, roundNumber, matchSeason])

  // Build prev/next match links
  const currentMatchIndex = roundMatches && matchUrl
    ? roundMatches.findIndex(m => m.match_url && matchUrl.includes(m.match_url))
    : -1
  const prevMatch = currentMatchIndex > 0 ? roundMatches[currentMatchIndex - 1] : null
  const nextMatch = currentMatchIndex >= 0 && currentMatchIndex < (roundMatches?.length || 0) - 1
    ? roundMatches[currentMatchIndex + 1] : null

  const matchNavLabel = (m) => {
    if (!m) return ''
    return `${m.home_team || '?'} vs ${m.away_team || '?'}`
  }

  // Compact 3-letter form for the Prev/Next nav buttons — full names made
  // that bar too wide on mobile. Same first-3-letters convention already
  // used for the team-badge fallback in MatchCard.jsx, so it stays one
  // shorthand rule instead of introducing a second official-code table.
  const matchNavCode = (m) => {
    if (!m) return ''
    const code = (name) => (name || '?').substring(0, 3).toUpperCase()
    return `${code(m.home_team)} vs ${code(m.away_team)}`
  }

  if (loading) return (
    <div className="match-detail">
      <div className="nav-bar sticky">
        <Link to={roundLink} className="nav-btn">
          &larr; {roundNumber ? `Round ${roundNumber}` : 'Rounds'}
        </Link>
      </div>
      <LoadingSpinner text="Crunching the numbers..." />
    </div>
  )
  if (error) return (
    <div className="error-container">
      <div className="nav-bar sticky">
        <Link to={roundLink} className="nav-btn">&larr; Back</Link>
      </div>
      <div className="error-message">{error}</div>
      <button className="nav-btn" onClick={() => setRetryCount(c => c + 1)}>Retry</button>
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
      <div className="nav-bar sticky">
        <Link to={roundLink} className="nav-btn">
          &larr; {roundNumber ? `Round ${roundNumber}` : 'Rounds'}
        </Link>
        {roundMatches && roundMatches.length > 1 && (
          <div className="nav-bar-group">
            <div className="nav-bar-side nav-bar-side-left">
              {prevMatch ? (
                <Link to={`/match?url=${encodeURIComponent(prevMatch.match_url)}`} className="nav-btn" title={matchNavLabel(prevMatch)}>
                  &larr; {matchNavCode(prevMatch)}
                </Link>
              ) : (
                <span className="nav-btn disabled">&larr; —</span>
              )}
            </div>
            <span className="nav-bar-divider" aria-hidden>|</span>
            <div className="nav-bar-side nav-bar-side-right">
              {nextMatch ? (
                <Link to={`/match?url=${encodeURIComponent(nextMatch.match_url)}`} className="nav-btn" title={matchNavLabel(nextMatch)}>
                  {matchNavCode(nextMatch)} &rarr;
                </Link>
              ) : (
                <span className="nav-btn disabled">— &rarr;</span>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Combined match header + win prediction + team summaries */}
      <div className="match-overview">
        <div className="match-detail-header">
          <div className="detail-team home">
            <Link to={`/team?name=${encodeURIComponent(match.home_nickname)}`} className="detail-team-link" aria-label={`View ${match.home_team} stats`}>
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
            </Link>
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
            <Link to={`/team?name=${encodeURIComponent(match.away_nickname)}`} className="detail-team-link" aria-label={`View ${match.away_team} stats`}>
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
            </Link>
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

      {/* Best Edge — edge-based when bookmaker odds available, otherwise heuristic fallback */}
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
            <div className="edge-picks-column">
              <h4 style={{ color: colour }}>{picks[0].team}</h4>
              {picks.map((vp, i) => {
                const scored = isCompleted && actualSet.has(vp.name)
                return (
                  <div key={i} className={`edge-pick-row ${isCompleted ? (scored ? 'hit' : 'miss') : ''}`}>
                    <div className="edge-pick-main">
                      <span className="pick-rank">{i + 1}.</span>
                      <span className="pick-name">{vp.name}</span>
                      <span className="pick-pos">#{vp.number} {vp.position}</span>
                      <span className="pick-pct">{vp.try_percentage}%</span>
                      {isCompleted && (
                        <span className={`pick-result ${scored ? 'hit' : 'miss'}`}>
                          {scored ? 'SCORED' : 'NO TRY'}
                        </span>
                      )}
                    </div>
                    <div className="edge-pick-odds">
                      <span>Model ${vp.model_odds.toFixed(2)} vs {vp.bestBookmaker} ${vp.bestDecimal.toFixed(2)}</span>
                      <span className="edge-pick-badge">+{(vp.bestEdge * 100).toFixed(1)}%</span>
                    </div>
                  </div>
                )
              })}
            </div>
          )
        )

        if (hasEdgePicks) {
          return (
            <div className="edge-picks-section">
              <h3 className="section-title">Best Edge</h3>
              <p className="edge-picks-subtitle">Players where model probability beats the best bookmaker price — biggest edge first</p>
              <div className="edge-picks-columns">
                {renderEdgeColumn(homeEdgePicks, match.home_colour, actualHome)}
                {renderEdgeColumn(awayEdgePicks, match.away_colour, actualAway)}
              </div>
            </div>
          )
        }

        // Fallback: old heuristic picks (no bookmaker odds configured)
        const renderFallbackColumn = (picks, colour, name) => (
          picks && picks.length > 0 && (
            <div className="edge-picks-column">
              <h4 style={{ color: colour }}>{name}</h4>
              {picks.map((vp, i) => (
                <div key={i} className={`edge-pick-row ${isCompleted ? (vp.scored ? 'hit' : 'miss') : ''}`}>
                  <div className="edge-pick-main">
                    <span className="pick-rank">{vp.rank}.</span>
                    <span className="pick-name">{vp.name}</span>
                    <span className="pick-pos">#{vp.number} {vp.position}</span>
                    <span className="pick-pct">{vp.try_percentage}%</span>
                    {isCompleted && (
                      <span className={`pick-result ${vp.scored ? 'hit' : 'miss'}`}>
                        {vp.scored ? 'SCORED' : 'NO TRY'}
                      </span>
                    )}
                  </div>
                  <div className="edge-pick-reasons">
                    {vp.reasons.map((reason, j) => (
                      <span key={j} className="edge-pick-reason">{reason}</span>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )
        )

        return (
          <div className="edge-picks-section">
            <h3 className="section-title">Best Edge</h3>
            <p className="edge-picks-subtitle">No live bookmaker odds available — showing the model's most fancied outsiders instead</p>
            <div className="edge-picks-columns">
              {renderFallbackColumn(match.value_picks_home, match.home_colour, match.home_nickname)}
              {renderFallbackColumn(match.value_picks_away, match.away_colour, match.away_nickname)}
            </div>
          </div>
        )
      })()}

      {/* Last time these teams met */}
      {match.h2h_recent_tries && match.h2h_recent_tries.length > 0 && (
        <div className="h2h-section">
          <h3 className="section-title">Last Time These Teams Met</h3>
          <p className="h2h-subtitle">
            Try scorers from their {match.h2h_recent_tries.length === 1 ? 'previous meeting' : `last ${match.h2h_recent_tries.length} meetings`}
          </p>
          {match.h2h_recent_tries.map((game, i) => {
            const gameHomeColour = game.home_team === match.home_nickname ? match.home_colour : match.away_colour
            const gameAwayColour = game.home_team === match.home_nickname ? match.away_colour : match.home_colour
            const homeScorers = game.try_scorers.filter(s => s.team === game.home_team)
            const awayScorers = game.try_scorers.filter(s => s.team === game.away_team)
            return (
              <div key={i} className="h2h-game">
                <div className="h2h-game-header">
                  <span className="h2h-game-round">{game.round_title || `Round ${game.round_number}`}, {game.season}</span>
                  <span className="h2h-game-score">
                    <span style={{ color: gameHomeColour }}>{game.home_team} {game.home_score}</span>
                    {' – '}
                    <span style={{ color: gameAwayColour }}>{game.away_score} {game.away_team}</span>
                  </span>
                </div>
                <div className="scoring-summary h2h-scoring-summary">
                  <div className="scoring-team">
                    <h4 style={{ color: gameHomeColour }}>{game.home_team}</h4>
                    <div className="try-list">
                      {homeScorers.length > 0 ? homeScorers.map((s, j) => (
                        <span key={j} className="try-entry">{s.name}{s.tries > 1 && <span className="try-min"> x{s.tries}</span>}</span>
                      )) : <span className="no-tries">No tries</span>}
                    </div>
                  </div>
                  <div className="scoring-divider" />
                  <div className="scoring-team">
                    <h4 style={{ color: gameAwayColour }}>{game.away_team}</h4>
                    <div className="try-list">
                      {awayScorers.length > 0 ? awayScorers.map((s, j) => (
                        <span key={j} className="try-entry">{s.name}{s.tries > 1 && <span className="try-min"> x{s.tries}</span>}</span>
                      )) : <span className="no-tries">No tries</span>}
                    </div>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      )}

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
