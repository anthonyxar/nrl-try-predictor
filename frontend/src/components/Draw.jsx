import React, { useEffect, useState } from 'react'
import { useParams, useNavigate, Link } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'
import MatchCard from './MatchCard'
import { fetchJson } from '../api'

// 27 regular-season rounds + 4 finals weeks — must match backend/nrl_client.py's TOTAL_ROUNDS
const TOTAL_ROUNDS = 31

export default function Draw({ apiBase }) {
  const { roundNumber } = useParams()
  const [roundData, setRoundData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryCount, setRetryCount] = useState(0)
  const navigate = useNavigate()

  useEffect(() => {
    setRoundData(null)
    setLoading(true)
    setError(null)
    window.scrollTo(0, 0)

    let cancelled = false
    fetchJson(`${apiBase}/rounds/${roundNumber}`)
      .then(data => {
        if (cancelled) return
        setRoundData(data)
        setLoading(false)
        // Prefetch adjacent rounds in background
        const rn = parseInt(roundNumber)
        if (rn > 1) fetchJson(`${apiBase}/rounds/${rn - 1}`).catch(() => {})
        if (rn < TOTAL_ROUNDS) fetchJson(`${apiBase}/rounds/${rn + 1}`).catch(() => {})
      })
      .catch(e => { if (!cancelled) { setError(e.message); setLoading(false) } })

    return () => { cancelled = true }
  }, [apiBase, roundNumber, retryCount])

  if (loading) return (
    <div className="draw">
      <div className="nav-bar sticky">
        <Link to="/predictions" className="nav-btn">&larr; All Rounds</Link>
        <h2 className="nav-bar-title">Round {roundNumber}</h2>
      </div>
      <LoadingSpinner text={`Loading Round ${roundNumber} predictions...`} />
    </div>
  )
  if (error) return (
    <div className="error-container">
      <div className="nav-bar sticky">
        <Link to="/predictions" className="nav-btn">&larr; All Rounds</Link>
      </div>
      <div className="error-message">{error}</div>
      <button className="nav-btn" onClick={() => setRetryCount(c => c + 1)}>Retry</button>
    </div>
  )
  if (!roundData) return <div className="error">Round not found</div>

  const handleMatchClick = (match) => {
    if (!match.match_url) return
    navigate(`/match?url=${encodeURIComponent(match.match_url)}`)
  }

  const currentRound = parseInt(roundNumber)
  const prevRound = currentRound > 1 ? currentRound - 1 : null
  const nextRound = currentRound < TOTAL_ROUNDS ? currentRound + 1 : null

  return (
    <div className="draw">
      <div className="nav-bar sticky">
        <Link to="/predictions" className="nav-btn">&larr; All Rounds</Link>
        <div className="nav-bar-group">
          <div className="nav-bar-side nav-bar-side-left">
            {prevRound ? (
              <Link to={`/round/${prevRound}`} className="nav-btn" aria-label={`Previous round (Round ${prevRound})`}>
                &larr; R{prevRound}
              </Link>
            ) : (
              <span className="nav-btn disabled" aria-hidden>&larr; R—</span>
            )}
          </div>
          <h2 className="nav-bar-title">{roundData.name}</h2>
          <div className="nav-bar-side nav-bar-side-right">
            {nextRound ? (
              <Link to={`/round/${nextRound}`} className="nav-btn" aria-label={`Next round (Round ${nextRound})`}>
                R{nextRound} &rarr;
              </Link>
            ) : (
              <span className="nav-btn disabled" aria-hidden>R— &rarr;</span>
            )}
          </div>
        </div>
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
        {roundData.matches.map((match, idx) => (
          <MatchCard key={match.match_id || idx} match={match} onClick={() => handleMatchClick(match)} />
        ))}
      </div>
    </div>
  )
}
