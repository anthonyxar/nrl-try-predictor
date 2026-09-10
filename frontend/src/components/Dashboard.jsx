import React, { useEffect, useMemo, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'
import { fetchJson } from '../api'

const DEFAULT_STAKE = 10

export default function Dashboard({ apiBase }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [retryCount, setRetryCount] = useState(0)
  const [stake, setStake] = useState(DEFAULT_STAKE)
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchJson(`${apiBase}/dashboard`)
      .then(d => { if (!cancelled) { setData(d); setLoading(false) } })
      .catch(e => { if (!cancelled) { setError(e.message); setLoading(false) } })
    return () => { cancelled = true }
  }, [apiBase, retryCount])

  const betting = data?.betting
  const stakeValue = Number.isFinite(stake) && stake > 0 ? stake : 0

  const settledPicks = useMemo(
    () => (betting?.picks || []).filter(p => p.status !== 'pending'),
    [betting]
  )

  const cumulative = useMemo(() => {
    let running = 0
    return settledPicks.map(p => {
      running += p.profit_per_unit_stake * stakeValue
      return running
    })
  }, [settledPicks, stakeValue])

  if (loading) return <LoadingSpinner text="Loading dashboard..." />
  if (error) return (
    <div className="error-container">
      <div className="error-message">{error}</div>
      <button className="nav-btn" onClick={() => setRetryCount(c => c + 1)}>Retry</button>
    </div>
  )
  if (!data) return null

  const win = data.accuracy.win_prediction
  const tries = data.accuracy.try_picks
  const multi = data.accuracy.multi
  const noAccuracyData = win.total === 0

  const totalProfit = (betting.total_profit_per_unit_stake || 0) * stakeValue
  const totalStaked = (betting.settled || 0) * stakeValue

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h2>Dashboard</h2>
        <p className="dashboard-subtitle">How the model has been performing, and what a $ per-game edge bet would have made.</p>
      </div>

      {noAccuracyData ? (
        <div className="accuracy-empty">
          <p>No predictions recorded yet.</p>
          <p>Browse completed matches to start tracking prediction accuracy. Each match you view will be automatically recorded.</p>
        </div>
      ) : (
        <>
          <div className="accuracy-cards">
            <div className="accuracy-card">
              <div className="accuracy-card-title">Win Predictions</div>
              <div className="accuracy-pct">{(win.accuracy * 100).toFixed(1)}%</div>
              <div className="accuracy-detail">{win.correct} / {win.total} correct</div>
              <div className="accuracy-bar-track">
                <div className="accuracy-bar-fill" style={{ width: `${win.accuracy * 100}%` }} />
              </div>
            </div>
            <div className="accuracy-card">
              <div className="accuracy-card-title">Try Picks (Top 3/team)</div>
              <div className="accuracy-pct">{(tries.hit_rate * 100).toFixed(1)}%</div>
              <div className="accuracy-detail">{tries.hits} / {tries.total_picks} scored</div>
              <div className="accuracy-bar-track">
                <div className="accuracy-bar-fill" style={{ width: `${tries.hit_rate * 100}%` }} />
              </div>
            </div>
            <div className="accuracy-card">
              <div className="accuracy-card-title">Multi (All 3 Score)</div>
              <div className="accuracy-pct">{(multi.hit_rate * 100).toFixed(1)}%</div>
              <div className="accuracy-detail">{multi.all_scored} / {multi.total} all scored, avg {multi.avg_hits}/3 hits</div>
              <div className="accuracy-bar-track">
                <div className="accuracy-bar-fill" style={{ width: `${multi.hit_rate * 100}%` }} />
              </div>
            </div>
          </div>
          <Link to="/accuracy" className="dashboard-accuracy-link">View full accuracy breakdown by round &rarr;</Link>
        </>
      )}

      <div className="dashboard-section betting-section">
        <div className="betting-section-header">
          <h3 className="section-title">Betting Edge Profit/Loss</h3>
          <label className="stake-input-label">
            Stake per bet
            <span className="stake-input-wrap">
              <span className="stake-currency">$</span>
              <input
                className="stake-input"
                type="number"
                min="1"
                step="1"
                value={stake}
                onChange={(e) => setStake(e.target.valueAsNumber)}
              />
            </span>
          </label>
        </div>

        {betting.total_picks === 0 ? (
          <div className="accuracy-empty">
            <p>No betting-edge picks tracked yet.</p>
            <p>
              Profit/loss can only be simulated from bookmaker odds captured going forward — historical
              odds weren't stored before this feature shipped. Open an upcoming match's page before
              kickoff (with odds available) to start capturing best-edge picks.
            </p>
          </div>
        ) : (
          <>
            {betting.tracking_since && (
              <p className="betting-tracking-note">
                Tracking best-edge picks since {new Date(betting.tracking_since).toLocaleDateString()}.
                Figures reflect only picks captured from then on, not the model's full history.
              </p>
            )}

            <div className="betting-stat-tiles">
              <div className="betting-stat-tile">
                <span className="betting-stat-value">{betting.total_picks}</span>
                <span className="betting-stat-label">Picks Tracked</span>
              </div>
              <div className="betting-stat-tile">
                <span className="betting-stat-value">{betting.settled}</span>
                <span className="betting-stat-label">Settled ({betting.pending} pending)</span>
              </div>
              <div className="betting-stat-tile">
                <span className="betting-stat-value">{(betting.win_rate * 100).toFixed(1)}%</span>
                <span className="betting-stat-label">Win Rate</span>
              </div>
              <div className="betting-stat-tile">
                <span className="betting-stat-value">{totalStaked.toFixed(0)}</span>
                <span className="betting-stat-label">Total Staked ($)</span>
              </div>
              <div className={`betting-stat-tile ${totalProfit >= 0 ? 'profit' : 'loss'}`}>
                <span className="betting-stat-value">
                  {totalProfit >= 0 ? '+' : '-'}${Math.abs(totalProfit).toFixed(2)}
                </span>
                <span className="betting-stat-label">Total Profit/Loss</span>
              </div>
              <div className={`betting-stat-tile ${betting.roi_per_unit_stake >= 0 ? 'profit' : 'loss'}`}>
                <span className="betting-stat-value">{(betting.roi_per_unit_stake * 100).toFixed(1)}%</span>
                <span className="betting-stat-label">ROI</span>
              </div>
            </div>

            {settledPicks.length > 1 && (
              <div className="pnl-chart-wrap">
                <PnlChart values={cumulative} />
              </div>
            )}

            <div className="betting-picks-table">
              <div className="betting-pick-row betting-pick-heading">
                <span className="bp-round">Rd</span>
                <span className="bp-match">Match</span>
                <span className="bp-player">Pick</span>
                <span className="bp-odds">Odds</span>
                <span className="bp-edge">Edge</span>
                <span className="bp-status">Result</span>
                <span className="bp-profit">P/L</span>
              </div>
              {betting.picks.slice().reverse().map(p => (
                <div
                  key={p.match_url}
                  className={`betting-pick-row clickable ${p.status}`}
                  onClick={() => navigate(`/match?url=${encodeURIComponent(p.match_url)}`)}
                >
                  <span className="bp-round">R{p.round_number}</span>
                  <span className="bp-match">{p.home_team} v {p.away_team}</span>
                  <span className="bp-player">{p.player_name}</span>
                  <span className="bp-odds">{p.bookmaker_decimal_odds.toFixed(2)}</span>
                  <span className="bp-edge">+{(p.edge * 100).toFixed(1)}%</span>
                  <span className="bp-status">
                    <span className={`bp-status-badge ${p.status}`}>{p.status}</span>
                  </span>
                  <span className="bp-profit">
                    {p.status === 'pending' ? '—' : `${p.profit_per_unit_stake >= 0 ? '+' : ''}$${(p.profit_per_unit_stake * stakeValue).toFixed(2)}`}
                  </span>
                </div>
              ))}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

function PnlChart({ values }) {
  const width = 600
  const height = 160
  const pad = 8
  const min = Math.min(0, ...values)
  const max = Math.max(0, ...values)
  const range = max - min || 1

  const points = values.map((v, i) => {
    const x = values.length > 1 ? (i / (values.length - 1)) * (width - pad * 2) + pad : width / 2
    const y = height - pad - ((v - min) / range) * (height - pad * 2)
    return `${x.toFixed(1)},${y.toFixed(1)}`
  }).join(' ')

  const zeroY = height - pad - ((0 - min) / range) * (height - pad * 2)
  const last = values[values.length - 1]
  const lineColour = last >= 0 ? 'var(--accent)' : '#dc2626'

  return (
    <svg className="pnl-chart" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      <line x1={pad} y1={zeroY} x2={width - pad} y2={zeroY} stroke="var(--border)" strokeWidth="1" strokeDasharray="4 4" />
      <polyline points={points} fill="none" stroke={lineColour} strokeWidth="2.5" strokeLinejoin="round" strokeLinecap="round" />
    </svg>
  )
}
