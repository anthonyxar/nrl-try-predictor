import React, { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import LoadingSpinner from './LoadingSpinner'

export default function AccuracyDashboard({ apiBase }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [modelFilter, setModelFilter] = useState(null)

  useEffect(() => {
    setLoading(true)
    const params = modelFilter ? `?model_version=${modelFilter}` : ''
    fetch(`${apiBase}/accuracy${params}`)
      .then(r => r.json())
      .then(d => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [apiBase, modelFilter])

  if (loading) return <LoadingSpinner text="Loading accuracy data..." />
  if (!data) return <div className="error-message">No accuracy data available yet. View some completed matches to start tracking.</div>

  const win = data.win_prediction
  const tries = data.try_picks
  const multi = data.multi

  const noData = win.total === 0

  return (
    <div className="accuracy-dashboard">
      <div className="accuracy-header">
        <Link to="/" className="back-link">&larr; All Rounds</Link>
        <h2>Prediction Accuracy</h2>
      </div>

      <div className="version-selector">
        <div className="version-btn-wrap">
          <button className={`version-btn ${modelFilter === null ? 'active' : ''}`}
            onClick={() => setModelFilter(null)}>
            All <span className="version-desc">Combined</span>
          </button>
        </div>
        <div className="version-btn-wrap">
          <button className={`version-btn ${modelFilter === 1 ? 'active' : ''}`}
            onClick={() => setModelFilter(1)}>
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
          <button className={`version-btn ${modelFilter === 2 ? 'active' : ''}`}
            onClick={() => setModelFilter(2)}>
            V2 <span className="version-desc">Enhanced</span>
          </button>
          <div className="version-tooltip">
            <strong>Version 2 — Enhanced Model</strong>
            <ul>
              <li>Recency-weighted form, edge vulnerability, venue-specific home advantage, weather impact</li>
            </ul>
          </div>
        </div>
        <div className="version-btn-wrap">
          <button className={`version-btn ${modelFilter === 3 ? 'active' : ''}`}
            onClick={() => setModelFilter(3)}>
            V3 <span className="version-desc">Full Model</span>
          </button>
          <div className="version-tooltip">
            <strong>Version 3 — Full Model</strong>
            <ul>
              <li>Margin-weighted form, rest days, bye-week boost, opponent-quality tries, interchange timing, calibration</li>
            </ul>
          </div>
        </div>
      </div>

      {noData ? (
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

          {/* Model comparison */}
          {modelFilter === null && data.by_model && data.by_model.length > 1 && (
            <div className="accuracy-section">
              <h3 className="section-title">Model Comparison</h3>
              <div className="model-comparison-table">
                <div className="mc-row mc-heading">
                  <span className="mc-model">Model</span>
                  <span className="mc-val">Matches</span>
                  <span className="mc-val">Win %</span>
                  <span className="mc-val">Try Hits</span>
                  <span className="mc-val">Multi Hits</span>
                </div>
                {data.by_model.map(m => (
                  <div key={m.model_version} className="mc-row">
                    <span className="mc-model">V{m.model_version}</span>
                    <span className="mc-val">{m.total}</span>
                    <span className="mc-val">{m.total > 0 ? ((m.win_correct / m.total) * 100).toFixed(1) : 0}%</span>
                    <span className="mc-val">{m.try_hits || 0}</span>
                    <span className="mc-val">{m.multi_all_hit || 0}/{m.total}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* By round */}
          {data.by_round && data.by_round.length > 0 && (
            <div className="accuracy-section">
              <h3 className="section-title">Accuracy by Round</h3>
              <div className="round-accuracy-chart">
                {data.by_round.map(r => {
                  const winPct = r.total > 0 ? (r.win_correct / r.total) * 100 : 0
                  return (
                    <div key={r.round_number} className="round-bar-group">
                      <div className="round-bar-col">
                        <div className="round-bar-track">
                          <div className="round-bar-fill" style={{ height: `${winPct}%` }} />
                        </div>
                        <span className="round-bar-pct">{winPct.toFixed(0)}%</span>
                      </div>
                      <span className="round-bar-label">R{r.round_number}</span>
                      <span className="round-bar-count">{r.win_correct}/{r.total}</span>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}
