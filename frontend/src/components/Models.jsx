import React from 'react'
import { Link } from 'react-router-dom'

/*
  Model explainer page — describes how the prediction model works.
*/

const TRY_FACTORS = [
  'Position base rate — how often each position scores tries league-wide (e.g. wingers ~35%, props ~6%).',
  'Player try factor — blends the player’s recent 5-game form (60%) with career rate (40%), relative to their position’s expected rate.',
  'Team attack / opponent defence — blends last-5 and last-10 game averages for both sides.',
  'Edge vulnerability — the opponent’s left/right/middle defensive weakness, weighted by how much each position actually attacks that edge.',
  'Home advantage — a 6% try-rate boost when playing at home.',
  'Weather & ground conditions — wet weather and heavy grounds suppress try rates, backs more than forwards.',
  'Margin-of-victory weighted form — a 30-point win counts more than a scrappy 2-point win, via a sigmoid quality score.',
  'Rest days & bye week — short turnarounds (≤5 days) penalise try rate, a bye week or 9+ days rest boosts it.',
  'Season progression — early-season rounds (1–8) are discounted, since there’s less reliable data yet.',
  'Opponent-quality adjusted try rate — tries scored against strong defences count for more than tries against weak ones.',
  'Interchange timing — bench players are scaled by their actual average minutes on field, not a flat assumption.',
  'Historical calibration — predictions are pulled toward the observed hit rate for their probability bucket, correcting for any systematic over/under-confidence.',
]

const WIN_FACTORS = [
  'Recency-weighted form — last-5 form (20%) and last-10 form (10%), so recent results matter more than older ones.',
  'Points differential — last-5 scoring margin (15%).',
  'Margin-weighted quality score — the same sigmoid-of-margins signal used for try rates (20%).',
  'Head-to-head record (10%).',
  'Venue-specific home advantage — the home team’s actual win rate at that ground, where there’s enough history; a flat +8% otherwise (15%).',
  'Season win rate (10%).',
  'Rest days, bye week, and early-season adjustments layered on top of the weighted blend.',
]

export default function Models() {
  return (
    <div className="models-page">
      <div className="models-header">
        <Link to="/predictions" className="back-link">&larr; All Rounds</Link>
        <h2>How the Model Works</h2>
      </div>

      <p className="models-intro">
        Every match is scored by the same prediction model, and every prediction is
        tracked in the {' '}<Link to="/accuracy" className="inline-link">accuracy dashboard</Link>{' '}
        against what actually happened.
      </p>

      <section className="model-card">
        <div className="model-card-header">
          <div>
            <h3 className="model-name">Try Probability</h3>
            <p className="model-tagline">What decides how likely a player is to score.</p>
          </div>
        </div>
        <div className="model-description">
          <ul className="models-factor-list">
            {TRY_FACTORS.map((text, i) => <li key={i}>{text}</li>)}
          </ul>
        </div>
      </section>

      <section className="model-card">
        <div className="model-card-header">
          <div>
            <h3 className="model-name">Win Probability</h3>
            <p className="model-tagline">What decides the predicted winner and score.</p>
          </div>
        </div>
        <div className="model-description">
          <ul className="models-factor-list">
            {WIN_FACTORS.map((text, i) => <li key={i}>{text}</li>)}
          </ul>
        </div>
      </section>
    </div>
  )
}
