import React from 'react'
import { Link } from 'react-router-dom'

/*
  Models explainer page.

  Describes each model version (V1, V2, V3), how try and win probabilities
  are calculated, and shows a comparison matrix of which calculation items
  are applicable to each model.
*/

const MODELS = [
  {
    key: 'v1',
    version: 1,
    name: 'Version 1 — Baseline',
    tagline: 'The simplest model. Career averages plus a flat home bump.',
    description: [
      'V1 is the starting point: player try factors come from their entire career history, team attack/defence is pulled from the last 10 games only, and factors are weighted equally.',
      'Try probability starts from the player\u2019s positional base rate, is multiplied by a career-based player factor (career tries per game \u00f7 expected tries per game), then scaled by team attack, opponent defence, and a flat 6% home advantage.',
      'Win probability is an equal-weighted blend of last-10 form (30%), H2H (25%), venue win rate (25%) and season win rate (20%).',
    ],
  },
  {
    key: 'v2',
    version: 2,
    name: 'Version 2 — Enhanced',
    tagline: 'Recency-weighted form, edge matchups, venue and weather.',
    description: [
      'V2 adds situational awareness. Recent form is blended: 60% last-5 games, 40% last-10, for both players and team attack/defence numbers.',
      'Edge vulnerability kicks in \u2014 the opponent\u2019s weakness on the left/right/middle is compared to league average, and players on jerseys 1\u201313 are nudged up or down depending on which side they attack from (flat 0.3 weight for all positions in V2).',
      'Win probability is re-weighted toward recency: last-5 form 30%, last-10 form 20%, points differential 15%, H2H 10%, venue-specific home/away win rate 15%, season 10%.',
      'Weather and ground conditions suppress try rates for backs more than forwards when wet, and venue-specific home advantage replaces the flat 8% bonus.',
    ],
  },
  {
    key: 'v3',
    version: 3,
    name: 'Version 3 — Full Model',
    tagline: 'Adds margin, rest, quality-of-opposition, interchange timing, and calibration.',
    description: [
      'V3 keeps everything in V2 and layers on the situational factors that actually show up in the historical data.',
      'Form is re-weighted by margin: a 30-point win counts more than a scrappy 2-point win via a sigmoid quality score (0\u20131).',
      'Rest days matter \u2014 5-day turnarounds penalise try rate (\u00d70.93) and win rate (\u22123%), while a bye week boosts both (+5% try rate, +4% win rate). Early-season rounds (1\u20134) discount predictions because the sample is noisy.',
      'Player try factor is blended 70/30 with an opponent-quality adjusted rate (tries against strong defences count more). Edge vulnerability uses position-specific sensitivity weights instead of a flat 0.3.',
      'Interchange players get minutes-based scaling using their actual average bench minutes rather than a flat 40% haircut.',
      'Finally, predictions are calibrated against historical hit rates per probability bucket \u2014 if V3 has been over-confident in the 30\u201340% bucket, future predictions are pulled toward the observed rate.',
    ],
  },
]

// Calculation item rows. applies is a map of model key -> boolean.
const CALCULATION_ITEMS = [
  {
    group: 'Try Probability',
    items: [
      { label: 'Position base rate', applies: { v1: true, v2: true, v3: true } },
      { label: 'Career player try factor', applies: { v1: true, v2: true, v3: true } },
      { label: 'Recent form (last-5 / last-10 blend)', applies: { v1: false, v2: true, v3: true } },
      { label: 'Team attack (last-10 only)', applies: { v1: true, v2: false, v3: false } },
      { label: 'Team attack (last-5 / last-10 blend)', applies: { v1: false, v2: true, v3: true } },
      { label: 'Opponent defence (last-10 only)', applies: { v1: true, v2: false, v3: false } },
      { label: 'Opponent defence (last-5 / last-10 blend)', applies: { v1: false, v2: true, v3: true } },
      { label: 'Flat home-advantage multiplier (6%)', applies: { v1: true, v2: true, v3: true } },
      { label: 'Edge vulnerability (flat 0.3 weight)', applies: { v1: false, v2: true, v3: false } },
      { label: 'Edge vulnerability (position-specific weights)', applies: { v1: false, v2: false, v3: true } },
      { label: 'Weather / ground conditions factor', applies: { v1: false, v2: true, v3: true } },
      { label: 'Margin-of-victory weighted form', applies: { v1: false, v2: false, v3: true } },
      { label: 'Rest-days short-turnaround penalty', applies: { v1: false, v2: false, v3: true } },
      { label: 'Bye-week boost', applies: { v1: false, v2: false, v3: true } },
      { label: 'Season progression discount (rounds 1\u20138)', applies: { v1: false, v2: false, v3: true } },
      { label: 'Opponent-quality adjusted try rate blend', applies: { v1: false, v2: false, v3: true } },
      { label: 'Interchange flat 40% minutes haircut', applies: { v1: true, v2: true, v3: false } },
      { label: 'Interchange actual bench-minutes scaling', applies: { v1: false, v2: false, v3: true } },
      { label: 'Historical probability calibration', applies: { v1: false, v2: false, v3: true } },
    ],
  },
  {
    group: 'Win Probability',
    items: [
      { label: 'Last-10 form weighting', applies: { v1: true, v2: true, v3: true } },
      { label: 'Last-5 form weighting', applies: { v1: false, v2: true, v3: true } },
      { label: 'Points differential (last-5)', applies: { v1: false, v2: true, v3: true } },
      { label: 'Head-to-head record', applies: { v1: true, v2: true, v3: true } },
      { label: 'Flat venue win-rate', applies: { v1: true, v2: false, v3: false } },
      { label: 'Venue-specific home/away win-rate', applies: { v1: false, v2: true, v3: true } },
      { label: 'Season win-rate', applies: { v1: true, v2: true, v3: true } },
      { label: 'Margin-weighted quality score', applies: { v1: false, v2: false, v3: true } },
      { label: 'Rest-days advantage adjustment', applies: { v1: false, v2: false, v3: true } },
      { label: 'Bye-week boost', applies: { v1: false, v2: false, v3: true } },
      { label: 'Early-season discount (rounds 1\u20134)', applies: { v1: false, v2: false, v3: true } },
    ],
  },
]

function Tick() {
  return <span className="model-tick" aria-label="applies">&#10003;</span>
}

function Cross() {
  return <span className="model-cross" aria-label="does not apply">&#10007;</span>
}

export default function Models() {
  return (
    <div className="models-page">
      <div className="models-header">
        <Link to="/" className="back-link">&larr; All Rounds</Link>
        <h2>Prediction Models</h2>
      </div>

      <p className="models-intro">
        The NRL Try Predictor runs three model versions side-by-side so we can
        compare how each iteration performs over time. Every match is scored by
        all three models and their predictions are tracked in the
        {' '}<Link to="/accuracy" className="inline-link">accuracy dashboard</Link>.
      </p>

      {MODELS.map(m => (
        <section key={m.key} className="model-card">
          <div className="model-card-header">
            <span className={`model-badge model-badge-v${m.version}`}>V{m.version}</span>
            <div>
              <h3 className="model-name">{m.name}</h3>
              <p className="model-tagline">{m.tagline}</p>
            </div>
          </div>
          <div className="model-description">
            {m.description.map((para, idx) => (
              <p key={idx}>{para}</p>
            ))}
          </div>
        </section>
      ))}

      <section className="model-matrix-section">
        <h3 className="section-title">Calculation Matrix</h3>
        <p className="models-intro">
          Every calculation item below is a single factor that feeds into a
          prediction. A green tick means the model applies the factor; a red
          cross means it does not.
        </p>

        <div className="model-matrix-wrap">
          <table className="model-matrix">
            <thead>
              <tr>
                <th className="mm-item-col">Calculation item</th>
                {MODELS.map(m => (
                  <th key={m.key} className="mm-model-col">V{m.version}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {CALCULATION_ITEMS.map(group => (
                <React.Fragment key={group.group}>
                  <tr className="mm-group-row">
                    <td colSpan={1 + MODELS.length}>{group.group}</td>
                  </tr>
                  {group.items.map(item => (
                    <tr key={item.label}>
                      <td className="mm-item-col">{item.label}</td>
                      {MODELS.map(m => (
                        <td key={m.key} className="mm-model-col">
                          {item.applies[m.key] ? <Tick /> : <Cross />}
                        </td>
                      ))}
                    </tr>
                  ))}
                </React.Fragment>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}
