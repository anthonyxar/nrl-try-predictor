import React from 'react'
import { Routes, Route, Link } from 'react-router-dom'
import WeekSelector from './components/WeekSelector'
import Draw from './components/Draw'
import MatchDetail from './components/MatchDetail'
import PlayerDetail from './components/PlayerDetail'
import TeamDetail from './components/TeamDetail'
import AccuracyDashboard from './components/AccuracyDashboard'
import Models from './components/Models'
import SearchBar from './components/SearchBar'

const API_BASE = import.meta.env.VITE_API_BASE || '/api'

export default function App() {
  return (
    <div className="app">
      <header className="header">
        <div className="header-inner">
          <div className="header-top">
            <div className="header-left">
              <h1 className="logo">
                <Link to="/" className="logo-link">
                  <span className="logo-nrl">NRL</span> Try Predictor
                </Link>
              </h1>
              <p className="subtitle">Live player try-scoring probabilities for the 2026 season</p>
            </div>
            <div className="header-right">
              <SearchBar apiBase={API_BASE} />
              <Link to="/models" className="header-link">Models</Link>
              <Link to="/accuracy" className="header-link">Accuracy</Link>
            </div>
          </div>
        </div>
      </header>
      <main className="main">
        <Routes>
          <Route path="/" element={<WeekSelector apiBase={API_BASE} />} />
          <Route path="/round/:roundNumber" element={<Draw apiBase={API_BASE} />} />
          <Route path="/match" element={<MatchDetail apiBase={API_BASE} />} />
          <Route path="/player" element={<PlayerDetail apiBase={API_BASE} />} />
          <Route path="/team" element={<TeamDetail apiBase={API_BASE} />} />
          <Route path="/accuracy" element={<AccuracyDashboard apiBase={API_BASE} />} />
          <Route path="/models" element={<Models />} />
        </Routes>
      </main>
    </div>
  )
}
