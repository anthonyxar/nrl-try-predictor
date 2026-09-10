import React from 'react'
import { Routes, Route, Link } from 'react-router-dom'
import Sidebar from './components/Sidebar'
import Dashboard from './components/Dashboard'
import WeekSelector from './components/WeekSelector'
import Draw from './components/Draw'
import MatchDetail from './components/MatchDetail'
import PlayerDetail from './components/PlayerDetail'
import PlayersList from './components/PlayersList'
import TeamDetail from './components/TeamDetail'
import TeamsList from './components/TeamsList'
import AccuracyDashboard from './components/AccuracyDashboard'
import Models from './components/Models'
import SearchBar from './components/SearchBar'

const API_BASE = import.meta.env.VITE_API_BASE || '/api'

export default function App() {
  return (
    <div className="app">
      <Sidebar />
      <div className="app-content">
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
              </div>
            </div>
          </div>
        </header>
        <main className="main">
          <Routes>
            <Route path="/" element={<Dashboard apiBase={API_BASE} />} />
            <Route path="/predictions" element={<WeekSelector apiBase={API_BASE} />} />
            <Route path="/round/:roundNumber" element={<Draw apiBase={API_BASE} />} />
            <Route path="/teams" element={<TeamsList apiBase={API_BASE} />} />
            <Route path="/team" element={<TeamDetail apiBase={API_BASE} />} />
            <Route path="/players" element={<PlayersList apiBase={API_BASE} />} />
            <Route path="/player" element={<PlayerDetail apiBase={API_BASE} />} />
            <Route path="/match" element={<MatchDetail apiBase={API_BASE} />} />
            <Route path="/accuracy" element={<AccuracyDashboard apiBase={API_BASE} />} />
            <Route path="/models" element={<Models />} />
          </Routes>
        </main>
        <footer className="footer">
          <div className="footer-inner">
            <p className="footer-disclaimer">
              Disclaimer: this website is for informational and entertainment
              purposes only. All predictions are statistical estimates and are
              not betting advice. This website and its operators accept no
              responsibility for any financial loss incurred from decisions made
              based on the information presented here. Please gamble responsibly.
            </p>
          </div>
        </footer>
      </div>
    </div>
  )
}
